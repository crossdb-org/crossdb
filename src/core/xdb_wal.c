/******************************************************************************
* Copyright (c) 2024-present JC Wang. All rights reserved
*
*   https://crossdb.org
*   https://github.com/crossdb-org/crossdb
*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at https://mozilla.org/MPL/2.0/.
******************************************************************************/

#if XDB_LOG_FLAGS & XDB_LOG_WAL
#define xdb_wallog(...)	xdb_print(__VA_ARGS__)
#else
#define xdb_wallog(...)
#endif

XDB_STATIC void
xdb_init_wal (xdb_stgmgr_t *pStgMgr, void *pArg, xdb_size fsize)
{
	xdb_wal_t		*pWal = (xdb_wal_t*) pStgMgr->pStgHdr;

	if (fsize > 0) {
		if (pWal->wal_size != fsize) {
			xdb_errlog ("Detect WAL fsize is %"PRIi64" != size %"PRIi64"\n", pWal->wal_size, fsize);
			pWal->wal_size = fsize;
		}
		if (pWal->commit_size > fsize) {
			pWal->commit_size = fsize;
		}
		pWal->sync_size = pWal->commit_size;
	} else {
		pWal->wal_size 		= sizeof (xdb_wal_t) + pStgMgr->pStgHdr->blk_cap * 4096;
		pWal->commit_size	= sizeof (xdb_wal_t);
		pWal->sync_size		= sizeof (xdb_wal_t);
		pWal->wal_commit[0].commit_len = 0;
	}
}

XDB_STATIC int 
xdb_wal_create (xdb_conn_t *pConn, xdb_dbm_t *pDbm)
{
	if (pDbm->bMemory) {
		return XDB_OK;
	}

	int rc;
	for (int i = 0; i < 2; ++i) {
		xdb_stghdr_t stg_hdr = {.stg_magic = 0xE7FCFDFB, .blk_size = 4096, .ctl_off = XDB_STG_NOALLOC, .blk_off = XDB_OFFSET(xdb_wal_t, wal_row)};
		xdb_walm_t *pWalm = &pDbm->wal_mgr[i];
		xdb_stgmgr_t *pStgMgr = &pWalm->stg_mgr;
		pWalm->pDbm = pDbm;

		char path[XDB_PATH_LEN + 32];
		xdb_sprintf (path, "%s/xdb%d.wal", pWalm->pDbm->db_path, i);

		pStgMgr->pOps = pDbm->bMemory ? &s_xdb_store_mem_ops : &s_xdb_store_file_ops;
		pStgMgr->pStgHdr	= &stg_hdr;
		rc = xdb_stg_open (pStgMgr, path, xdb_init_wal, NULL);
		XDB_EXPECT (rc == XDB_OK, XDB_ERROR, "Failed to create wal '%d'", i);
		pWalm->wal_cap = pStgMgr->pStgHdr->blk_cap;
	}

	xdb_wal_t *pWal = (xdb_wal_t*)pDbm->wal_mgr[1].stg_mgr.pStgHdr;

	if (pWal->wal_active) {
		pDbm->pWalm		= &pDbm->wal_mgr[1];
		pDbm->pWalmBak	= &pDbm->wal_mgr[0];
	} else {
		pDbm->pWalm		= &pDbm->wal_mgr[0];
		pDbm->pWalmBak	= &pDbm->wal_mgr[1];
	}

	pWal = (xdb_wal_t*)pDbm->pWalm->stg_mgr.pStgHdr;
	pWal->wal_active = true;

	return XDB_OK;

error:
	return rc;
}

XDB_STATIC void 
xdb_wal_close (xdb_dbm_t *pDbm)
{
	if (pDbm->bMemory) {
		return;
	}
	for (int i = 0; i < 2; ++i) {
		xdb_stg_close (&pDbm->wal_mgr[i].stg_mgr);
	}
}

XDB_STATIC void 
xdb_wal_drop (xdb_dbm_t *pDbm)
{
	if (pDbm->bMemory) {
		return;
	}

	for (int i = 0; i < 2; ++i) {
		char path[XDB_PATH_LEN + 32];
		xdb_sprintf (path, "%s/xdb%d.wal", pDbm->db_path, i);
		xdb_stg_drop (&pDbm->wal_mgr[i].stg_mgr, path);
	}
}

static int xdb_wal_expand (xdb_walm_t *pWalm, int size)
{
	xdb_wal_t			*pWal = (xdb_wal_t*)pWalm->stg_mgr.pStgHdr;

	uint64_t wal_size = pWal->wal_size;
	uint64_t req_size = pWal->commit_size + size + sizeof (xdb_commit_t);
	if (wal_size < req_size) {
		int wal_cap = pWalm->wal_cap;
		do {
			wal_cap <<= 1;
			wal_size = sizeof (xdb_wal_t) + wal_cap * 4096;
			
		} while (wal_size < req_size);
		xdb_stg_truncate (&pWalm->stg_mgr, wal_cap);
		xdb_wallog ("wal increase from %d to %d size %d\n", pWalm->wal_cap, wal_cap, size);
		pWalm->wal_cap = wal_cap;
		pWal = (xdb_wal_t*)pWalm->stg_mgr.pStgHdr;
		pWal->wal_size = wal_size;
	}

	return XDB_OK;
}

XDB_STATIC int 
xdb_trans_newrow_wal (uint32_t rid, void *pArg)
{
	xdb_tblTrans_t	*pTblTrans 	= pArg;
	xdb_stgmgr_t	*pStgMgr 	= &pTblTrans->pTblm->stg_mgr;

	xdb_rowid *pRow = XDB_IDPTR(pStgMgr, rid);
	xdb_walm_t			*pWalm = pTblTrans->pDbTrans->pDbm->pWalm;

	int size = sizeof (xdb_walrow_t) + pTblTrans->pTblm->row_size;
	uint32_t *pVdat = NULL;
	if (pTblTrans->pTblm->vfld_count > 0) {
		size += 4;
		pVdat = xdb_row_vdata_get (pTblTrans->pTblm, pRow);
		if (pVdat != NULL) {
			size += (*pVdat&XDB_VDAT_LENMASK);
		}
	}
	size = XDB_ALIGN4 (size);

	xdb_wal_expand (pWalm, size);

	xdb_wal_t			*pWal = (xdb_wal_t*)pWalm->stg_mgr.pStgHdr;
	xdb_walrow_t		*pWalRow = (void*)pWal + pWal->commit_size + pTblTrans->pDbTrans->commit_len;
	pWalRow->row_len	= size;

	pWalRow->tbl_xoid   = XDB_OBJ_ID(pTblTrans->pTblm);
	pWalRow->row_id 	= rid;

	if (pTblTrans->pTblm->vfld_count > 0) {
		memcpy (pWalRow->row_data, pRow, pTblTrans->pTblm->row_size + 4);
		if (pVdat != NULL) {
			memcpy (pWalRow->row_data + pTblTrans->pTblm->row_size + 4, (void*)pVdat + 4, (*pVdat&XDB_VDAT_LENMASK));
		}
	} else {
		memcpy (pWalRow->row_data, pRow, pTblTrans->pTblm->row_size);
	}

	pTblTrans->pDbTrans->commit_len += pWalRow->row_len;

	xdb_wallog ("      wal new row %d len %d\n", rid, pWalRow->row_len);

	return XDB_OK;
}

XDB_STATIC int 
xdb_trans_delrow_wal (uint32_t rid, void *pArg)
{
	xdb_tblTrans_t	*pTblTrans 	= pArg;
	xdb_walm_t			*pWalm = pTblTrans->pDbTrans->pDbm->pWalm;

	//xdb_stgmgr_t	*pStgMgr 	= &pTblTrans->pTblm->stg_mgr;
	//xdb_rowid *pRow = XDB_IDPTR(pStgMgr, rid);

	xdb_wal_expand (pWalm, sizeof (xdb_walrow_t));

	xdb_wal_t			*pWal = (xdb_wal_t*)pWalm->stg_mgr.pStgHdr;
	xdb_walrow_t		*pWalRow = (void*)pWal + pWal->commit_size + pTblTrans->pDbTrans->commit_len;

	pWalRow->row_len	= sizeof (xdb_walrow_t);
	pWalRow->row_id 	= rid | XDB_ROWID_MSB;

	pTblTrans->pDbTrans->commit_len += pWalRow->row_len;

	pWalRow->tbl_xoid   = XDB_OBJ_ID(pTblTrans->pTblm);

	xdb_wallog ("      wal del row %d len %d\n", rid, pWalRow->row_len);

	return XDB_OK;
}

XDB_STATIC int 
xdb_trans_tbl_wal (uint32_t tid, void *pArg)
{
	xdb_dbTrans_t 	*pDbTrans	= pArg;
	xdb_tblTrans_t	*pTblTrans	= pDbTrans->pTblTrans[tid];

	xdb_wallog ("    commit table '%s'\n", XDB_OBJ_NAME(pTblTrans->pTblm));

	if (pTblTrans->pTblm->bMemory) {
		// skip memory table
		return XDB_OK;
	}

	// iterate del rows -> delete
	xdb_bmp_iterate (&pTblTrans->del_rows, xdb_trans_delrow_wal, pTblTrans);

	// iterate new rows -> commit
	xdb_bmp_iterate (&pTblTrans->new_rows, xdb_trans_newrow_wal, pTblTrans);

	return XDB_OK;
}

XDB_STATIC int 
xdb_trans_db_wal (uint32_t did, void *pArg)
{
	xdb_conn_t *pConn = pArg;
	xdb_dbTrans_t *pDbTrans = pConn->pDbTrans[did];
	xdb_dbm_t	*pDbm = pDbTrans->pDbm;

	if (pDbm->bMemory) {
		// Skip memory table
		return XDB_OK;
	}

	pDbTrans->commit_len = 0;

	xdb_lv2bmp_iterate (&pDbTrans->tbl_rows, xdb_trans_tbl_wal, pDbTrans);

	if (0 == pDbTrans->commit_len) {
		return XDB_OK;
	}
	xdb_wal_t *pWal = (void*)pDbm->pWalm->stg_mgr.pStgHdr;

	xdb_commit_t *pCommit = (void*)pWal + pWal->commit_size - sizeof (xdb_commit_t);

	pCommit->timestamp	= xdb_timestamp_us();
	pCommit->commit_id	= ++pWal->commit_id;
	pCommit->checksum = xdb_wyhash (pCommit + 1, pDbTrans->commit_len);
	pCommit->commit_len = pDbTrans->commit_len + sizeof (xdb_commit_t);

	xdb_commit_t *pCommitEnd = (void*)pWal + pWal->commit_size + pDbTrans->commit_len;
	pCommitEnd->commit_len = 0;

	uint64_t flush_size = pWal->commit_size - pWal->sync_size + pCommit->commit_len + 8;
	xdb_wallog ("  DB '%s' WAL commitid %"PRIu64" len %u, wal sync off %u len %d\n", XDB_OBJ_NAME(pDbm), pCommit->commit_id,
				(uint32_t)pDbTrans->commit_len, (uint32_t)(pWal->commit_size - sizeof(xdb_commit_t)), (uint32_t)flush_size);

	if (pDbm->sync_mode > 0) {
		// include endof commit_len=0
		if (pWal->commit_id - pWal->sync_cid >= pDbm->sync_mode) {
			xdb_stg_sync (&pDbm->stg_mgr, pWal->commit_size - sizeof(xdb_commit_t), flush_size, false);
			pWal->commit_size += pCommit->commit_len;
			pWal->sync_size = pWal->commit_size;
			pWal->sync_cid	= pCommit->commit_id;
		} else {
			xdb_stg_sync (&pDbm->stg_mgr, pWal->commit_size - sizeof(xdb_commit_t), flush_size, true);
			pWal->commit_size += pCommit->commit_len;
		}
	} else {
		pWal->commit_size += pCommit->commit_len;
		pWal->sync_size = pWal->commit_size;
		pWal->sync_cid	= pCommit->commit_id;
	}

	if (pWal->commit_size > XDB_WAL_MAX_SIZE) {
		xdb_flush_db (pDbm, 0);
	}

	return XDB_OK;
}

typedef xdb_ret (*xdb_wal_callbck) (xdb_tblm_t *pTblm, xdb_walrow_t *pWalRow, void *pArg);

XDB_STATIC xdb_ret 
__xdb_wal_flush (xdb_walm_t *pWalm, bool bFlush)
{
	xdb_wal_t			*pWal = XDB_WAL_PTR(pWalm);

	xdb_wallog ("Transaction flush commit_size %d\n", pWal->commit_size - sizeof (xdb_wal_t));

	pWal->sync_size = pWal->commit_size = sizeof (xdb_wal_t);
	pWal->wal_commit->commit_len = 0;
	pWal->wal_commit->commit_id = 0;
	pWal->sync_cid = pWal->commit_id;

	xdb_stg_sync (&pWalm->stg_mgr, 0, 0, !bFlush);

	return XDB_OK;
}

XDB_STATIC xdb_ret 
xdb_wal_flush (xdb_walm_t *pWalm)
{
	// Lock WAL
	xdb_wal_wrlock (pWalm->pDbm);

	__xdb_wal_flush (pWalm, true);

	// Lock WAL
	xdb_wal_wrunlock (pWalm->pDbm);

	return XDB_OK;
}

XDB_STATIC xdb_commit_id 
xdb_wal_iterate (xdb_walm_t *pWalm, xdb_wal_callbck cb_func, void *pArg)
{
	xdb_wal_t			*pWal = XDB_WAL_PTR(pWalm);
	xdb_commit_t		*pCommit = pWal->wal_commit;
	xdb_walrow_t		*pWalRow;
	xdb_tblm_t			*pTblm;
	xdb_dbm_t			*pDbm = pWalm->pDbm;
	uint64_t			commit_size = sizeof (xdb_wal_t);
	xdb_commit_id		last_commit_id = pWal->commit_id;
	void				*pWalEnd = (void*)pWal + pWal->commit_size;

	// Go over until end or checksum error
	while (((void*)pCommit < pWalEnd) && (pCommit->commit_len > 0)) {

		xdb_wallog ("  === Begin Iterate commit %u off %u len %"PRIu64"\n", pCommit->commit_id, (void*)pCommit-(void*)pWal, pCommit->commit_len);

		commit_size += pCommit->commit_len;
		
		if (commit_size > pWal->commit_size) {
			xdb_wallog ("Total commit size %"PRIu64" > WAL size %"PRIu64"\n", commit_size, pWal->commit_size);
			break;
		}

		if (pCommit->checksum > 0) {
			uint32_t checksum = xdb_wyhash (pCommit+1, pCommit->commit_len - sizeof (*pCommit));
			if (checksum != pCommit->checksum) {
				xdb_errlog ("WAL Checksum error: expect 0x%x != 0x%x\n", checksum, pCommit->checksum);
				break;
			}
		}
		last_commit_id = pCommit->commit_id;
		pWalRow = (xdb_walrow_t*)(pCommit + 1);
		void	*pComEnd= (void*)pCommit + pCommit->commit_len;
		do {
			pTblm = xdb_objm_getbyid (&pDbm->db_objm, pWalRow->tbl_xoid);
			if (NULL == pTblm) {
				xdb_errlog ("Can't find table id %d\n", pWalRow->tbl_xoid);
				break;
			}
			cb_func (pTblm, pWalRow, pArg);
			pWalRow = (xdb_walrow_t*)((void*)pWalRow + pWalRow->row_len);
		} while ((void*)pWalRow < (void*)pComEnd);

		xdb_wallog ("  === End iterate commit ID %u off %u len %d\n\n", pCommit->commit_id, (void*)pCommit-(void*)pWal, pCommit->commit_len);

		// Next commit
		pCommit = (xdb_commit_t*)((void*)pCommit + pCommit->commit_len);
	} 

	return 	last_commit_id;
}

XDB_STATIC xdb_ret 
xdb_wal_redo_row (xdb_tblm_t *pTblm, xdb_walrow_t *pWalRow, void *pArg)
{
	xdb_rowid rid = pWalRow->row_id & XDB_ROWID_MASK;
	bool bDelete = pWalRow->row_id & XDB_ROWID_MSB;
	void		*pRow = pWalRow->row_data;
	xdb_stgmgr_t *pStgMgr = &pTblm->stg_mgr;

	xdb_wallog ("	WAL Redo %s '%s' rowid=%d: ", bDelete ? "DEELTE" : "INSERT", XDB_OBJ_NAME(pTblm), rid);

	if (bDelete) {
		if (XDB_ROWID_VALID(rid, XDB_STG_MAXID(pStgMgr))) {
			pRow = XDB_IDPTR (&pTblm->stg_mgr, rid);			
			#if XDB_LOG_FLAGS & XDB_LOG_WAL
			xdb_fprint_dbrow (stdout, pTblm, pRow, 0);
			xdb_wallog ("\n");
			#endif
			__xdb_row_delete (pTblm, rid, pRow);
		} else {
			xdb_wallog ("    WAL Redo delete invalid rowid=%d\n", rid);
		}
	} else {
		void *pDbRow = XDB_IDPTR (&pTblm->stg_mgr, rid);
		if (pTblm->vfld_count > 0) {
			memcpy (pDbRow, pRow, pTblm->row_size + 4);
			int vlen = pWalRow->row_len - sizeof(xdb_walrow_t) - pTblm->row_size - 4;
			if (vlen > 0) {
				uint32_t *pVdat = xdb_row_vdata_get (pTblm, pDbRow);
				memcpy ((void*)pVdat + 4, pRow + pTblm->row_size, vlen);
				// b1000
				*pVdat = (8<<XDB_VDAT_LENBITS) | vlen;
			}
		} else {
			memcpy (pDbRow, pRow, pTblm->row_size);
		}

		XDB_ROW_CTRL (pStgMgr->pStgHdr, pDbRow) &= ~XDB_ROW_MASK;
		XDB_ROW_CTRL (pStgMgr->pStgHdr, pDbRow) |= XDB_ROW_COMMIT;
		// will not check unique
		xdb_idx_addRow (NULL, pTblm, rid, pDbRow);		

		#if XDB_LOG_FLAGS & XDB_LOG_WAL
		xdb_fprint_dbrow (stdout, pTblm, pDbRow, 0);
		xdb_wallog ("\n");
		#endif
	}

	return XDB_OK;
}

XDB_STATIC xdb_ret 
xdb_wal_redo (xdb_dbm_t	*pDbm)
{
	xdb_walm_t *pWalm;

	xdb_print ("  --- Begin Redo WAL Log\n");
	
	// Lock WAL
	xdb_wal_rdlock (pDbm);

	// redo backup
	pWalm = pDbm->pWalmBak;
	XDB_WAL_PTR(pWalm)->commit_id = xdb_wal_iterate (pWalm, xdb_wal_redo_row, NULL);

	// redo current
	pWalm = pDbm->pWalm;
	XDB_WAL_PTR(pWalm)->commit_id = xdb_wal_iterate (pWalm, xdb_wal_redo_row, NULL);

	// UnLock WAL
	xdb_wal_rdunlock (pDbm);

	xdb_flush_db (pDbm, 0);
	xdb_wal_flush (pDbm->pWalm);
	xdb_wal_flush (pDbm->pWalmBak);

	xdb_print ("  --- End Redo WAL Log\n");

	return XDB_OK;
}

XDB_STATIC xdb_ret 
__xdb_wal_dump_callbck (xdb_tblm_t *pTblm, xdb_walrow_t *pWalRow, void *pArg)
{
	xdb_rowid rid = pWalRow->row_id & XDB_ROWID_MASK;
	bool bDelete = pWalRow->row_id & XDB_ROWID_MSB;
	void *pRow = pWalRow->row_data;
	if (bDelete) {
		pRow = XDB_IDPTR (&pTblm->stg_mgr, rid);
	}
	xdb_print ("    %s %s rowid=%d: ", bDelete ? "DEELTE" : "INSERT", XDB_OBJ_NAME(pTblm), rid);
	xdb_fprint_dbrow (stdout, pTblm, pRow, 0);
	xdb_print ("\n");
	return XDB_OK;
}

XDB_STATIC void 
__xdb_wal_dump (xdb_walm_t *pWalm)
{
	if (NULL == pWalm) {
		return;
	}
	xdb_wal_t		*pWal = (xdb_wal_t*)pWalm->stg_mgr.pStgHdr;

	xdb_print ("  WAL Size %"PRIu64" Commit Size %"PRIu64" Sync Size %"PRIu64"\n", 
		pWal->wal_size, pWal->commit_size, pWal->sync_size);

	xdb_print ("  Last Commit ID %"PRIu64" Rollback Count %"PRIu64"\n", 
		pWal->commit_id, pWal->rollback_count);

	xdb_wal_iterate (pWalm, __xdb_wal_dump_callbck, NULL);
}

XDB_STATIC void 
xdb_wal_dump (xdb_dbm_t *pDbm)
{
	xdb_print ("Active WAL");
	__xdb_wal_dump (pDbm->pWalm);
	xdb_print ("Backup WAL");
	__xdb_wal_dump (pDbm->pWalmBak);
}

XDB_STATIC bool 
xdb_wal_switch (xdb_dbm_t *pDbm)
{
	bool bSwitch = false;
	xdb_walm_t *pWalm;
	xdb_wal_t *pWal, *pWalBak;
	
	// Lock WAL
	xdb_wal_wrlock (pDbm);

	xdb_wal_t *pBakWal = (xdb_wal_t*)pDbm->pWalmBak->stg_mgr.pStgHdr;
	// Backup still has data, which means flush DB not down yet
	if (pBakWal->commit_size > sizeof (xdb_wal_t)) {
		bSwitch = true;
		goto exit;	
	}
	pWalm = pDbm->pWalm;
	if (XDB_WAL_PTR(pWalm)->commit_size <= sizeof (xdb_wal_t)) {
		goto exit;
	}
	pDbm->pWalm = pDbm->pWalmBak;
	pDbm->pWalmBak = pWalm;

	pWal = XDB_WAL_PTR(pDbm->pWalm);
	pWalBak = XDB_WAL_PTR(pDbm->pWalmBak);
	pWal->wal_active = true;
	pWalBak->wal_active = false;
	pWal->rollback_count = pWalBak->rollback_count;
	pWal->commit_id		= pWalBak->commit_id;
	bSwitch = true;

	xdb_wallog ("Switch WAL\n");

exit:
	// Lock WAL
	xdb_wal_wrunlock (pDbm);

	return bSwitch;
}
