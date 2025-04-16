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

#if XDB_LOG_FLAGS & XDB_LOG_TBL
#define xdb_tbllog(...)	xdb_print(__VA_ARGS__)
#else
#define xdb_tbllog(...)
#endif

XDB_STATIC void 
xdb_alloc_offset (xdb_tblm_t *pTblm, uint64_t types, int len)
{
	for (int i = 0; i < pTblm->fld_count; ++i) {
		xdb_field_t *pFld = &pTblm->pFields[i];
		int extra = 0;
		int fld_len = pFld->fld_len;
		if (types & (1LL<<pFld->fld_type)) {
			if (0 == pFld->fld_len) {
				fld_len = pFld->fld_len = len;
			}
			pFld->fld_off = pTblm->row_size;
			if ((XDB_TYPE_CHAR == pFld->fld_type) || (XDB_TYPE_BINARY == pFld->fld_type)) {
				pFld->fld_off += 2;
				extra = 2 + 1; // len(2B) + '\0'
			} else if (s_xdb_vdat[pFld->fld_type]) {
				fld_len = 4;
			}
			pTblm->row_size += fld_len + extra;
		}
	}
}

XDB_STATIC void
xdb_init_table (xdb_stgmgr_t *pStgMgr, void *pArg, xdb_size fsize)
{
	if (0 == fsize) {
		xdb_tbl_t *pTbl = (xdb_tbl_t*)pStgMgr->pStgHdr;
		pTbl->row_count = 0;
	}
}

/*
|--------------------real size ------|
|----row size---|
| fixdat | null  ctrl | 0xff  | len + vdat |

|--------------------real size ------|
|----row size---|
| fixdat | null  ctrl | 0xfe  | ptr ary |

|--------------------real size ------|
|----row size---|
| fixdat | null  ctrl | vtype | vrid |
*/

XDB_STATIC int 
xdb_create_table (xdb_stmt_tbl_t *pStmt)
{
	int			rc;
	xdb_conn_t	*pConn = pStmt->pConn;
	xdb_dbm_t*	pDbm = pStmt->pDbm;
	xdb_tblm_t *pTblm = NULL;

	if (NULL != pStmt->pTblm) {
		return XDB_OK;
	}

	xdb_wrlock_db (pDbm);

	XDB_EXPECT (XDB_OBJM_COUNT (pDbm->db_objm) < XDB_MAX_TBL, XDB_E_FULL, "Can create at most %d tables", XDB_MAX_TBL);

	pTblm = xdb_calloc (sizeof(xdb_tblm_t));

	XDB_EXPECT (NULL!=pTblm, XDB_E_MEMORY, "Can't alloc memory");

	pTblm->lock_mode = pDbm->lock_mode;
	pTblm->bMemory = pStmt->bMemory;
	if (pTblm->bMemory && (XDB_LOCK_PROCESS == pTblm->lock_mode)) {
		pTblm->lock_mode = XDB_LOCK_THREAD;
	}
		
	XDB_OBJ_ID(pTblm) = pStmt->xoid;
	xdb_strcpy (XDB_OBJ_NAME(pTblm), pStmt->tbl_name);

	xdb_objm_add (&pDbm->db_objm, pTblm);

	char path[XDB_PATH_LEN + 32];
	xdb_sprintf (path, "%s/T%06d/xdb.dat", pDbm->db_path, XDB_OBJ_ID(pTblm));

	XDB_RWLOCK_INIT(pTblm->tbl_lock);
	XDB_RWLOCK_INIT(pTblm->stg_lock);

	pTblm->fld_count = pStmt->fld_count;
	pTblm->vfld_count = pStmt->vfld_count;

	pTblm->meta_size = XDB_ALIGN4 (sizeof(xdb_meta_t) + strlen(XDB_OBJ_NAME(pDbm)) + 1 + strlen(XDB_OBJ_NAME(pTblm)) + 1);
	uint16_t cols_off = pTblm->meta_size;

	int flds_size = sizeof(*pTblm->pFields) * pTblm->fld_count;
	pTblm->null_bytes = (pStmt->fld_count+7)>>3;
	pTblm->pFields = xdb_malloc (flds_size + sizeof(void*) * (pTblm->fld_count + pTblm->vfld_count) + XDB_ALIGN8(pTblm->null_bytes));
	XDB_EXPECT (NULL!=pTblm->pFields, XDB_E_MEMORY, "Can't alloc memory");
	memcpy (pTblm->pFields, pStmt->stmt_flds, flds_size);
	pTblm->ppFields = (void*)pTblm->pFields + flds_size;
	pTblm->ppVFields = (void*)pTblm->ppFields + sizeof(void*) * pTblm->fld_count;
	pTblm->pNullBytes = (void*)pTblm->ppVFields + sizeof(pTblm->ppVFields) * pTblm->vfld_count;
	memset (pTblm->pNullBytes, 0, XDB_ALIGN8(pTblm->null_bytes));

	int vi = 0;
	for (int i = 0; i < pTblm->fld_count; ++i) {
		xdb_field_t *pField = &pTblm->pFields[i];
		pTblm->ppFields[i] = pField;
		pTblm->meta_size += XDB_ALIGN4 (sizeof(xdb_col_t) + XDB_OBJ_NMLEN(pField) + 1);
		pField->pTblm = pTblm;
		xdb_objm_add (&pTblm->fld_objm, pField);
		if (s_xdb_vdat[pField->fld_type]) {
			pTblm->ppVFields[vi++] = pField;
		}
	}

	//pTblm->tbl_fd = tbl_fd;
	pTblm->row_size = pStmt->row_size;
	if (pStmt->row_size < 1) {
		pTblm->row_size = 0;
		xdb_alloc_offset (pTblm, (1LL<<XDB_TYPE_BIGINT) | (1LL<<XDB_TYPE_DOUBLE)| (1LL<<XDB_TYPE_TIMESTAMP), 8);
		xdb_alloc_offset (pTblm, (1LL<<XDB_TYPE_INT) | (1LL<<XDB_TYPE_FLOAT), 4);
		xdb_alloc_offset (pTblm, (1LL<<XDB_TYPE_VCHAR) | (1LL<<XDB_TYPE_VBINARY), 4);
		xdb_alloc_offset (pTblm, (1LL<<XDB_TYPE_CHAR) | (1LL<<XDB_TYPE_BINARY) | (1LL<<XDB_TYPE_SMALLINT), 2);
		xdb_alloc_offset (pTblm, (1LL<<XDB_TYPE_TINYINT) | (1LL<<XDB_TYPE_BOOL) | (1LL<<XDB_TYPE_INET) | (1LL<<XDB_TYPE_MAC), 1);
		pTblm->row_size +=pTblm->null_bytes + 1 + 1; // add null bits + vtype + ctrl
		pTblm->row_size  = XDB_ALIGN4 (pTblm->row_size);
		// null = ptr+row_size-2-null_bytes		ctrl = ptr+row_size-2	vtype = ptr+row_size-1
	}
	pTblm->null_off = pTblm->row_size - 2 - pTblm->null_bytes;
	pTblm->vtype_off  = pTblm->row_size - 1;

	// add fast pointer access, last 2B col_len=0 to mark end of meta
	int meta_size_align = XDB_ALIGN8 (pTblm->meta_size + 2);
	xdb_meta_t *pMeta = xdb_calloc (meta_size_align + 8 * pTblm->fld_count);
	XDB_EXPECT (NULL!=pMeta, XDB_E_MEMORY, "Can't alloc memory");
	pTblm->pMeta = pMeta;
	pMeta->col_count = pTblm->fld_count;
	pMeta->cols_off	 = cols_off;
	pMeta->null_off  = pTblm->null_off;
	pMeta->row_size	= pTblm->row_size;
	pMeta->tbl_nmlen = sprintf (pMeta->tbl_name, "%s.%s", XDB_OBJ_NAME(pDbm), XDB_OBJ_NAME(pTblm));
	xdb_col_t	*pCol = (void*)pMeta + pMeta->cols_off;
	uint64_t	*pColPtr = (void*)pMeta + meta_size_align;
	pMeta->col_list = (uintptr_t)pColPtr;

	for (int i = 0; i < pTblm->fld_count; ++i) {
		xdb_field_t *pFld = &pTblm->pFields[i];
		pColPtr[i] = (uintptr_t)pCol;
		pCol->col_type 	= pFld->fld_type;
		pCol->col_off	= pFld->fld_off;
		pCol->col_flags	= pFld->fld_flags;
		pCol->col_nmlen = XDB_OBJ_NMLEN(pFld);
		memcpy (pCol->col_name, XDB_OBJ_NAME(pFld), pCol->col_nmlen + 1);
		pCol->col_len = XDB_ALIGN4 (sizeof (*pCol) + pCol->col_nmlen + 1);
		pCol = (void*)pCol + pCol->col_len;
	}
	pCol->col_len = 0;

	pMeta->len_type = pTblm->meta_size | (XDB_RET_META<<28);
	pMeta->null_off = pTblm->null_off;

	pTblm->blk_size = pTblm->row_size;

	// for vdata
	if (pStmt->vfld_count) {
		pTblm->blk_size += sizeof (xdb_rowid);
	}

	xdb_stghdr_t stg_hdr = {.stg_magic = 0xE7FCFDFB, .blk_size = pTblm->blk_size, .ctl_off = pTblm->row_size - 2, .blk_off = XDB_OFFSET(xdb_tbl_t, pRowDat)};
	pTblm->stg_mgr.pOps = pTblm->bMemory ? &s_xdb_store_mem_ops : &s_xdb_store_file_ops;
	pTblm->stg_mgr.pStgHdr	= &stg_hdr;
	rc = xdb_stg_open (&pTblm->stg_mgr, path, xdb_init_table, NULL);
	XDB_EXPECT (rc == XDB_OK, XDB_ERROR, "Failed to create table '%s'", XDB_OBJ_NAME(pTblm));

	pTblm->row_cap = XDB_STG_CAP(&pTblm->stg_mgr);

	//pDbm->pObjm[pStmt->xoid] = pTblm;

	pTblm->pDbm = pDbm;

	if (pStmt->vfld_count) {
		pTblm->pVdatm = xdb_create_vdata (pTblm);
	}

#ifdef XDB_DEBUG
	xdb_dbglog ("ok create tbl '%s' fld %d size %d\n", XDB_OBJ_NAME(pTblm), pTblm->fld_count, pTblm->row_size);
	for (int i = 0; i < pTblm->fld_count; ++i) {
		xdb_field_t *pFld = &pTblm->pFields[i];
		xdb_print ("  %s %s %d off %d\n", XDB_OBJ_NAME(pFld), xdb_type2str(pFld->fld_type), pFld->fld_len, pFld->fld_off);
	}
#endif

	for (int i = 0; i < pStmt->idx_count; ++i) {
		xdb_stmt_idx_t *pStmtIdx = &pStmt->stmt_idx[i];
		pStmtIdx->pConn = pStmt->pConn;
		pStmtIdx->pTblm = pTblm;
		xdb_create_index (pStmtIdx, true);
	}

	for (int i = 0; i < pStmt->fkey_count; ++i) {
		xdb_stmt_fkey_t *pStmtFkey = &pStmt->stmt_fkey[i];
		pStmtFkey->pTblm = pTblm;
		xdb_create_fkey (pStmtFkey);
	}

	for (int i = 0; i < pTblm->fld_count; ++i) {
		if (pTblm->pFields[i].fld_flags & XDB_FLD_NOTNULL) {
			XDB_SET_NOTNULL (pTblm->pNullBytes, i);
		}
	}

	xdb_sysdb_add_tbl (pTblm, true, true, true);

	if (pStmt->xoid < 0) {
		xdb_gen_db_schema (pTblm->pDbm);
	}

	xdb_wrunlock_db (pDbm);

	return XDB_OK;

error:
	if (NULL != pTblm) {	
		xdb_objm_del (&pDbm->db_objm, pTblm);
	}
	xdb_wrunlock_db (pDbm);
	xdb_free (pTblm);
	return -pConn->conn_res.errcode;
}

XDB_STATIC void 
xdb_free_table (xdb_tblm_t *pTblm)
{
	xdb_free (pTblm->pMeta);
	xdb_objm_free (&pTblm->fld_objm);
	xdb_objm_free (&pTblm->idx_objm);
	for (int i=0; i< XDB_ARY_LEN(pTblm->trig_objm); ++i) {
		xdb_objm_free (&pTblm->trig_objm[i]);
	}
	xdb_free (pTblm->pFields);
	xdb_free (pTblm);
}

XDB_STATIC int 
xdb_close_table (xdb_tblm_t *pTblm)
{
	xdb_dbm_t*	pDbm = pTblm->pDbm;

	xdb_tbllog ("Close Table '%s'\n", XDB_OBJ_NAME(pTblm));

	xdb_sysdb_del_tbl (pTblm);

	int count = XDB_OBJM_MAX(pTblm->idx_objm);
	for (int i = 0; i < count; ++i) {
		xdb_idxm_t *pIdxm = XDB_OBJM_GET(pTblm->idx_objm, i);
		if (NULL != pIdxm) {
			xdb_close_index (pIdxm);
		}
	}

	char path[XDB_PATH_LEN + 32];
	xdb_sprintf (path, "%s/T%06d/xdb.dat", pDbm->db_path, XDB_OBJ_ID(pTblm));

	xdb_vdata_close (pTblm->pVdatm);

	xdb_stg_close (&pTblm->stg_mgr); 

	xdb_free_table (pTblm);

	return XDB_OK;
}

XDB_STATIC int 
xdb_drop_table (xdb_tblm_t *pTblm)
{
	xdb_dbm_t*	pDbm = pTblm->pDbm;

	xdb_wrlock_db (pDbm);

	xdb_tbllog ("Drop Table '%s'\n", XDB_OBJ_NAME(pTblm));

	xdb_sysdb_del_tbl (pTblm);

	int count = XDB_OBJM_MAX(pTblm->idx_objm);
	for (int i = 0; i < count; ++i) {
		xdb_idxm_t *pIdxm = XDB_OBJM_GET(pTblm->idx_objm, i);
		if (NULL != pIdxm) {
			xdb_drop_index (pIdxm);
		}
	}

	xdb_vdata_drop (pTblm->pVdatm);

	char path[XDB_PATH_LEN + 32];
	xdb_sprintf (path, "%s/T%06d/xdb.dat", pDbm->db_path, XDB_OBJ_ID(pTblm));
	xdb_stg_drop (&pTblm->stg_mgr, path); 

	if (!pTblm->bMemory) {
		xdb_sprintf (path, "%s/T%06d", pDbm->db_path, XDB_OBJ_ID(pTblm));
		xdb_rmdir (path);
	}

	xdb_objm_del (&pDbm->db_objm, pTblm);

	xdb_free_table (pTblm);

	xdb_gen_db_schema (pDbm);

	xdb_wrunlock_db (pDbm);

	return XDB_OK;
}

XDB_STATIC int 
xdb_dump_create_table (xdb_tblm_t *pTblm, char buf[], xdb_size size, uint32_t flags)
{
	xdb_size			len = 0;

	len += sprintf (buf+len, "CREATE TABLE%s %s%s%s (\n", (flags & XDB_DUMP_EXIST) ? " IF NOT EXISTS" : "", 
						(flags & XDB_DUMP_FULLNAME) ? XDB_OBJ_NAME(pTblm->pDbm) : "", 
						(flags & XDB_DUMP_FULLNAME) ? "." : "",
						XDB_OBJ_NAME(pTblm));

	// dump field
	for (int i = 0; i < pTblm->fld_count; ++i) {
		xdb_field_t *pFld = XDB_OBJM_GET(pTblm->fld_objm, i);
		//len += sprintf (buf+len, "  %-*s %s", 16, XDB_OBJ_NAME(pFld), xdb_type2str (pFld->fld_type));
		len += sprintf (buf+len, "  %-16s %s", XDB_OBJ_NAME(pFld), xdb_type2str (pFld->fld_type));
		if ((XDB_TYPE_CHAR == pFld->fld_type) || (XDB_TYPE_VCHAR == pFld->fld_type)) {
			len += sprintf (buf+len, "(%d) COLLATE %s", pFld->fld_len, (pFld->fld_flags&XDB_FLD_BINARY)?"BINARY":"NOCASE");
		} else if ((XDB_TYPE_BINARY == pFld->fld_type) || (XDB_TYPE_VBINARY == pFld->fld_type)) {
			len += sprintf (buf+len, "(%d)", pFld->fld_len);
		}
		if (pFld->fld_flags & XDB_FLD_NOTNULL) {
			len += sprintf (buf+len, " NOT NULL");
		}
		len += sprintf (buf+len, ",\n");
	}

	// dump index
	for (int i = 0; i < XDB_OBJM_MAX(pTblm->idx_objm); ++i) {
		xdb_idxm_t	*pIdxm = XDB_OBJM_GET(pTblm->idx_objm, i);
		if (pIdxm != NULL) {
			if (pIdxm->bPrimary) {
				len += sprintf (buf+len, "  PRIMARY KEY USING %s (", xdb_idx2str(pIdxm->idx_type));
			} else if (pIdxm->bUnique) {
				len += sprintf (buf+len, "  UNIQUE  KEY %s USING %s (", XDB_OBJ_NAME(pIdxm), xdb_idx2str(pIdxm->idx_type));
			} else {
				len += sprintf (buf+len, "  KEY         %s USING %s (", XDB_OBJ_NAME(pIdxm), xdb_idx2str(pIdxm->idx_type));
			}
			for (int j = 0; j < pIdxm->fld_count; ++j) {
				len += sprintf (buf+len, "%s,", XDB_OBJ_NAME(pIdxm->pFields[j]));
			}
			len --;
			if (0 == flags) {
				len += sprintf (buf+len, "),\n");
			} else {
				len += sprintf (buf+len, ") XOID=%d,\n", XDB_OBJ_ID(pIdxm));
			}
		}
	}

	buf[len-2] = '\n';
	buf[len-1] = ')';
	if (pTblm->bMemory && !pTblm->pDbm->bMemory) {
		len += sprintf (buf+len, " ENGINE=MEMORY");
	}
	if (flags&XDB_DUMP_XOID) {
		len += sprintf (buf+len, " XOID=%d", XDB_OBJ_ID(pTblm));
	}
	buf[len++] = ';';
	buf[len] = '\0';

	return len;
}

XDB_STATIC int 
xdb_flush_table (xdb_tblm_t *pTblm, uint32_t flags)
{
	xdb_wrlock_tblstg (pTblm);
	xdb_tbl_t *pTbl = XDB_TBLPTR(pTblm);
	if (pTbl->flush_id == pTbl->lastchg_id) {
		xdb_wrunlock_tblstg (pTblm);
		return XDB_OK;
	}

	xdb_tbllog ("Flush Table '%s' %"PRIu64" -> %"PRIu64"\n", XDB_OBJ_NAME(pTblm), pTbl->flush_id, pTbl->lastchg_id);

	pTbl->flush_id = pTbl->lastchg_id;

	xdb_wrunlock_tblstg (pTblm);

	xdb_stg_sync (&pTblm->stg_mgr,    0, 0, false); 

	xdb_flush_vdat (pTblm->pVdatm);

	xdb_flush_index (pTblm);

	return 0;
}

XDB_STATIC int 
__xdb_repair_table (xdb_tblm_t *pTblm, uint32_t flags)
{
	xdb_print ("  --- Begin Repair table %s\n", XDB_OBJ_NAME(pTblm));

	xdb_stg_init (&pTblm->stg_mgr);

	xdb_init_index (pTblm);

	xdb_vdat_mark (pTblm->pVdatm);

	xdb_stgmgr_t	*pStgMgr = &pTblm->stg_mgr;
	xdb_rowid max_rid = XDB_STG_MAXID(pStgMgr);
	xdb_stg_init (pStgMgr);
	for (xdb_rowid rid = 1; rid <= max_rid; ++rid) {
		void *pRow = XDB_IDPTR(pStgMgr, rid);
		uint8_t ctrl = XDB_ROW_CTRL (pStgMgr->pStgHdr, pRow) & XDB_ROW_MASK;
		if (XDB_ROW_COMMIT == ctrl) {
			if (pTblm->vfld_count > 0) {
				void *pVdat = xdb_row_vdata_get (pTblm, pRow);
				if (NULL != pVdat) {
					// 1000b
					*(uint32_t*)pVdat = (0x8 << XDB_VDAT_LENBITS) | (*(uint32_t*)pVdat & XDB_VDAT_LENMASK);
				}
			}
			xdb_idx_addRow (NULL, pTblm, rid, pRow);
		} else {
			xdb_stg_free (pStgMgr, rid, pRow);
		}
	}

	xdb_vdat_clean (pTblm->pVdatm);

	xdb_print ("  --- End Repair table %s\n", XDB_OBJ_NAME(pTblm));

	return XDB_OK;	
}
