/******************************************************************************
* Copyright (c) 2024-present JC Wang. All rights reserved
*
*   https://crossdb.org
*   https://github.com/crossdb-org/crossdb
*
* This program is free software: you can use, redistribute, and/or modify it under
* the terms of the GNU Lesser General Public License, version 3 or later ("LGPL"),
* as published by the Free Software Foundation.
*
* This program is distributed in the hope that it will be useful, but WITHOUT
* ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS 
* FOR A PARTICULAR PURPOSE. See the GNU published General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public License along with 
* this program. If not, see <http://www.gnu.org/licenses/>.
******************************************************************************/

XDB_STATIC void 
xdb_alloc_offset (xdb_tblm_t *pTblm, uint64_t types, int len)
{
	for (int i = 0; i < pTblm->fld_count; ++i) {
		xdb_field_t *pFld = &pTblm->pFields[i];
		int extra = 0;
		if (types & (1LL<<pFld->fld_type)) {
			if (0 == pFld->fld_len) {
				pFld->fld_len = len;
			}
			pFld->fld_off = pTblm->row_size;
			if (XDB_TYPE_CHAR == pFld->fld_type) {
				pFld->fld_off += 2;
				extra = 2 + 1; // len(2B) + '\0'
			}
			pTblm->row_size += pFld->fld_len + extra;
		}
	}
}

XDB_STATIC void
xdb_init_table (xdb_stgmgr_t *pStgMgr, void *pArg)
{
	xdb_tbl_t *pTbl = (xdb_tbl_t*)pStgMgr->pStgHdr;
	pTbl->row_count = 0;
}

XDB_STATIC int 
xdb_create_table (xdb_stmt_tbl_t *pStmt)
{
	int			rc;
	xdb_conn_t	*pConn = pStmt->pConn;
	xdb_dbm_t*	pDbm = pConn->pCurDbm;
	xdb_tblm_t *pTblm = NULL;

	if (NULL != pStmt->pTblm) {
		return XDB_OK;
	}

	XDB_EXPECT (XDB_OBJM_COUNT (pDbm->db_objm) < XDB_MAX_TBL, XDB_E_FULL, "Can create at most %d tables", XDB_MAX_TBL);

	pTblm = xdb_calloc (sizeof(xdb_tblm_t));

	XDB_EXPECT (NULL!=pTblm, XDB_E_MEMORY, "Can't alloc memory");

	pTblm->lock_mode = pDbm->lock_mode;
	pTblm->bMemory = pStmt->bMemory;
	if (pTblm->bMemory && (XDB_LOCK_PROCESS == pTblm->lock_mode)) {
		pTblm->lock_mode = XDB_LOCK_THREAD;
	}
		
	XDB_OBJ_ID(pTblm) = pStmt->xoid;
	xdb_strcpy (pTblm->obj.obj_name, pStmt->tbl_name);

	xdb_objm_add (&pDbm->db_objm, pTblm);

	char path[XDB_PATH_LEN + 32];
	xdb_sprintf (path, "%s/T%06d/xdb.dat", pDbm->db_path, XDB_OBJ_ID(pTblm));

	XDB_RWLOCK_INIT(pTblm->tbl_lock);
	XDB_RWLOCK_INIT(pTblm->stg_lock);

	pTblm->fld_count = pStmt->fld_count;

	// last 2B col_len=0 to mark end of meta
	pTblm->meta_size = sizeof(xdb_meta_t) + 4;

	int flds_size = sizeof(*pTblm->pFields) * pTblm->fld_count;
	pTblm->pFields = xdb_malloc (flds_size);
	XDB_EXPECT (NULL!=pTblm->pFields, XDB_E_MEMORY, "Can't alloc memory");
	memcpy (pTblm->pFields, pStmt->stmt_flds, flds_size);

	for (int i = 0; i < pTblm->fld_count; ++i) {
		pTblm->meta_size += (sizeof(xdb_col_t) + XDB_OBJ_NMLEN(&pTblm->pFields[i]) + 1 + 3) & (~3); // 4B align
		xdb_objm_add (&pTblm->fld_objm, &pTblm->pFields[i]);
	}

	//pTblm->tbl_fd = tbl_fd;
	pTblm->row_size = pStmt->row_size;
	if (pTblm->row_size < 1) {
		xdb_alloc_offset (pTblm, (1LL<<XDB_TYPE_BIGINT) | (1LL<<XDB_TYPE_DOUBLE), 8);
		xdb_alloc_offset (pTblm, (1LL<<XDB_TYPE_INT) | (1LL<<XDB_TYPE_FLOAT), 4);
		xdb_alloc_offset (pTblm, (1<<XDB_TYPE_CHAR) | (1<<XDB_TYPE_SMALLINT), 2);
		xdb_alloc_offset (pTblm, (1<<XDB_TYPE_TINYINT), 1);
		// TBD row_size need to add bitmap
	}

	// add fast pointer access
	int meta_size_align = (pTblm->meta_size + 2 + 7) & (~7); // 8B align
	xdb_meta_t *pMeta = xdb_calloc (meta_size_align + 8 * pTblm->fld_count);
	XDB_EXPECT (NULL!=pMeta, XDB_E_MEMORY, "Can't alloc memory");
	pTblm->pMeta = pMeta;
	pMeta->col_count = pTblm->fld_count;
	pMeta->null_off  = pTblm->null_off;
	pMeta->row_size	= pTblm->row_size;
	xdb_col_t	*pCol = pTblm->pMeta->cols;
	uint64_t	*pColPtr = (void*)pMeta + meta_size_align;
	pMeta->col_list = (uintptr_t)pColPtr;

	for (int i = 0; i < pTblm->fld_count; ++i) {
		xdb_field_t *pFld = &pTblm->pFields[i];
		pColPtr[i] = (uintptr_t)pCol;
		pCol->col_type 	= pFld->fld_type;
		pCol->col_off	= pFld->fld_off;
		pCol->col_nmlen = XDB_OBJ_NMLEN(pFld);
		memcpy (pCol->col_name, XDB_OBJ_NAME(pFld), pCol->col_nmlen + 1);
		pCol->col_len = (sizeof (*pCol) + pCol->col_nmlen + 1 + 3) & (~3); // 4B allign
		pCol = (void*)pCol + pCol->col_len;
	}
	pCol->col_len = 0;

	pMeta->len_type = pTblm->meta_size | (XDB_RET_META<<28);

	pTblm->blk_size = pTblm->row_size + 1;

	xdb_stghdr_t stg_hdr = {.stg_magic = 0xE7FCFDFB, .blk_size = pTblm->blk_size, .ctl_off = pTblm->blk_size - 1, .blk_off = XDB_OFFSET(xdb_tbl_t, pRowDat)};
	pTblm->stg_mgr.pOps = pTblm->bMemory ? &s_xdb_store_mem_ops : &s_xdb_store_file_ops;
	pTblm->stg_mgr.pStgHdr	= &stg_hdr;
	rc = xdb_stg_open (&pTblm->stg_mgr, path, xdb_init_table, NULL);
	XDB_EXPECT (rc == XDB_OK, XDB_ERROR, "Failed to create table '%s'", XDB_OBJ_NAME(pTblm));

	pTblm->row_cap = XDB_STG_CAP(&pTblm->stg_mgr);

	//pDbm->pObjm[pStmt->xoid] = pTblm;

	pTblm->pDbm = pDbm;

#ifdef XDB_DEBUG
	xdb_dbglog ("ok create tbl '%s' fld %d size %d\n", XDB_OBJ_NAME(pTblm), pTblm->fld_count, pTblm->row_size);
	for (int i = 0; i < pTblm->fld_count; ++i) {
		xdb_field_t *pFld = &pTblm->pFields[i];
		xdb_print ("  %s %s %d off %d\n", XDB_OBJ_NAME(pFld), xdb_type2str(pFld->fld_type), pFld->fld_len, pFld->fld_off);
	}
#endif

	for (int i = 0; i < pStmt->idx_count; ++i) {
		xdb_stmt_idx_t *pStmtIdx = &pStmt->stmt_idx[i];
		pStmtIdx->pTblm = pTblm;
		xdb_create_index (pStmtIdx, true);
	}

	xdb_sysdb_add_tbl (pTblm, true, true, true);

	if (pStmt->xoid < 0) {
		xdb_gen_db_schema (pTblm->pDbm);
	}

	return XDB_OK;

error:
	xdb_free (pTblm);
	return -pConn->conn_res.errcode;
}

XDB_STATIC void 
xdb_free_table (xdb_tblm_t *pTblm)
{
	xdb_free (pTblm->pMeta);
	xdb_objm_free (&pTblm->fld_objm);
	xdb_objm_free (&pTblm->idx_objm);
	xdb_free (pTblm->pFields);
	xdb_free (pTblm);
}

XDB_STATIC int 
xdb_close_table (xdb_tblm_t *pTblm)
{
	xdb_dbm_t*	pDbm = pTblm->pDbm;

	xdb_dbglog ("  ------ close table %s\n", XDB_OBJ_NAME(pTblm));

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

	xdb_stg_close (&pTblm->stg_mgr); 

	xdb_free_table (pTblm);

	return XDB_OK;
}

XDB_STATIC int 
xdb_drop_table (xdb_tblm_t *pTblm)
{
	xdb_dbm_t*	pDbm = pTblm->pDbm;

	xdb_sysdb_del_tbl (pTblm);

	int count = XDB_OBJM_MAX(pTblm->idx_objm);
	for (int i = 0; i < count; ++i) {
		xdb_idxm_t *pIdxm = XDB_OBJM_GET(pTblm->idx_objm, i);
		if (NULL != pIdxm) {
			xdb_drop_index (pIdxm);
		}
	}

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

	return XDB_OK;
}

XDB_STATIC xdb_tblm_t* 
xdb_find_table (xdb_dbm_t *pDbm, const char *tbl_name)
{
	return xdb_objm_get (&pDbm->db_objm, tbl_name);
}

XDB_STATIC int 
xdb_find_field (xdb_tblm_t *pTblm, const char *fld_name, int len)
{
	xdb_field_t *pFld = xdb_objm_get2 (&pTblm->fld_objm, fld_name, len);
	return pFld != NULL ? pFld->fld_id : -1;
}

XDB_STATIC int 
xdb_dump_create_table (xdb_tblm_t *pTblm, char buf[], xdb_size size, uint32_t flags)
{
	xdb_size			len = 0;

	len += sprintf (buf+len, "CREATE TABLE %s (\n", XDB_OBJ_NAME(pTblm));

	// dump field
	for (int i = 0; i < pTblm->fld_count; ++i) {
		xdb_field_t *pFld = XDB_OBJM_GET(pTblm->fld_objm, i);
		//len += sprintf (buf+len, "  %-*s %s", 16, XDB_OBJ_NAME(pFld), xdb_type2str (pFld->fld_type));
		len += sprintf (buf+len, "  %-16s %s", XDB_OBJ_NAME(pFld), xdb_type2str (pFld->fld_type));
		if (XDB_TYPE_CHAR == pFld->fld_type) {
			len += sprintf (buf+len, "(%d)", pFld->fld_len);
		}
		len += sprintf (buf+len, ",\n");
	}

	// dump index
	for (int i = 0; i < XDB_OBJM_MAX(pTblm->idx_objm); ++i) {
		xdb_idxm_t	*pIdxm = XDB_OBJM_GET(pTblm->idx_objm, i);
		if (pIdxm != NULL) {
			if (!strcasecmp (XDB_OBJ_NAME(pIdxm), "PRIMARY")) {
				len += sprintf (buf+len, "  PRIMARY KEY (");
			} else if (pIdxm->bUnique) {
				len += sprintf (buf+len, "  UNIQUE  KEY %s (", XDB_OBJ_NAME(pIdxm));
			} else {
				len += sprintf (buf+len, "  KEY         %s (", XDB_OBJ_NAME(pIdxm));
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
	if (0 == flags) {
		len += sprintf (buf+len, ";\n");
	} else {
		len += sprintf (buf+len, " XOID=%d;\n", XDB_OBJ_ID(pTblm));
	}

	if (0 == flags) {
		len --;
		buf[len] = '\0';
	}

	return len;
}

XDB_STATIC int 
xdb_flush_table (xdb_tblm_t *pTblm, uint32_t flags)
{
	xdb_stg_sync (&pTblm->stg_mgr,    0, 0, false); 

	int count = XDB_OBJM_MAX(pTblm->idx_objm);
	for (int i = 0; i < count; ++i) {
		xdb_idxm_t *pIdxm = XDB_OBJM_GET(pTblm->idx_objm, i);
		if (NULL != pIdxm) {
			xdb_flush_index (pIdxm);
		}
	}

	return 0;
}
