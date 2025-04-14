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

#if XDB_LOG_FLAGS & XDB_LOG_IDX
#define xdb_idxlog(...)	xdb_print(__VA_ARGS__)
#else
#define xdb_idxlog(...)
#endif


XDB_STATIC xdb_idxm_t* 
xdb_find_index (xdb_tblm_t *pTblm, const char *idx_name)
{
	return xdb_objm_get (&pTblm->idx_objm, idx_name);
}

XDB_STATIC int 
xdb_idx_addRow (xdb_conn_t *pConn, xdb_tblm_t *pTblm, xdb_rowid rid, void *pRow)
{
	int rc;
	for (int i = 0; i < XDB_OBJM_COUNT(pTblm->idx_objm); ++i) {
		xdb_idxm_t *pIdxm = XDB_OBJM_GET(pTblm->idx_objm, pTblm->idx_order[i]);
		rc = pIdxm->pIdxOps->idx_add (pConn, pIdxm, rid, pRow);
		if (xdb_unlikely (rc != XDB_OK)) {
			// recover added index
			for (int j = 0; j < i; ++j) {
				xdb_idxm_t *pIdxm = XDB_OBJM_GET(pTblm->idx_objm, pTblm->idx_order[i]);
				pIdxm->pIdxOps->idx_rem (pIdxm, rid, pRow);
			}
			return rc;
		}
	}
	return 0;
}

XDB_STATIC int 
xdb_idx_addRow_bmp (xdb_conn_t *pConn, xdb_tblm_t *pTblm, xdb_rowid rid, void *pRow, uint8_t *idx_affect, int count)
{
	int rc;
	for (int i = 0; i < count; ++i) {
		xdb_idxm_t *pIdxm = XDB_OBJM_GET(pTblm->idx_objm, idx_affect[i]);
		rc = pIdxm->pIdxOps->idx_add (pConn, pIdxm, rid, pRow);
		if (xdb_unlikely (rc != XDB_OK)) {
			// recover added index
			for (int j = 0; j < i; ++j) {
				xdb_idxm_t *pIdxm = XDB_OBJM_GET(pTblm->idx_objm, pTblm->idx_order[i]);
				pIdxm->pIdxOps->idx_rem (pIdxm, rid, pRow);
			}
			return rc;
		}
	}
	return 0;
}

XDB_STATIC int 
xdb_idx_remRow_bmp (xdb_tblm_t *pTblm, xdb_rowid rid, void *pRow, uint8_t *idx_affect, int count)
{
	for (int i = 0; i < count; ++i) {
		xdb_idxm_t *pIdxm = XDB_OBJM_GET(pTblm->idx_objm, idx_affect[i]);
		pIdxm->pIdxOps->idx_rem (pIdxm, rid, pRow);
	}
	return count;
}

XDB_STATIC int 
xdb_idx_remRow (xdb_tblm_t *pTblm, xdb_rowid rid, void *pRow)
{
	for (int i = 0; i < XDB_OBJM_COUNT(pTblm->idx_objm); ++i) {
		xdb_idxm_t *pIdxm = XDB_OBJM_GET(pTblm->idx_objm, pTblm->idx_order[i]);
		pIdxm->pIdxOps->idx_rem (pIdxm, rid, pRow);
	}
	return 0;
}

static xdb_idx_ops s_xdb_hash_ops;
static xdb_idx_ops s_xdb_rbtree_ops;

static xdb_idx_ops *s_xdb_idx_ops[] = {
	[XDB_IDX_HASH]		= &s_xdb_hash_ops,
	[XDB_IDX_RBTREE]	= &s_xdb_rbtree_ops,
};


XDB_STATIC void 
xdb_index_order (xdb_tblm_t	*pTblm)
{
	int id = 0;
	for (int i = 0; i < XDB_OBJM_MAX(pTblm->idx_objm); ++i) {
		if (XDB_OBJM_GET(pTblm->idx_objm, i) != NULL) {
			pTblm->idx_order[id++] = i;
		}
	}
}

XDB_STATIC int 
xdb_create_index (xdb_stmt_idx_t *pStmt, bool bCreateTbl)
{
	xdb_conn_t		*pConn = pStmt->pConn;
	xdb_idxlog ("create index %s on %s %d (", pStmt->idx_name, pStmt->XDB_OBJ_NAME(pTblm), pStmt->fld_count);

	int rc = -1;
	xdb_tblm_t	*pTblm = pStmt->pTblm;
	xdb_idxm_t *pIdxm = xdb_find_index (pTblm, pStmt->idx_name);

	if (NULL != pIdxm) {
		return XDB_OK;
	}

	XDB_EXPECT (XDB_OBJM_COUNT (pTblm->idx_objm) < XDB_MAX_INDEX, XDB_E_FULL, "Can create at most %d indexes", XDB_MAX_INDEX);

	pIdxm = xdb_calloc (sizeof(xdb_idxm_t));
	if (NULL == pIdxm) {
		goto error;
	}

	pIdxm->pTblm = pStmt->pTblm;
	xdb_strcpy (XDB_OBJ_NAME(pIdxm), pStmt->idx_name);
	XDB_OBJ_ID(pIdxm) = pStmt->xoid;
	xdb_objm_add (&pTblm->idx_objm, pIdxm);

	pIdxm->fld_count = pStmt->fld_count;
	pIdxm->idx_type = pStmt->idx_type;
	pIdxm->bUnique = pStmt->bUnique;
	pIdxm->bPrimary = pStmt->bPrimary;
	pIdxm->idx_type = pStmt->idx_type;

	for (int i = 0; i < pIdxm->fld_count; ++i) {
		pIdxm->pFields[i] = xdb_find_field (pStmt->pTblm, pStmt->idx_col[i], 0);
		XDB_EXPECT (pIdxm->pFields[i] != NULL, XDB_E_STMT, "Can't find field '%s'", pStmt->idx_col[i]);
		pIdxm->pFields[i]->idx_fid[XDB_OBJ_ID(pIdxm)] = i;
		pIdxm->pFields[i]->idx_bmp |= (1<<XDB_OBJ_ID(pIdxm));
		if (pIdxm->bPrimary) {
			pIdxm->pFields[i]->fld_flags |= (XDB_FLD_NOTNULL | XDB_FLD_PRIKEY);
			pIdxm->pTblm->bPrimary = true;
		} else if  (pIdxm->bUnique) {
			pIdxm->pFields[i]->fld_flags |= XDB_FLD_UNIKEY;
		}
		if (++pIdxm->pFields[i]->fld_idxnum > 1) {
			pIdxm->pFields[i]->fld_flags |= XDB_FLD_MULKEY;
		}
	}

	pIdxm->pIdxOps = s_xdb_idx_ops[pIdxm->idx_type];
	rc = pIdxm->pIdxOps->idx_create (pIdxm);
	if (rc != 0) {
		goto error;
	}

	xdb_index_order (pTblm);

	if (pStmt->xoid < 0) {
		xdb_stgmgr_t	 *pStgMgr = &pTblm->stg_mgr;
		xdb_rowid		max_rid = XDB_STG_MAXID(pStgMgr);
		
		for (xdb_rowid rid = 1; rid <= max_rid; ++rid) {
			void *pRow = XDB_IDPTR(pStgMgr, rid);
			if (xdb_row_valid (pConn, pTblm, pRow, rid)) {
				pIdxm->pIdxOps->idx_add (pConn, pIdxm, rid, pRow);
			}
		}
		if (!bCreateTbl) {
			xdb_gen_db_schema (pTblm->pDbm);
		}
	}

	if (!bCreateTbl) {
		xdb_sysdb_add_idx (pIdxm);
	}

	return XDB_OK;

error:
	return rc;
}

XDB_STATIC int 
xdb_close_index (xdb_idxm_t *pIdxm)
{
	xdb_tblm_t	*pTblm = pIdxm->pTblm;

	xdb_idxlog ("    ------ close index %s\n", XDB_OBJ_NAME(pIdxm));

	for (int i = 0; i < pIdxm->fld_count; ++i) {
		pIdxm->pFields[i]->idx_fid[XDB_OBJ_ID(pIdxm)] = -1;
	}

	xdb_objm_del (&pTblm->idx_objm, pIdxm);

	xdb_index_order (pTblm);

	pIdxm->pIdxOps->idx_close (pIdxm);

	xdb_sysdb_del_idx (pIdxm);

	xdb_free (pIdxm);

	return XDB_OK;
}

XDB_STATIC int 
xdb_drop_index (xdb_idxm_t *pIdxm)
{
	xdb_tblm_t	*pTblm = pIdxm->pTblm;

	for (int i = 0; i < pIdxm->fld_count; ++i) {
		pIdxm->pFields[i]->idx_fid[XDB_OBJ_ID(pIdxm)] = -1;
		pIdxm->pFields[i]->idx_bmp &= ~(1<<XDB_OBJ_ID(pIdxm));
	}

	xdb_objm_del (&pTblm->idx_objm, pIdxm);

	xdb_index_order (pTblm);

	pIdxm->pIdxOps->idx_drop (pIdxm);

	xdb_sysdb_del_idx (pIdxm);

	xdb_free (pIdxm);

	xdb_gen_db_schema (pTblm->pDbm);

	return XDB_OK;
}

#if 0
XDB_STATIC int 
xdb_dump_create_index ()
{
	for (int i = 0; i < XDB_OBJM_MAX(pTblm->idx_objm); ++i) {
		xdb_idxm_t	*pIdxm = XDB_OBJM_GET(pTblm->idx_objm, i);
		if (pIdxm != NULL) {
			len += sprintf (buf+len, "CREATE INDEX %s ON %s (", XDB_OBJ_NAME(pIdxm), XDB_OBJ_NAME(pTblm));
			for (int j = 0; j < pIdxm->fld_count; ++j) {
				len += sprintf (buf+len, "%s,", XDB_OBJ_NAME(pIdxm->pFields[j]));
			}
			len --;
			if (0 == flags) {
				len += sprintf (buf+len, ");\n");
			} else {
				len += sprintf (buf+len, ") XOID=%d;\n", XDB_OBJ_ID(pIdxm));
			}
		}
	}
}
#endif

XDB_STATIC void 
xdb_flush_index (xdb_tblm_t *pTblm)
{
	int count = XDB_OBJM_MAX(pTblm->idx_objm);
	for (int i = 0; i < count; ++i) {
		xdb_idxm_t *pIdxm = XDB_OBJM_GET(pTblm->idx_objm, i);
		if (NULL != pIdxm) {
			pIdxm->pIdxOps->idx_sync (pIdxm);
		}
	}
}

XDB_STATIC void 
xdb_init_index (xdb_tblm_t *pTblm)
{
	int count = XDB_OBJM_MAX(pTblm->idx_objm);
	for (int i = 0; i < count; ++i) {
		xdb_idxm_t *pIdxm = XDB_OBJM_GET(pTblm->idx_objm, i);
		if (NULL != pIdxm) {
			pIdxm->pIdxOps->idx_init (pIdxm);
		}
	}
}

XDB_STATIC const char* 
xdb_idx2str(xdb_idx_type tp) 
{
	const char *id2str[] = {
		[XDB_IDX_HASH	] = "HASH",
		[XDB_IDX_RBTREE	] = "RBTREE",
	};
	return tp <= XDB_ARY_LEN(id2str) ? id2str[tp] : "Unknown";
}

