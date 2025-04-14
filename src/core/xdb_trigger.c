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
	
typedef struct xdb_trigfunc_t {
	xdb_obj_t		obj;
	char			lang[16];
	char			func_name[XDB_NAME_LEN * 2];
	xdb_trig_callback cb_func;
	void			*pArg;
} xdb_trigfunc_t;

typedef struct xdb_trig_t {
	xdb_obj_t		obj;
	xdb_tblm_t		*pTblm;
	int				priority;
	xdb_trigfunc_t	*pTrigf;
} xdb_trig_t;

static xdb_objm_t s_xdb_trigfunc;

XDB_STATIC xdb_trigfunc_t* 
xdb_find_trigfunc (const char *name)
{
	return xdb_objm_get (&s_xdb_trigfunc, name);
}

int 
xdb_create_func (const char *name, xdb_func_e type, const char *lang, void *cb_func, void *pArg)
{
	xdb_trigfunc_t *pTrigf = xdb_find_trigfunc (name);

	if (type >= XDB_FUNC_MAX) {
		return -XDB_E_PARAM;
	}

	if (NULL != pTrigf) {
		return -XDB_E_EXISTS;
	}

	pTrigf = xdb_calloc (sizeof (xdb_trigfunc_t));
	if (NULL == pTrigf) {
		return -XDB_E_MEMORY;
	}

	xdb_strcpy (XDB_OBJ_NAME(pTrigf), name);
	XDB_OBJ_ID(pTrigf) = -1;

	xdb_strcpy (pTrigf->lang, lang);
	pTrigf->cb_func = cb_func;
	pTrigf->pArg = pArg;
	
	xdb_objm_add (&s_xdb_trigfunc, pTrigf);
	return XDB_OK;
}

XDB_STATIC xdb_trig_t* 
xdb_find_trigger (xdb_tblm_t *pTblm, const char *name)
{
	for (xdb_trig_e type = 0; type < XDB_TRIG_MAX; ++type) {
		xdb_trig_t *pTrig = xdb_objm_get (&pTblm->trig_objm[type], name);
		if (NULL != pTrig) {
			return pTrig;
		}
	}
	return NULL;
}

// when for newrow
XDB_STATIC int 
xdb_create_triggerEx (xdb_conn_t *pConn, const char *name, xdb_trig_e type, xdb_tblm_t *pTblm, const char *func_name, 
								const char *when, const char *upd_cols, int priority)
{
	xdb_trigfunc_t *pTrigf = xdb_find_trigfunc (func_name);
	if (NULL == pTrigf) {
		xdb_errlog ("Can't find trig func %s\n", func_name);
		return -XDB_E_NOTFOUND;
	}

#if 0	
	char *tblname = strchr (tbl_name, '.');
	if (NULL != tblname) {
		char db_name[XDB_NAME_LEN+1];
		memcpy (db_name, tbl_name, tblname-tbl_name);
		db_name[tblname-tbl_name] = '\0';
		pDbm = xdb_find_db (db_name);
		if (NULL == pDbm) {
			xdb_errlog ("Can't find db %s\n", db_name);
			return -XDB_E_NOTFOUND;
		}
		tbl_name = tblname + 1;
	}
	
	xdb_tblm_t		*pTblm = xdb_find_table (pDbm, tbl_name);
	if (NULL == pTblm) {
		xdb_errlog ("Can't find tbl %s\n", tbl_name);
		return -1;
	}
#endif

	xdb_trig_t *pTrig = xdb_find_trigger (pTblm, name);
	if (NULL != pTrig) {
		return -XDB_E_EXISTS;
	}

	pTrig = xdb_calloc (sizeof (xdb_trig_t));
	if (NULL == pTrig) {
		return -XDB_E_MEMORY;
	}

	xdb_strcpy (XDB_OBJ_NAME(pTrig), name);
	pTrig->pTrigf = pTrigf;
	pTrig->priority = priority;
	xdb_objm_add (&pTblm->trig_objm[type], pTrig);

	return XDB_OK;
}

int 
xdb_create_trigger (xdb_stmt_trig_t *pStmt)
{
	return xdb_create_triggerEx (pStmt->pConn, pStmt->trig_name, pStmt->trig_type, pStmt->pTblm, pStmt->func_name, NULL, NULL, 0);
}

int xdb_call_trigger (xdb_conn_t *pConn, xdb_tblm_t *pTblm, xdb_trig_e type, xdb_row_t *pNewRow, xdb_row_t *pOldRow)
{
	int count = XDB_OBJM_COUNT(pTblm->trig_objm[type]);
	xdb_objm_t	*pTrigObj = &pTblm->trig_objm[type];
	xdb_res_t	res;
	res.col_meta = (uintptr_t)pTblm->pMeta;
	res.row_data = (uintptr_t)pTblm;
	for (int i = 0; i < count; ++i) {
		xdb_trig_t *pTrig = XDB_OBJM_GET(*pTrigObj, i);
		pTrig->pTrigf->cb_func (pConn, &res, type, pNewRow, pOldRow, pTrig->pTrigf->pArg);
	}
	return XDB_OK;
}

