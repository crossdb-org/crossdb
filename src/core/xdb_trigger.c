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

typedef struct xdb_trig_t {
	xdb_obj_t		obj;
	xdb_tblm_t		*pTblm;
	int				priority;
	xdb_trig_callback cb_func;
	void 			*pArg;
} xdb_trig_t;

// when for newrow
int xdb_create_triggerEx (xdb_conn_t *pConn, const char *name, const char *tbl_name, 
								int type, xdb_trig_callback cb_func, void *pArg, 
								int priority, const char *when, const char *upd_cols)
{
	xdb_tblm_t		*pTblm = xdb_find_table (pConn->pCurDbm, tbl_name);
	if (NULL == pTblm) {
		return -1;
	}
	xdb_trig_t *pTrig = xdb_calloc (sizeof (xdb_trig_t));
	if (NULL == pTrig) {
		return -1;
	}
	xdb_strcpy (pTrig->obj.obj_name, name);
	pTrig->cb_func = cb_func;
	pTrig->priority = priority;
	xdb_objm_add (&pTblm->trig_objm[type], pTrig);

	return XDB_OK;
}

int xdb_create_trigger (xdb_conn_t *pConn, const char *name, const char *tbl_name, 
								int type, xdb_trig_callback cb_func, void *pArg)
{
	return xdb_create_triggerEx (pConn, name, tbl_name, type, cb_func, pArg, 0, NULL, NULL);
}

int xdb_call_trigger (xdb_conn_t *pConn, xdb_tblm_t *pTblm, uint32_t type, xdb_row_t *pNewRow, xdb_row_t *pOldRow)
{
	int count = XDB_OBJM_COUNT(pTblm->trig_objm[type]);
	xdb_objm_t	*pTrigObj = &pTblm->trig_objm[type];
	for (int i = 0; i < count; ++i) {
		xdb_trig_t *pTrig = XDB_OBJM_GET(*pTrigObj, i);
		pTrig->cb_func (pConn, (uintptr_t)pTblm->pMeta, type, pNewRow, pOldRow, pTrig->pArg);
	}
	return XDB_OK;
}

