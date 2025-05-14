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

#if XDB_LOG_FLAGS & XDB_LOG_TRANS
#define xdb_translog(...)	xdb_print(__VA_ARGS__)
#else
#define xdb_translog(...)
#endif

XDB_STATIC bool 
xdb_trans_getrow (xdb_conn_t *pConn, xdb_tblm_t *pTblm, xdb_rowid rid, bool bNew)
{
	int db_xoid = XDB_OBJ_ID(pTblm->pDbm);
	int tbl_xoid = XDB_OBJ_ID(pTblm);

	xdb_dbTrans_t	*pDbTrans = pConn->pDbTrans[db_xoid];
	if (NULL == pDbTrans) {
		return false;
	}
	xdb_tblTrans_t	*pTblTrans = pDbTrans->pTblTrans[tbl_xoid];
	if (NULL == pTblTrans) {
		return false;
	}

	return xdb_bmp_get (bNew ? &pTblTrans->new_rows : &pTblTrans->del_rows, rid);
}

XDB_STATIC int 
xdb_trans_addrow (xdb_conn_t *pConn, xdb_tblm_t *pTblm, xdb_rowid rid, bool bNew)
{
	int db_xoid = XDB_OBJ_ID(pTblm->pDbm);
	int tbl_xoid = XDB_OBJ_ID(pTblm);

	xdb_translog ("%s %d from '%s'\n", bNew?"add new":"del old", rid, XDB_OBJ_NAME(pTblm));

	xdb_dbTrans_t	*pDbTrans  = pConn->pDbTrans[db_xoid];
	xdb_tblTrans_t	*pTblTrans = pDbTrans->pTblTrans[tbl_xoid];
	if (NULL == pTblTrans) {
		pTblTrans = xdb_malloc (sizeof (xdb_tblTrans_t));
		XDB_EXPECT (NULL != pTblTrans, XDB_E_MEMORY, "No memory");
		pDbTrans->pTblTrans[tbl_xoid] = pTblTrans;
		xdb_tbltrans_init (pTblTrans);
	}
	pTblTrans->pTblm = pTblm;
	pTblTrans->pDbTrans = pDbTrans;

	xdb_lv2bmp_set (&pDbTrans->tbl_rows, tbl_xoid);

	xdb_bmp_set (bNew ? &pTblTrans->new_rows : &pTblTrans->del_rows, rid);

	return XDB_OK;
error:
	return -XDB_ERROR;
}

XDB_STATIC int 
xdb_trans_delrow (xdb_conn_t *pConn, xdb_tblm_t *pTblm, xdb_rowid rid)
{
	int db_xoid  = XDB_OBJ_ID(pTblm->pDbm);
	int tbl_xoid = XDB_OBJ_ID(pTblm);

	xdb_translog ("del new %d from '%s'\n", rid, XDB_OBJ_NAME(pTblm));

	xdb_dbTrans_t	*pDbTrans = pConn->pDbTrans[db_xoid];
	xdb_tblTrans_t	*pTblTrans = pDbTrans->pTblTrans[tbl_xoid];

	xdb_bmp_clr (&pTblTrans->new_rows, rid);
	return XDB_OK;
}

XDB_STATIC void 
xdb_dbtrans_init (xdb_dbTrans_t *pDbTrans)
{
	xdb_lv2bmp_init (&pDbTrans->tbl_wrlocks);
	xdb_lv2bmp_init (&pDbTrans->tbl_rdlocks);
	xdb_lv2bmp_init (&pDbTrans->tbl_rows);
}

XDB_STATIC void 
xdb_tbltrans_init (xdb_tblTrans_t *pTblTrans)
{
	xdb_bmp_init (&pTblTrans->new_rows);
	xdb_bmp_init (&pTblTrans->del_rows);
}

#if 0
XDB_STATIC int 
xdb_rdlock_table (xdb_conn_t *pConn, xdb_tblm_t *pTblm) 
{
	int db_xoid  = XDB_OBJ_ID(pTblm->pDbm);
	int tbl_xoid = XDB_OBJ_ID(pTblm);

	if (xdb_unlikely (!pConn->bInTrans)) {
		xdb_begin2 (pConn, pConn->bAutoCommit);
	}

	xdb_lv2bmp_set (&pConn->dbTrans_bmp, db_xoid);

	xdb_dbTrans_t *pDbTrans = pConn->pDbTrans[db_xoid];
	if (NULL == pDbTrans) {
		pDbTrans = xdb_calloc (sizeof(*pConn->pDbTrans[0]) + sizeof(void*) * XDB_MAX_TBL);
		XDB_EXPECT (NULL != pDbTrans, XDB_E_MEMORY, "No memory");
		xdb_dbtrans_init (pDbTrans);
	}
	pConn->pDbTrans[db_xoid] = pDbTrans;

	if (xdb_lv2bmp_get (&pDbTrans->tbl_wrlocks, tbl_xoid) ||
		xdb_lv2bmp_get (&pDbTrans->tbl_rdlocks, tbl_xoid)) {
		return XDB_OK;
	}

	xdb_translog ("read lock table '%s'\n", XDB_OBJ_NAME(pTblm));

	if (XDB_LOCK_THREAD == pTblm->lock_mode) {
		xdb_rwlock_rdlock (&pTblm->tbl_lock);
	} else if (XDB_LOCK_PROCESS == pTblm->lock_mode) {
		xdb_file_rdlock (pTblm->stg_mgr.stg_fd, 0, 1);
	}

	xdb_lv2bmp_clr (&pDbTrans->tbl_rdlocks, tbl_xoid);

	return XDB_OK;

error:
	return -XDB_ERROR;
}
#endif

XDB_STATIC int 
xdb_rdunlock_table (xdb_tblm_t *pTblm) 
{
	xdb_translog ("read unlock table '%s'\n", XDB_OBJ_NAME(pTblm));

	if (XDB_LOCK_THREAD == pTblm->lock_mode) {
		xdb_rwlock_rdunlock (&pTblm->tbl_lock);
	} else if (XDB_LOCK_PROCESS == pTblm->lock_mode) {
		xdb_file_unlock (pTblm->stg_mgr.stg_fd, 0, 1);
	}

	return XDB_OK;
}

XDB_STATIC int 
xdb_wrlock_table (xdb_conn_t *pConn, xdb_tblm_t *pTblm) 
{
	int db_xoid = XDB_OBJ_ID(pTblm->pDbm);
	int tbl_xoid = XDB_OBJ_ID(pTblm);

	if (xdb_unlikely (!pConn->bInTrans)) {
		xdb_begin2 (pConn, pConn->bAutoCommit);
	}

	xdb_lv2bmp_set (&pConn->dbTrans_bmp, db_xoid);

	xdb_dbTrans_t *pDbTrans = pConn->pDbTrans[db_xoid];
	if (NULL == pDbTrans) {
		pDbTrans = xdb_calloc (sizeof(*pConn->pDbTrans[0]) + sizeof(void*) * XDB_MAX_TBL);
		XDB_EXPECT (NULL != pDbTrans, XDB_E_MEMORY, "No memory");
		xdb_dbtrans_init (pDbTrans);
	}
	pDbTrans->pDbm = pTblm->pDbm;
	pConn->pDbTrans[db_xoid] = pDbTrans;

	if (xdb_lv2bmp_get (&pDbTrans->tbl_wrlocks, tbl_xoid)) {
		return XDB_OK;
	} else if (xdb_lv2bmp_get (&pDbTrans->tbl_rdlocks, tbl_xoid)) {
		// release readlock
		xdb_rdunlock_table (pTblm);
		xdb_lv2bmp_clr (&pDbTrans->tbl_rdlocks, tbl_xoid);
	}

	xdb_translog ("write lock table '%s'\n", XDB_OBJ_NAME(pTblm));

	if (XDB_LOCK_THREAD == pTblm->lock_mode) {
		xdb_rwlock_wrlock (&pTblm->tbl_lock);
	} else if (XDB_LOCK_PROCESS == pTblm->lock_mode) {
		xdb_file_wrlock (pTblm->stg_mgr.stg_fd, 0, 1);
	}

	xdb_lv2bmp_set (&pDbTrans->tbl_wrlocks, tbl_xoid);

	return XDB_OK;

error:
	return -XDB_ERROR;
}

XDB_STATIC int 
xdb_wrunlock_table (xdb_tblm_t *pTblm) 
{
	xdb_translog ("write unlock table '%s'\n", XDB_OBJ_NAME(pTblm));

	if (XDB_LOCK_THREAD == pTblm->lock_mode) {
		xdb_rwlock_wrunlock (&pTblm->tbl_lock);
	} else if (XDB_LOCK_PROCESS == pTblm->lock_mode) {
		xdb_file_unlock (pTblm->stg_mgr.stg_fd, 0, 1);
	}

	return XDB_OK;
}

XDB_STATIC int 
xdb_trans_tbl_wrunlock (uint32_t tid, void *pArg)
{
	xdb_dbm_t *pDbm = pArg;
	xdb_tblm_t *pTblm = XDB_OBJM_GET(pDbm->db_objm, tid);
	xdb_wrunlock_table (pTblm);
	return XDB_OK;
}

XDB_STATIC int 
xdb_trans_tbl_rdunlock (uint32_t tid, void *pArg)
{
	xdb_dbm_t *pDbm = pArg;
	xdb_tblm_t *pTblm = XDB_OBJM_GET(pDbm->db_objm, tid);
	xdb_rdunlock_table (pTblm);
	return XDB_OK;
}

XDB_STATIC int 
xdb_trans_db_unlock (uint32_t did, void *pArg)
{
	xdb_conn_t *pConn = pArg;
	xdb_dbTrans_t *pDbTrans = pConn->pDbTrans[did];
	xdb_dbm_t *pDbm = XDB_OBJM_GET(s_xdb_db_list, did);

	xdb_translog ("  unlock db %s\n", XDB_OBJ_NAME(pDbm));

	xdb_lv2bmp_iterate (&pDbTrans->tbl_wrlocks, xdb_trans_tbl_wrunlock, pDbm);
	xdb_lv2bmp_iterate (&pDbTrans->tbl_rdlocks, xdb_trans_tbl_rdunlock, pDbm);

	xdb_dbtrans_init (pDbTrans);

	return XDB_OK;
}

XDB_STATIC int 
xdb_trans_newrow_commit (uint32_t rid, void *pArg)
{
	xdb_tblTrans_t	*pTblTrans = pArg;
	xdb_stgmgr_t	*pStgMgr = &pTblTrans->pTblm->stg_mgr;

	xdb_translog ("      commit new row %d\n", rid);

	xdb_rowid *pRow = XDB_IDPTR(pStgMgr, rid);
	// change TRANS->COMMIT
	XDB_ROW_CTRL (pStgMgr->pStgHdr, pRow) &= ~XDB_ROW_MASK;
	XDB_ROW_CTRL (pStgMgr->pStgHdr, pRow) |= XDB_ROW_COMMIT;

	return XDB_OK;
}

XDB_STATIC int 
xdb_trans_delrow_commit (uint32_t rid, void *pArg)
{
	xdb_tblTrans_t	*pTblTrans = pArg;
	xdb_stgmgr_t	*pStgMgr = &pTblTrans->pTblm->stg_mgr;

	xdb_translog ("      commit del row %d\n", rid);

	xdb_rowid *pRow = XDB_IDPTR(pStgMgr, rid);

	xdb_idx_remRow (pTblTrans->pTblm, rid, pRow);
	__xdb_row_delete (pTblTrans->pTblm, rid, pRow);

	return XDB_OK;
}

XDB_STATIC int 
xdb_trans_tbl_commit (uint32_t tid, void *pArg)
{
	xdb_dbTrans_t *pDbTrans = pArg;
	xdb_tblTrans_t	*pTblTrans = pDbTrans->pTblTrans[tid];

	xdb_translog ("    commit table '%s'\n", XDB_OBJ_NAME(pTblTrans->pTblm));

	xdb_mark_dirty (pTblTrans->pTblm);

	// iterate new rows -> commit
	xdb_bmp_iterate (&pTblTrans->new_rows, xdb_trans_newrow_commit, pTblTrans);

	// iterate del rows -> delete
	xdb_bmp_iterate (&pTblTrans->del_rows, xdb_trans_delrow_commit, pTblTrans);

	xdb_tbltrans_init (pTblTrans);

	return XDB_OK;		
}

XDB_STATIC int 
xdb_trans_db_commit (uint32_t did, void *pArg)
{
	xdb_conn_t *pConn = pArg;
	xdb_dbTrans_t *pDbTrans = pConn->pDbTrans[did];
	xdb_translog ("  commit db '%s'\n", XDB_OBJ_NAME((xdb_dbm_t*)XDB_OBJM_GET(s_xdb_db_list, did)));
	xdb_lv2bmp_iterate (&pDbTrans->tbl_rows, xdb_trans_tbl_commit, pDbTrans);

	return XDB_OK;
}

XDB_STATIC void 
xdb_trans_unlock (xdb_conn_t *pConn)
{
	xdb_lv2bmp_iterate (&pConn->dbTrans_bmp, xdb_trans_db_unlock, pConn);
	pConn->bInTrans = false;
	pConn->bAutoTrans = false;
}

xdb_ret 
xdb_commit (xdb_conn_t *pConn)
{
	if (xdb_unlikely (pConn->conn_client)) {
		xdb_res_t *pRes = xdb_exec (pConn, "COMMIT");
		return -pRes->errcode;
	}

	if (xdb_unlikely (!pConn->bInTrans)) {
		return XDB_OK;
	}
	xdb_translog ("Commit Transaction %s\n", pConn->bAutoTrans ? "AUTO" : "");

	// write wal for each DB
	xdb_lv2bmp_iterate (&pConn->dbTrans_bmp, xdb_trans_db_wal, pConn);

	// commit each DB
	xdb_lv2bmp_iterate (&pConn->dbTrans_bmp, xdb_trans_db_commit, pConn);

	// release each DB locks
	xdb_trans_unlock (pConn);

	return XDB_OK;
}

XDB_STATIC int 
xdb_trans_tbl_rollback (uint32_t tid, void *pArg)
{
	xdb_dbTrans_t *pDbTrans = pArg;
	xdb_tblTrans_t	*pTblTrans = pDbTrans->pTblTrans[tid];

	xdb_mark_dirty (pTblTrans->pTblm);

	// iterate new rows, -> delete
	xdb_bmp_iterate (&pTblTrans->new_rows, xdb_trans_delrow_commit, pTblTrans);
	// iterate del rows -> just free bmp

	xdb_tbltrans_init (pTblTrans);

	return XDB_OK;		
}

XDB_STATIC int 
xdb_trans_db_rollback (uint32_t did, void *pArg)
{
	xdb_conn_t *pConn = pArg;
	xdb_dbTrans_t *pDbTrans = pConn->pDbTrans[did];

	xdb_lv2bmp_iterate (&pDbTrans->tbl_rows, xdb_trans_tbl_rollback, pDbTrans);

	return XDB_OK;
}

int 
xdb_rollback (xdb_conn_t *pConn)
{
	if (xdb_unlikely (pConn->conn_client)) {
		return XDB_OK;
	}

	if (xdb_unlikely (!pConn->bInTrans)) {
		return XDB_OK;
	}

	xdb_translog ("Rollback Transaction\n");

	xdb_lv2bmp_iterate (&pConn->dbTrans_bmp, xdb_trans_db_rollback, pConn);

	xdb_trans_unlock (pConn);

	return XDB_OK;
}

XDB_STATIC xdb_ret 
xdb_begin2 (xdb_conn_t *pConn, bool bAutoCommit)
{
	if (xdb_unlikely (pConn->conn_client)) {
		xdb_res_t *pRes = xdb_exec (pConn, "BEGIN");
		return -pRes->errcode;
	}
	
	xdb_translog ("begin transaction %s\n", bAutoCommit ? "AUTO" : "");

	if (xdb_unlikely (pConn->bInTrans)) {
		xdb_commit (pConn);
	}

	pConn->bInTrans = true;
	pConn->bAutoTrans = bAutoCommit;

	return XDB_OK;
}

xdb_ret 
xdb_begin (xdb_conn_t *pConn)
{
	return xdb_begin2 (pConn, false);
}

XDB_STATIC xdb_ret 
xdb_trans_free (xdb_conn_t *pConn)
{
	for (int i = 0; i < XDB_MAX_DB; ++i) {
		xdb_dbTrans_t *pDbTrans = pConn->pDbTrans[i];
		if (pDbTrans != NULL) {
			for (int t = 0 ; t < XDB_MAX_TBL; ++t) {
				xdb_tblTrans_t *pTblTrans = pDbTrans->pTblTrans[t];
				if (NULL != pTblTrans) {
					xdb_bmp_free (&pTblTrans->new_rows);
					xdb_bmp_free (&pTblTrans->del_rows);
					xdb_free (pTblTrans);
				}
			}
		}
		xdb_free (pDbTrans);		
	}
	
	return XDB_OK;
}

static volatile uint64_t	s_xdb_bg_run = 0;
static uint32_t	s_xdb_bg_flush = 0;
static uint32_t	s_xdb_flush_period = 1;

void* xdb_bg_task (void *data)
{
	sleep (10);
	while (s_xdb_bInit) {
		sleep (s_xdb_flush_period);
		if (!s_xdb_bInit) {
			break;
		}
		s_xdb_bg_run++;
		if (!s_xdb_bInit) {
			break;
		}
		for (int i = 0; i < XDB_OBJM_MAX(s_xdb_db_list); ++i) {
			xdb_dbm_t	*pDbm	= XDB_OBJM_GET(s_xdb_db_list, i);
			xdb_db_t	*pDb	= XDB_DBPTR(pDbm);
			if (s_xdb_bInit && (NULL != pDbm) && (pDb->lastchg_id != pDb->flush_id)) {
				xdb_translog ("XDB BGTask run %d flush %s\n", s_xdb_bg_run, XDB_OBJ_NAME(pDbm));
				xdb_flush_db (pDbm, 0);
				s_xdb_bg_flush++;
			}
			if (s_xdb_bInit && (NULL != pDbm) && pDbm->bTtlTbl) {				
				xdb_ttl_db (pDbm, false, 0);
			}
		}
	}

	s_xdb_bg_run = 0;
	return NULL;	
}

#if 0
void* xdb_task_test (void *data)
{
	int id =0;
	while (1) {
		usleep (5000);
		//printf ("%d\n", id++);
	}
}
#endif

static xdb_thread_t s_xdb_bg_tid;
XDB_STATIC xdb_ret xdb_bgtask_init ()
{
	xdb_create_thread (&s_xdb_bg_tid, NULL, xdb_bg_task, NULL);
	return XDB_OK;
}
