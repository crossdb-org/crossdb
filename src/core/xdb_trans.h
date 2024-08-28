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

#ifndef __XDB_TRANS_H__
#define __XDB_TRANS_H__

typedef struct {
	struct xdb_dbTrans_t *pDbTrans;
	xdb_tblm_t *pTblm;
	xdb_bmp_t	new_rows;
	xdb_bmp_t	del_rows;
} xdb_tblTrans_t;

typedef struct xdb_dbTrans_t {
	xdb_dbm_t		*pDbm;
	xdb_lv2bmp_t	tbl_wrlocks;
	xdb_lv2bmp_t	tbl_rdlocks;
	xdb_lv2bmp_t	tbl_rows;
	xdb_tblTrans_t	*pTblTrans[];
} xdb_dbTrans_t;

XDB_STATIC int 
xdb_trans_addrow (xdb_conn_t *pConn, xdb_tblm_t *pTblm, xdb_rowid rid, bool bNew);

XDB_STATIC int 
xdb_trans_delrow (xdb_conn_t *pConn, xdb_tblm_t *pTblm, xdb_rowid rid);

XDB_STATIC bool 
xdb_trans_getrow (xdb_conn_t *pConn, xdb_tblm_t *pTblm, xdb_rowid rid, bool bNew);

//XDB_STATIC int 
//xdb_rdlock_table (xdb_conn_t *pConn, xdb_tblm_t *pTblm);

XDB_STATIC int 
xdb_rdunlock_table (xdb_tblm_t *pTblm);

XDB_STATIC int 
xdb_wrlock_table (xdb_conn_t *pConn, xdb_tblm_t *pTblm);

XDB_STATIC int 
xdb_wrunlock_table (xdb_tblm_t *pTblm);

static inline int 
xdb_rdlock_tblstg (xdb_tblm_t *pTblm) 
{
	if (xdb_likely (XDB_LOCK_THREAD == pTblm->lock_mode)) {
		xdb_rwlock_rdlock (&pTblm->stg_lock);
	} else if (XDB_LOCK_PROCESS == pTblm->lock_mode) {
		xdb_file_rdlock (pTblm->stg_mgr.stg_fd, 1, 1);
	}
	return XDB_OK;
}

static inline int 
xdb_rdunlock_tblstg (xdb_tblm_t *pTblm) 
{
	if (xdb_likely (XDB_LOCK_THREAD == pTblm->lock_mode)) {
		xdb_rwlock_rdunlock (&pTblm->stg_lock);
	} else if (XDB_LOCK_PROCESS == pTblm->lock_mode) {
		return xdb_file_unlock (pTblm->stg_mgr.stg_fd, 1, 1);
	}
	return XDB_OK;
}

static inline int 
xdb_wrlock_tblstg (xdb_tblm_t *pTblm) 
{
	if (xdb_likely (XDB_LOCK_THREAD == pTblm->lock_mode)) {
		xdb_rwlock_wrlock (&pTblm->stg_lock);
	} else if (XDB_LOCK_PROCESS == pTblm->lock_mode) {
		return xdb_file_wrlock (pTblm->stg_mgr.stg_fd, 1, 1);
	}
	return XDB_OK;
}

static inline int 
xdb_wrunlock_tblstg (xdb_tblm_t *pTblm) 
{
	if (xdb_likely (XDB_LOCK_THREAD == pTblm->lock_mode)) {
		xdb_rwlock_wrunlock (&pTblm->stg_lock);
	} else if (XDB_LOCK_PROCESS == pTblm->lock_mode) {
		return xdb_file_unlock (pTblm->stg_mgr.stg_fd, 1, 1);
	}
	return XDB_OK;
}

XDB_STATIC void 
xdb_tbltrans_init (xdb_tblTrans_t *pTblRows);

XDB_STATIC int 
xdb_begin2 (xdb_conn_t *pConn, bool bAutoCommit);

static inline bool 
xdb_row_valid (xdb_conn_t *pConn, xdb_tblm_t *pTblm, void *pRow, xdb_rowid rid)
{
	uint8_t ctrl = XDB_ROW_CTRL (pTblm->stg_mgr.pStgHdr, pRow) & XDB_ROW_MASK;
	if (ctrl < XDB_ROW_COMMIT) {
		return false;
	}
	if (XDB_ROW_COMMIT == ctrl) {
		// not delete by this conn?
		return ! xdb_trans_getrow (pConn, pTblm, rid, false);
	} else {
		// insert by this conn?
		return   xdb_trans_getrow (pConn, pTblm, rid, true);
	}
}

#endif // __XDB_TRANS_H__
