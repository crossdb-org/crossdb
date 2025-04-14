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

#ifndef __XDB_DB_H__
#define __XDB_DB_H__

typedef struct xdb_dbm_t {
	xdb_obj_t		obj;
	char			db_path[XDB_PATH_LEN+1];
	xdb_objm_t		db_objm;
	xdb_stgmgr_t	stg_mgr;
	bool			bMemory;
	bool			bSysDb;
	xdb_lockmode_e	lock_mode;
	xdb_syncmode_e	sync_mode;

	xdb_walm_t 			wal_mgr[2];
	xdb_walm_t 			*pWalm;
	xdb_walm_t 			*pWalmBak;

	xdb_rwlock_t		wal_lock;
	xdb_rwlock_t		db_lock;
} xdb_dbm_t;

typedef struct xdb_dbobj_t {
	uint64_t	val[8];
} xdb_dbobj_t;

typedef struct xdb_db_t {
	xdb_stghdr_t	blk_hdr;
	uint64_t		flush_time;
	uint64_t		lastchg_id;	// last commit id
	uint64_t		flush_id;	// last flush id
	uint8_t			lock_mode;  // xdb_lockmode_e
	uint32_t		sync_mode;	// xdb_syncmode_e
	uint8_t			rsvd[7];
	xdb_dbobj_t 	dbobj[];
} xdb_db_t;

#define XDB_DBPTR(pDbm)	((xdb_db_t*)pDbm->stg_mgr.pStgHdr)

XDB_STATIC xdb_dbm_t* 
xdb_find_db (const char *name);

XDB_STATIC xdb_ret
xdb_repair_db (xdb_dbm_t *pDbm, int flags);

XDB_STATIC int 
xdb_flush_db (xdb_dbm_t *pDbm, uint32_t flags);

XDB_STATIC int 
xdb_dump_create_db (xdb_dbm_t *pDbm, char buf[], xdb_size size, uint32_t flags);

static inline int 
xdb_wal_rdlock (struct xdb_dbm_t *pDbm) 
{
	if (xdb_likely (XDB_LOCK_THREAD == pDbm->lock_mode)) {
		xdb_rwlock_rdlock (&pDbm->wal_lock);
	} else if (XDB_LOCK_PROCESS == pDbm->lock_mode) {
		xdb_file_rdlock (pDbm->stg_mgr.stg_fd, 1, 1);
	}
	return XDB_OK;
}

static inline int 
xdb_wal_rdunlock (struct xdb_dbm_t *pDbm) 
{
	if (xdb_likely (XDB_LOCK_THREAD == pDbm->lock_mode)) {
		xdb_rwlock_rdunlock (&pDbm->wal_lock);
	} else if (XDB_LOCK_PROCESS == pDbm->lock_mode) {
		return xdb_file_unlock (pDbm->stg_mgr.stg_fd, 1, 1);
	}
	return XDB_OK;
}

static inline int 
xdb_wal_wrlock (struct xdb_dbm_t *pDbm) 
{
	if (xdb_likely (XDB_LOCK_THREAD == pDbm->lock_mode)) {
		xdb_rwlock_wrlock (&pDbm->wal_lock);
	} else if (XDB_LOCK_PROCESS == pDbm->lock_mode) {
		return xdb_file_wrlock (pDbm->stg_mgr.stg_fd, 1, 1);
	}
	return XDB_OK;
}

static inline int 
xdb_wal_wrunlock (struct xdb_dbm_t *pDbm) 
{
	if (xdb_likely (XDB_LOCK_THREAD == pDbm->lock_mode)) {
		xdb_rwlock_wrunlock (&pDbm->wal_lock);
	} else if (XDB_LOCK_PROCESS == pDbm->lock_mode) {
		return xdb_file_unlock (pDbm->stg_mgr.stg_fd, 1, 1);
	}
	return XDB_OK;
}

#endif // __XDB_DB_H__
