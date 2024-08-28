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
	xdb_lockmode_t	lock_mode;
	bool			db_dirty;

#ifdef XDB_EANBLE_WAL
	xdb_walm_t 			wal_mgmt1;
	xdb_walm_t 			wal_mgmt2;
	xdb_walm_t 			*pWalm;
	xdb_walm_t 			*pWalmBak;
	xdb_walrow_t		*pWalRow;
#endif

	xdb_rwlock_t	wal_lock;
} xdb_dbm_t;

typedef struct xdb_dbobj_t {
	uint64_t	val[8];
} xdb_dbobj_t;

typedef struct xdb_db_t {
	xdb_stghdr_t	blk_hdr;
	uint64_t		lsn;
	uint8_t			lock_mode; // xdb_lockmode_t
	uint8_t			rsvd[7];
	xdb_dbobj_t 	dbobj[];
} xdb_db_t;

XDB_STATIC xdb_dbm_t* 
xdb_find_db (const char *name);

#endif // __XDB_DB_H__
