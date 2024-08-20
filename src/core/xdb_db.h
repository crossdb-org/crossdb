/******************************************************************************
* Copyright (c) 2024-present JC Wang. All rights reserved
*
*   https://crossdb.org
*   https://github.com/crossdb-org/crossdb
*
* This program is free software: you can redistribute it and/or modify it under
* the terms of the GNU General Public License, version 3 or later, as published 
* by the Free Software Foundation.
*
* This program is distributed in the hope that it will be useful, but WITHOUT
* ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS 
* FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License along with 
* this program. If not, see <http://www.gnu.org/licenses/>.
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
