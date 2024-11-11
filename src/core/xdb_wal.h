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

#ifndef __XDB_WAL_H__
#define __XDB_WAL_H__

#if 0
#define XDB_WAL_LENBITS	28
#define XDB_WAL_LENMASK	((1<<XDB_VDAT_LENBITS) - 1)

typedef enum {
	XDB_WAL_INVALID	= 0,
	XDB_WAL_INSERT	= 1,
	XDB_WAL_DELETE	= 2,
	XDB_WAL_UPDATE	= 3,
	XDB_WAL_SQL		= 4,
} xdb_waltype_t;
#endif

typedef struct {
	uint32_t				row_len;	// ms4b is type
	xdb_rowid				row_id;		// for upd, new row len
	uint32_t				tbl_xoid;	// or interval from beg_ts in us
	uint8_t					row_data[];	// for update, old row, new row, (upd cnt, fid list)4B align
} xdb_walrow_t;

typedef struct {
	uint64_t				commit_len; // =0 means end
	uint64_t				commit_id;
	uint64_t				timestamp;	// in us
	uint32_t				checksum;
	uint32_t				rsvd[1];
} xdb_commit_t;

typedef struct {
	xdb_stghdr_t			blk_hdr;
	uint64_t				wal_size;		// current WAL size(alloc), include header
	uint64_t				sync_size;		// sync to disk size, include header	
	uint64_t				sync_cid;		// last sync commit id
	uint64_t				commit_id;		// next commit id
	uint64_t				commit_size;	// commit size, include header
	uint64_t				rollback_count;	// total rollback count 
	uint8_t					wal_active;		// is active
	uint8_t					wal_switch;		// need switch
	uint8_t					rsvd[6];
	uint64_t				rsvd2[4];
	xdb_commit_t			wal_commit[1];	// commit segment
	xdb_walrow_t			wal_row[0];
} xdb_wal_t;

typedef struct {
	struct xdb_dbm_t		*pDbm;
	xdb_stgmgr_t			stg_mgr;
	xdb_rowid				wal_cap;
	uint64_t				wal_size; // Windows WAL chg may fail
} xdb_walm_t;

#define XDB_WAL_PTR(pWalm) ((xdb_wal_t*)(pWalm->stg_mgr.pStgHdr))


#define XDB_WAL_MAX_SIZE		(256*1024*1024)

XDB_STATIC int 
xdb_trans_db_wal (uint32_t did, void *pArg);

XDB_STATIC bool 
xdb_wal_switch (struct xdb_dbm_t *pDbm);

XDB_STATIC xdb_ret 
__xdb_wal_flush (xdb_walm_t *pWalMgmt, bool bFlush);

XDB_STATIC void 
xdb_wal_close (struct xdb_dbm_t *pDbm);

XDB_STATIC void 
xdb_wal_drop (struct xdb_dbm_t *pDbm);

XDB_STATIC int 
xdb_wal_create (xdb_conn_t *pConn, struct xdb_dbm_t *pDbm);

XDB_STATIC void 
xdb_wal_dump (struct xdb_dbm_t *pDbm);

XDB_STATIC xdb_ret 
xdb_wal_redo (struct xdb_dbm_t *pDbm);

#endif // __XDB_WAL_H__
