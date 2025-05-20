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

#ifndef __XDB_STMT_H__
#define __XDB_STMT_H__

#ifdef __cplusplus
extern "C" {
#endif

#include "xdb_token.h"
#include "../core/xdb_expr.h"

/*
DDL: definition
DML: manipulation
DQL: query
DCL: control
*/

typedef enum {
	XDB_STMT_INVALID		= 0,
	XDB_STMT_USE_DB			= 1,

	///////////////////////////

	// DML
	XDB_STMT_SELECT 		= 2,
	XDB_STMT_INSERT,
	XDB_STMT_REPLACE,
	XDB_STMT_UPDATE,
	XDB_STMT_DELETE,
	XDB_STMT_CALL,

	// Transaction and Lock
	XDB_STMT_BEGIN 			= 10,
	XDB_STMT_COMMIT,
	XDB_STMT_ROLLBACK,
	XDB_STMT_LOCK,
	XDB_STMT_UNLOCK,
		
	///////////////////////////

	// DB
	XDB_STMT_CREATE_DB 		= 30,
	XDB_STMT_ALTER_DB,
	XDB_STMT_DROP_DB,
	XDB_STMT_OPEN_DB,
	XDB_STMT_OPEN_DATADIR,
	XDB_STMT_CLOSE_DB,
	XDB_STMT_SHOW_DB,
	XDB_STMT_SHOW_CREATE_DB,

	// Table
	XDB_STMT_CREATE_TBL 	= 40,
	XDB_STMT_ALTER_TBL,
	XDB_STMT_DROP_TBL,
	XDB_STMT_DESC,
	XDB_STMT_SHOW_TBL,
	XDB_STMT_SHOW_COL,
	XDB_STMT_SHOW_CREATE_TBL,
	XDB_STMT_FLUSH_TBL,
	XDB_STMT_SHOW_TBL_STATS,
	XDB_STMT_RENAME_TBL,

	// Table Admin
	XDB_STMT_ANAYZE_TBL 	= 50,
	XDB_STMT_CHECK_TBL,
	XDB_STMT_REPAIR_TBL,
	XDB_STMT_CHECKSUM_TBL,
	XDB_STMT_TRUNATE_TBL,

	// Index
	XDB_STMT_CREATE_IDX 	= 60,
	XDB_STMT_ALTER_IDX,
	XDB_STMT_DROP_IDX,
	XDB_STMT_SHOW_IDX,
	XDB_STMT_RENAME_IDX,

	// View
	XDB_STMT_CREATE_VIEW 	= 65,
	XDB_STMT_ALTER_VIEW,
	XDB_STMT_DROP_VIEW,
	XDB_STMT_SHOW_VIEW,
	XDB_STMT_SHOW_CREATE_VIEW,

	// Server
	XDB_STMT_CREATE_SVR		= 70,
	XDB_STMT_ALTER_SVR,
	XDB_STMT_DROP_SVR,
	XDB_STMT_SHOW_SVR,

	// Trigger
	XDB_STMT_CREATE_TRIG 	= 75,
	XDB_STMT_ALTER_TRIG,
	XDB_STMT_DROP_TRIG,
	XDB_STMT_SHOW_TRIG,

	// Function
	XDB_STMT_CREATE_FUNC 	= 80,

	// Event
	XDB_STMT_CREATE_EVT  	= 85,

	// Account
	XDB_STMT_CREATE_USR		= 90,
	XDB_STMT_ALTER_USR,
	XDB_STMT_DROP_USR,
	XDB_STMT_RENAME_USR,
	XDB_STMT_SHOW_USR,
	XDB_STMT_SHOW_CREATE_USR,

	XDB_STMT_CREATE_ROLE	= 100,
	XDB_STMT_DROP_ROLE,
	XDB_STMT_GRANT,
	XDB_STMT_REVOKE,

	// Prepared STMT

	///////////////////////////

	// Publication
	XDB_STMT_CREATE_PUB		= 100,
	XDB_STMT_ALTER_PUB,
	XDB_STMT_DROP_PUB,
	XDB_STMT_SHOW_PUB,

	// Replication
	XDB_STMT_CREATE_REPLICA,
	XDB_STMT_ALTER_REPLICA,
	XDB_STMT_DROP_REPLICA,
	XDB_STMT_SHOW_REPLICA,
	XDB_STMT_SUBSCRIBE,


	///////////////////////////

	// Misc
	XDB_STMT_EXPLAIN		= 150,
	XDB_STMT_SET,
	XDB_STMT_AUTH,
	XDB_STMT_REPAIR_DB,
	XDB_STMT_FLUSH_DB,
	XDB_STMT_DUMP_WAL,
	XDB_STMT_AUDIT_MARK,
	XDB_STMT_AUDIT_CLEAR,
	XDB_STMT_AUDIT_SWEEP,
	XDB_STMT_AUDIT_CANCEL,

	// Backup Restore
	XDB_STMT_BACKUP			= 200,
	XDB_STMT_LOAD,
	XDB_STMT_SOURCE,
	XDB_STMT_DUMP_DB,
	XDB_STMT_SHELL,
	XDB_STMT_HELP,
} xdb_stmt_type;


#define XDB_STMT_COMMON	\
	xdb_conn_t			*pConn;		\
	char				*pSql;		\
	void				*pArg;		\
	xdb_stmt_type 		stmt_type

#define XDB_STMT_INIT(pStmt, type)	\
	memset (pStmt, 0, sizeof(*pStmt));	\
	pStmt->stmt_type = type;

typedef struct xdb_stmt_t {
	XDB_STMT_COMMON;
} xdb_stmt_t;

typedef enum {
	XDB_LOCK_THREAD,
	XDB_LOCK_PROCESS,
	XDB_LOCK_NOLOCK,
} xdb_lockmode_e;

typedef enum {
	XDB_SYNC_NOSYNC,
	XDB_SYNC_SYNC,
	XDB_SYNC_ASYNC=(1U<<31)-1,
} xdb_syncmode_e;

typedef struct {
	XDB_STMT_COMMON;
	bool			bMemory;
	xdb_lockmode_e	lock_mode;
	xdb_syncmode_e	sync_mode;
	bool			bIfExistOrNot;
	char 	 		*db_name;
	struct xdb_dbm_t	*pDbm;
} xdb_stmt_db_t;

typedef struct {
	XDB_STMT_COMMON;
	char 	 		*file;
	char 	 		*db_name;
	struct xdb_dbm_t	*pDbm;
	bool			bNoDrop;
	bool			bNoCreate;
	bool			bNoData;
} xdb_stmt_backup_t;

typedef struct {
	XDB_STMT_COMMON;
	bool			bIfExistOrNot;
	char 	 		*svr_name;
	int				svr_port;
} xdb_stmt_svr_t;

typedef struct {
	XDB_STMT_COMMON;
	bool			bIfExistOrNot;
	char 	 		*pub_name;
} xdb_stmt_pub_t;

typedef struct {
	XDB_STMT_COMMON;
	bool			bIfExistOrNot;
	char 	 		*rep_name;
	char			*dbs;
	char			*tables;
	char			*svr_host;
	int				svr_port;
} xdb_stmt_replica_t;

typedef struct {
	XDB_STMT_COMMON;
	bool			bIfExistOrNot;
	char 	 		*sub_name;
	char			*dbs;
	char			*tables;
	char			*client_id;
	bool			bReplica;
} xdb_stmt_subscribe_t;

typedef enum {
	XDB_IDX_HASH 		= 0,
	XDB_IDX_RBTREE		= 1,
	XDB_IDX_MAX 		= 3,
} xdb_idx_type;

typedef enum {
	XDB_IDX_PRIMARY		= (1<<0),  // primary key index
	XDB_IDX_UNIQUE		= (1<<1),
	XDB_IDX_RANGE		= (1<<2),  // support range
} xdb_idx_flag;

typedef struct {
	XDB_STMT_COMMON;
	
	char				*idx_name;
	char				idxName[XDB_NAME_LEN+1];
	bool				bUnique;
	bool				bPrimary;
	xdb_idx_type		idx_type;
	int					xoid;
	struct xdb_tblm_t	*pTblm;
	struct xdb_idxm_t	*pIdxm;
	int					fld_count;
	//xdb_field_t			*pFields[XDB_MAX_MATCH_COL];
	const char 			*idx_col[XDB_MAX_MATCH_COL];
} xdb_stmt_idx_t;

typedef enum {
	XDB_FKEY_RESTRICT,  // U: don't allow update  D: don't allow delete
	XDB_FKEY_NOACT, 	// same with RESTRICT
	XDB_FKEY_CASCADE,   // U: update parent  D: delete parent
	XDB_FKEY_SETNULL,	// U/D: set parent to 0
	XDB_FKEY_SETDFT,	// U/D: set parent to default
	XDB_FKEY_MAX,
} xdb_fkey_action;

typedef struct {
	XDB_STMT_COMMON;

	struct xdb_dbm_t	*pDbm;
	char				*fkey_name;
	char				fkeyName[XDB_NAME_LEN+1];
	struct xdb_tblm_t	*pTblm;
	int					fkey_id;
	int					fld_count;
	const char 			*fkey_col[XDB_MAX_MATCH_COL];
	struct xdb_tblm_t	*pRefTblm;
	xdb_field_t			*pRefFlds[XDB_MAX_MATCH_COL];
	xdb_fkey_action		on_upd_act;
	xdb_fkey_action		on_del_act;
} xdb_stmt_fkey_t;

typedef struct {
	XDB_STMT_COMMON;
	
	char				*trig_name;
	xdb_trig_e			trig_type;
	struct xdb_tblm_t	*pTblm;
	char				*func_name;
} xdb_stmt_trig_t;

#if 0
typedef struct xdb_str {
	char	*ptr;
	int		len;
	int		cap;
} xdb_str;
#endif

typedef struct xdb_filter_t {
	uint8_t			cmp_op;		// xdb_token_type
	//uint8_t			fld_type;	
//	uint16_t		fld_off;
//	uint16_t		fld_id;
	xdb_field_t		*pField;
	char			*pExtract;
	xdb_value_t		val;
} xdb_filter_t;

typedef struct {
	xdb_token_type	exp_op;
	xdb_value_t		op_val[2];
} xdb_exp_t;

typedef struct xdb_setfld_t {
	xdb_field_t		*pField;
	xdb_exp_t		exp;
} xdb_setfld_t;

typedef struct {
	xdb_str_t		as_name;
	xdb_exp_t		exp;
} xdb_selcol_t;

typedef enum xdb_join_e {
	XDB_JOIN_NONE,
	XDB_JOIN_INNER,
	XDB_JOIN_LEFT,
} xdb_join_e;

typedef struct {
	struct xdb_idxm_t	*pIdxm;
	int					match_cnt;
	xdb_token_type		match_opt, match_opt2;
	xdb_value_t 		*pIdxVals[XDB_MAX_MATCH_COL];
	xdb_filter_t		*pIdxFlts[XDB_MAX_MATCH_COL];
	int 				idx_flt_cnt;
} xdb_idxfilter_t;

typedef struct {
	xdb_filter_t		*pFilters[XDB_MAX_MATCH_COL/4];
	xdb_idxfilter_t 	idx_filter;
	xdb_idxfilter_t 	*pIdxFilter;
	uint8_t				filter_count;
} xdb_singfilter_t;

#define XDB_MAX_MATCH_OR	64

typedef struct {
	xdb_field_t			*pField[XDB_MAX_MATCH_COL];
	xdb_field_t			*pJoinField[XDB_MAX_MATCH_COL];

	struct xdb_tblm_t	*pRefTblm;
	const char			*as_name;

	xdb_join_e			join_type;
	uint8_t				field_count;
	uint8_t				eq_bmp[(XDB_MAX_COLUMN+7)/8];

	uint8_t				filter_count;
	uint8_t				or_count;
	xdb_filter_t		filters[XDB_MAX_MATCH_COL*16];
	xdb_singfilter_t	or_list[XDB_MAX_MATCH_OR];
	bool				bUseIdx;
} xdb_reftbl_t;

typedef struct {
	XDB_STMT_COMMON;
	char 	 		*tbl_name;

	// if add more init, need to change xdb_init_where_stmt
	uint16_t		col_count;
	uint16_t		exp_count;
	uint16_t		agg_count;
	uint16_t		order_count;
	uint16_t		bind_count;
	uint16_t		set_bind_count;
	uint16_t		set_count;
	uint8_t			reftbl_count;
	uint8_t			rsvd;

	// bind value
	xdb_value_t	*pBind[XDB_MAX_MATCH_COL];

	xdb_field_t		*pSelFlds[XDB_MAX_COLUMN];

	struct xdb_tblm_t	*pTblm;
	xdb_reftbl_t		ref_tbl[XDB_MAX_JOIN];

	//uint16_t		null_off;
	uint32_t		meta_size;
	xdb_meta_t		*pMeta;
	//char			**pFldName;
	//uint8_t			*pFldType;
	//uint16_t		*pFldOff;

	// agg func result
	uint64_t		agg_buf[XDB_MAX_COLUMN];

	xdb_selcol_t	sel_cols[XDB_MAX_COLUMN];

	// orderby
	xdb_field_t		*pOrderFlds[XDB_MAX_MATCH_COL];
	bool			bOrderDesc[XDB_MAX_MATCH_COL];

	// limit
	xdb_rowid		limit;
	xdb_rowid		offset;

	xdb_res_t		*pRes;
	xdb_size		res_size;

	xdb_setfld_t	set_flds[XDB_MAX_COLUMN];

	xdb_row_callback 	callback;
	void 				*pCbArg;

	bool			bRegexp;
} xdb_stmt_select_t;

typedef struct {
	XDB_STMT_COMMON;
	const		char *datadir;
	const		char *format;
	const		char *svrid;
} xdb_stmt_set_t;

typedef enum {
	XDB_TIME_US,
	XDB_TIME_MS,
	XDB_TIME_SEC,
	XDB_TIME_MIN,
	XDB_TIME_HOUR,
	XDB_TIME_DAY,
	XDB_TIME_WEEK,
	XDB_TIME_MONTH,
	XDB_TIME_QUART,
	XDB_TIME_YEAR,
} xdb_time_unit;

static uint64_t xdb_timeunit_us (xdb_time_unit type)
{
	static uint64_t time_unit [] = {
		[XDB_TIME_US    ] = 1LL,
		[XDB_TIME_MS	] = 1000LL,
		[XDB_TIME_SEC	] = 1000000LL,
		[XDB_TIME_MIN	] = 60*1000000LL,
		[XDB_TIME_HOUR	] = 3600*1000000LL,
		[XDB_TIME_DAY	] = 24*3600*1000000LL,
		[XDB_TIME_WEEK  ] = 7*24*3600*1000000LL,
		[XDB_TIME_MONTH ] = 30*24*3600*1000000LL,
		[XDB_TIME_QUART ] = 120*24*3600*1000000LL,
		[XDB_TIME_YEAR  ] = 365*24*3600*1000000LL,
	};
	return time_unit[type];
}

typedef struct {
	XDB_STMT_COMMON;
	struct xdb_dbm_t		*pDbm;
	char 	 		*db_name;
	char 	 		*tbl_name;
	void			*sql;
	int				fld_count;
	int				vfld_count;
	int				row_size;
	int				xoid;
	bool			bMemory;
	char			*ttl_col;
	int				ttl_expire;
	xdb_time_unit	ttl_unit;
	struct xdb_tblm_t	*pTblm;
	xdb_field_t		stmt_flds[XDB_MAX_COLUMN];
	bool			bIfExistOrNot;
	xdb_stmt_idx_t	stmt_idx[XDB_MAX_INDEX];
	xdb_stmt_fkey_t	stmt_fkey[XDB_MAX_FKEY];

	char			pkey_idx;
	uint8_t			idx_count;
	uint8_t			fkey_count;
} xdb_stmt_tbl_t;

typedef struct {
	XDB_STMT_COMMON;

	char 	 			*tbl_name;
	struct xdb_tblm_t	*pTblm;
	xdb_field_t		*pFldList[XDB_MAX_COLUMN];
	uint32_t		row_offset[XDB_MAX_COLUMN];
	uint32_t		buf_len;
	void			*pRowsBuf;
	uint32_t		row_buf[1024]; // 4K

	uint16_t		fld_count;
	uint16_t		row_count;
	uint16_t		bind_count;

	xdb_field_t		*pBind[XDB_MAX_COLUMN];
	void			*pBindRow[XDB_MAX_COLUMN];
} xdb_stmt_insert_t;

typedef struct {
	XDB_STMT_COMMON;
	struct {
		struct xdb_tblm_t	*pTblm;
		bool				bWrite;
	} lock_tbl[16];
	int						tbl_num;
} xdb_stmt_lock_t;

typedef union {
	xdb_stmt_t			stmt;
	xdb_stmt_db_t		db_stmt;
	xdb_stmt_tbl_t		tbl_stmt;
	xdb_stmt_idx_t		idx_stmt;
	xdb_stmt_trig_t		trig_stmt;
	xdb_stmt_insert_t	insert_stmt;
	xdb_stmt_select_t	select_stmt;
	xdb_stmt_set_t		set_stmt;
	xdb_stmt_svr_t		svr_stmt;
	xdb_stmt_pub_t		pub_stmt;
	xdb_stmt_subscribe_t		subscribe_stmt;
	xdb_stmt_replica_t	replica_stmt;
	xdb_stmt_lock_t		lock_stmt;
	xdb_stmt_backup_t	backup_stmt;
} xdb_stmt_union_t;

XDB_STATIC void 
xdb_stmt_free (xdb_stmt_t *pStmt);

#ifdef __cplusplus
}
#endif

#define XDB_SQL_CREATE_DB_STMT		"CREATE DATABASE [IF NOT EXISTS] {db_name | 'db_path/db_name'} [ENGINE = MEMORY] [LOCKMODE = {PROCESS|THREAD|NOLOCK}] []"
#define XDB_SQL_CREATE_TBL_STMT		"CREATE TABLE [IF NOT EXISTS] [db_name.]tbl_name (col_name TYPE [PRIMARY]) [ENGINE = MEMORY]"
#define XDB_SQL_DROP_DB_STMT		"DROP DATABASE [IF EXISTS] db_name"
#define XDB_SQL_DROP_TBL_STMT		"DROP TABLE [IF EXISTS] tbl_name"
#define XDB_SQL_CREATE_IDX_STMT		"CREATE [UNIQUE] INDEX idx_name ON tbl_name (col_name,...)"
#define XDB_SQL_DROP_IDX_STMT		"DROP INDEX idx_name ON tbl_name"
#define XDB_SQL_LOCK_TBL_STMT		"LOCK TABLES tbl_name {WRITE|READ}, ..."

#define XDB_SQL_NO_DB_ERR			"No database selected"

#endif // __XDB_STMT_H__
