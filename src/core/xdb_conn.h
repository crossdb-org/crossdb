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

#ifndef __XDB_CONN_H__
#define __XDB_CONN_H__

typedef enum {
	XDB_FMT_TABLE,		// shell
	XDB_FMT_VERTICAL,	// shell
	XDB_FMT_JSON,		// shell
	XDB_FMT_JSONLINE,	// shell
	XDB_FMT_JSONMIME,	// shell
	XDB_FMT_NDJSON,		// shell
	
	XDB_FMT_NATIVELE	= 32,
	XDB_FMT_NATIVEBE,
} xdb_format_t;

typedef struct xdb_queryRes_t {
	uint64_t			buf_len;
	uint64_t			buf_free; // if not -1 means but not freed yet
	xdb_stmt_select_t	*pStmt;
	
	xdb_objm_t			*pMetaHash;
	xdb_conn_t			*pConn; // pConn must be before res
	xdb_res_t			res; // must be last
} xdb_queryRes_t;

typedef struct xdb_conn_t {
	uint64_t			session_id;
	int32_t				ref_cnt;
	pid_t				pid;
	char				connect_time[16];
	uint64_t			stmt_times;

	struct xdb_dbm_t*	pCurDbm;

	xdb_conn_t			*pConn; // pConn must be before conn_res
	xdb_res_t			conn_res; // store query result without meta and rows
	xdb_msg_t			conn_msg; // conn_msg must be after conn_res

	int 				sockfd;
	FILE				*conn_stdout;

	xdb_queryRes_t		*pQueryRes;	// store query result with meta and rows

	char				cur_db[XDB_NAME_LEN+1]; // current default DB
	char				host[128]; // server side to save peer addr
	uint16_t			port;
	char				user[XDB_NAME_LEN+1];
	char				passwd[XDB_NAME_LEN+1];

	xdb_status_t		status;
	xdb_stmt_union_t	stmt_union;

	char				*pSql;
	int					sqlbuf_len;
	char				*pNxtSql;
	char				sql_buf[4096];

#if (XDB_ENABLE_SERVER == 1)
	xdb_server_t		*pServer;
#endif
	xdb_rowset_t		row_set;

	bool				bAutoTrans;
	bool				bInTrans;
	bool				bAutoCommit;
	xdb_lv2bmp_t		dbTrans_bmp;
	
	xdb_dbTrans_t		*pDbTrans[XDB_MAX_DB];

	bool				conn_client;
	xdb_format_t		res_format;

	char				*poll_buf;
	uint32_t			poll_size;

	struct xdb_subscribe_t 	*pSubscribe;
} xdb_conn_t;

XDB_STATIC void 
xdb_conn_init (xdb_conn_t* pConn);

static inline void 
xdb_init_rowlist (xdb_queryRes_t *pQueryRes)
{
	pQueryRes->res.row_data = (uintptr_t)((void*)pQueryRes + sizeof (*pQueryRes) + pQueryRes->res.meta_len);
}

#endif // __XDB_CONN_H__
