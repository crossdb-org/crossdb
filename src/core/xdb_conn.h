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

typedef struct {
	uint64_t			buf_len;
	uint64_t			buf_free; // if not -1 means but not freed yet
	xdb_rowdat_t		*pCurRow;
	xdb_stmt_select_t	*pStmt;
	
	xdb_rowlist_t		rowlist;	// fetch row col, at most 8192/cols
	xdb_row_t			rowbuf[XDB_ROW_COL_CNT]; // store row pointer list

	xdb_conn_t			*pConn; // pConn must be before res
	xdb_res_t			res; // must be last
} xdb_queryRes_t;

typedef struct xdb_conn_t {
	uint64_t			session_id;
	pid_t				pid;
	char				connect_time[16];
	uint64_t			stmt_times;

	struct xdb_dbm_t*	pCurDbm;

	xdb_conn_t			*pConn; // pConn must be before conn_res
	xdb_res_t			conn_res;
	xdb_msg_t			conn_msg;

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

#ifdef XDB_ENABLE_SERVER
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
} xdb_conn_t;

XDB_STATIC void 
xdb_conn_init (xdb_conn_t* pConn);

static inline void 
xdb_init_rowlist (xdb_queryRes_t *pQueryRes)
{
	pQueryRes->pCurRow = (void*)pQueryRes + sizeof (*pQueryRes) + pQueryRes->res.meta_len;
	pQueryRes->rowlist.rl_count	= XDB_ROW_COL_CNT / pQueryRes->res.col_count;
	if (xdb_unlikely (pQueryRes->rowlist.rl_count > pQueryRes->res.row_count)) {
		pQueryRes->rowlist.rl_count = pQueryRes->res.row_count;
	}
	pQueryRes->rowlist.rl_curid= 0;
}

#endif // __XDB_CONN_H__