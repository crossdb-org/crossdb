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

#if XDB_LOG_FLAGS & XDB_LOG_PUBSUB
#define xdb_pubsublog(...)	xdb_print(__VA_ARGS__)
#else
#define xdb_pubsublog(...)
#endif

static xdb_objm_t	s_xdb_pub_list;
static xdb_objm_t	s_xdb_replicas_client;
static xdb_objm_t	s_xdb_subscriber_list;
static xdb_vec_t	s_xdb_replicas_global;

XDB_STATIC xdb_pub_t* 
xdb_find_pub (const char *pub_name)
{
	return xdb_objm_get (&s_xdb_pub_list, pub_name);
}

XDB_STATIC xdb_replica_t* 
xdb_find_replica (const char *rep_name)
{
	return xdb_objm_get (&s_xdb_replicas_client, rep_name);
}

XDB_STATIC xdb_subscribe_t* 
xdb_find_subscriber (const char *sub_name)
{
	return xdb_objm_get (&s_xdb_subscriber_list, sub_name);
}

XDB_STATIC int 
xdb_create_pub (xdb_stmt_pub_t *pStmt)
{
	xdb_conn_t* pConn = pStmt->pConn;
	xdb_pub_t *pPub = xdb_find_pub (pStmt->pub_name);
	if (pStmt->bIfExistOrNot && (NULL != pPub)) {
		return XDB_OK;
	}

	pPub = xdb_calloc (sizeof(xdb_pub_t));
	XDB_EXPECT (NULL != pPub, XDB_E_MEMORY, "Can't alloc memory");
	xdb_strcpy (XDB_OBJ_NAME(pPub), pStmt->pub_name);

	XDB_OBJ_ID(pPub) = -1;
	xdb_objm_add (&s_xdb_pub_list, pPub);

	xdb_sysdb_add_pub (pPub);

	return XDB_OK;

error:
	xdb_free (pPub);
	return -pConn->conn_res.errcode;
}

XDB_STATIC void* 
xdb_run_replica (void *pArg)
{
	xdb_res_t *pRes;
	xdb_replica_t *pReplica = pArg;

	xdb_conn_t* pPubConn;
	xdb_conn_t* pConn = xdb_connect (NULL, NULL, NULL, NULL, 0);
	if (NULL == pConn) {
		xdb_errlog ("Can't create local replica connection\n");
		return NULL;
	}

	while (1) {
		while (1) {
			pPubConn = xdb_connect (pReplica->svr_host, NULL, NULL, NULL, pReplica->svr_port);
			if (NULL != pPubConn) {
				break;
			}
			sleep (2);
		};

		//pRes = xdb_pexec (pPubConn, "CREATE PUBLICATION %s", pSub->pub_name);
		//XDB_RESCHK(pRes);

		if (NULL != pReplica->dbs) {
			pRes = xdb_pexec (pPubConn, "SUBSCRIBE %s REPLICA=1, DB='%s'", XDB_OBJ_NAME(pReplica), pReplica->dbs);
		} else if (NULL != pReplica->tables) {
			pRes = xdb_pexec (pPubConn, "SUBSCRIBE %s REPLICA=1, TABLE='%s'", XDB_OBJ_NAME(pReplica), pReplica->tables);
		} else {
			pRes = xdb_pexec (pPubConn, "SUBSCRIBE %s REPLICA=1", XDB_OBJ_NAME(pReplica));
		}
		XDB_RESCHK(pRes);
		while (1) {
			int len;
			const char * sql = xdb_poll (pPubConn, &len, 0);
			if (sql != NULL) {
				xdb_pubsublog ("=== Recv %d: %s\n", len, sql);
				pRes = xdb_exec (pConn, sql);
			} else {
				xdb_errlog ("REPLICA '%s' socket error, reconnect...\n", XDB_OBJ_NAME(pReplica));
				break;
			}
		}
	}

	return NULL;
}

XDB_STATIC int 
xdb_create_replica (xdb_stmt_replica_t *pStmt)
{
	xdb_conn_t* pConn = pStmt->pConn;
	xdb_replica_t *pReplica = xdb_find_replica (pStmt->rep_name);
	if (pStmt->bIfExistOrNot && (NULL != pReplica)) {
		return XDB_OK;
	}

	pReplica = xdb_calloc (sizeof(*pReplica));
	XDB_EXPECT (NULL != pReplica, XDB_E_MEMORY, "Can't alloc memory");
	xdb_strcpy (XDB_OBJ_NAME(pReplica), pStmt->rep_name);

	xdb_strcpy (pReplica->svr_host, pStmt->svr_host);
	pReplica->svr_port = pStmt->svr_port;
	if (NULL != pStmt->dbs) {
		pReplica->dbs = xdb_strdup (pStmt->dbs, 0);
		XDB_EXPECT (NULL != pReplica, XDB_E_MEMORY, "Can't alloc memory");
	}
	if (NULL != pStmt->tables) {
		pReplica->tables = xdb_strdup (pStmt->tables, 0);
		XDB_EXPECT (NULL != pReplica, XDB_E_MEMORY, "Can't alloc memory");
	}

	XDB_OBJ_ID(pReplica) = -1;
	xdb_objm_add (&s_xdb_replicas_client, pReplica);

	xdb_create_thread (&pReplica->tid, NULL, xdb_run_replica, pReplica);

	//xdb_sysdb_add_svr (pServer);

	return XDB_OK;

error:
	xdb_free (pReplica->dbs);
	xdb_free (pReplica->tables);
	xdb_free (pReplica);
	return -pConn->conn_res.errcode;
}

XDB_STATIC int 
xdb_subscribe_send (xdb_subscribe_t *pSub, const char *sql, int len)
{
	xdb_conn_t	*pConn = pSub->pConn;

	if (xdb_unlikely (NULL == pConn)) {
		return -1;
	}

	char buf[64];
	int slen = len + 1;
	int nn = sprintf (buf, "$%d\n", slen);
	int wlen = xdb_sock_write(pConn->sockfd, buf, nn);
	XDB_EXPECT_SOCK(wlen == nn, XDB_E_SOCK, "Socket Error write %d of %d", wlen, nn);
	wlen = xdb_sock_write(pConn->sockfd, sql, slen);
	XDB_EXPECT_SOCK(wlen == slen, XDB_E_SOCK, "Socket Error write %d of %d", wlen, slen);
	if (NULL != pSub->pPub) {
		xdb_pubsublog ("pub '%s' send to '%s' %d: %s%s\n", XDB_OBJ_NAME(pSub->pPub), XDB_OBJ_NAME(pSub), len, buf, sql);
	} else {
		xdb_pubsublog ("sub '%s' send to '%s' %d: %s%s\n", XDB_OBJ_NAME(pSub), pSub->client, len, buf, sql);
	}

	return 0;

error:
	pSub->pConn = NULL;
	return -1;
}

XDB_STATIC int 
xdb_subscribe_send2 (xdb_subscribe_t *pSub, const char *sql, int len)
{
	XDB_OBJ_WRLOCK (pSub);
	if (!pSub->bSync) {
		xdb_vec_addStr (&pSub->cud_cache, sql, len);
	} else {
		xdb_subscribe_send (pSub, sql, len);
	}
	XDB_OBJ_WRUNLOCK (pSub);
	return 0;
}

typedef struct {
	char 		*buf;
	int64_t 	cap;
	char 		dat[4096];
} xdb_buf_t;

void xdb_buf_init (xdb_buf_t *pBuf)
{
	pBuf->buf = pBuf->dat;
	pBuf->cap = sizeof (pBuf->dat);
}

void xdb_buf_free (xdb_buf_t *pBuf)
{
	if (pBuf->buf != pBuf->dat) {
		xdb_free (pBuf->buf);
	}
}

int64_t xdb_buf_resize (xdb_buf_t *pBuf, int64_t cap)
{
	if (cap > pBuf->cap) {
		void *pbuf;
		cap = XDB_ALIGN4K (cap + 1);
		if (pBuf->buf != pBuf->dat) {
			pbuf = xdb_realloc (pBuf->buf, cap);
			if (NULL == pbuf) {
				return -1;
			}
		} else {
			pbuf = xdb_malloc (cap);
			if (NULL == pbuf) {
				return -1;
			}
			memcpy (pbuf, pBuf->buf, sizeof(pBuf->dat));
		}
		pBuf->buf = pbuf;
		pBuf->cap = cap ;
	}
	return 0;
}

int xdb_dsprintf (xdb_buf_t *pBuf, int offset, const char *format, ...)
{
    va_list 	ap;
	va_start(ap, format);
	int64_t left = pBuf->cap - offset;
	int64_t len = vsnprintf (pBuf->buf + offset, left, format, ap);
	if (len >= left) {
		int rc = xdb_buf_resize (pBuf, offset + len + 1);
		if (rc < 0) {
			return rc;
		}
		len = vsnprintf (pBuf->buf + offset, len + 1, format, ap);
	}
	return offset + len;
}

#if 0
static int 
xdb_dsprint_field (xdb_type_t type, void *pVal, xdb_buf_t *pBuf, int offset)
{
	int len;
	char	*ptr;

	xdb_buf_resize (pBuf, offset + 64);

	if (NULL == pVal) {
		memcpy (pBuf->buf + offset, "NULL", 5);
		return offset + 4;
	}
	switch (type) {
	case XDB_TYPE_BOOL:
		if (*(int8_t*)pVal) {
			memcpy (pBuf->buf + offset, "true", 5);
			return offset + 4;
		} else {
			memcpy (pBuf->buf + offset, "false", 6);
			return offset + 5;
		}
		break;
	case XDB_TYPE_INT:
		return offset + sprintf (pBuf->buf + offset, "%d", *(int32_t*)pVal);
	case XDB_TYPE_TINYINT:
		return offset + sprintf (pBuf->buf + offset, "%d", *(int8_t*)pVal);
	case XDB_TYPE_SMALLINT:
		return offset + sprintf (pBuf->buf + offset, "%d", *(int16_t*)pVal);
	case XDB_TYPE_BIGINT:
		return offset + sprintf (pBuf->buf + offset, "%"PRIi64, *(int64_t*)pVal);
	case XDB_TYPE_FLOAT:
		return offset + sprintf (pBuf->buf + offset, "%f", *(float*)pVal);
	case XDB_TYPE_DOUBLE:
		return offset + sprintf (pBuf->buf + offset, "%f", *(double*)pVal);
	case XDB_TYPE_CHAR:
	case XDB_TYPE_VCHAR:
		len = *(uint16_t*)(pVal-2);
		xdb_buf_resize (pBuf, offset + len);
		ptr = pBuf->buf + offset;
		*ptr++ = '\'';
		memcpy (pBuf->buf + offset + 1, pVal, len);
		ptr += len;
		*ptr++ = '\'';
		return offset + len + 2;
	case XDB_TYPE_BINARY:
	case XDB_TYPE_VBINARY:
		len = *(uint16_t*)(pVal-2);
		xdb_buf_resize (pBuf, offset + len * 2);
		ptr = pBuf->buf + offset;
		*ptr++ = 'X';
		*ptr++ = '\'';
		for (int h = 0; h < len; ++h) {
			*ptr++ = s_xdb_hex_2_str[((uint8_t*)pVal)[h]][0];
			*ptr++ = s_xdb_hex_2_str[((uint8_t*)pVal)[h]][1];
		}
		*ptr++ = '\'';
		return offset + len + 3;
	case XDB_TYPE_TIMESTAMP:
		return offset + xdb_timestamp_sprintf (*(int64_t*)pVal, pBuf->buf + offset, 64);
	case XDB_TYPE_INET:
		return offset + xdb_inet_sprintf (pVal, pBuf->buf + offset, 64);
		break;
	case XDB_TYPE_MAC:
		return offset + xdb_mac_sprintf (pVal, pBuf->buf + offset, 64);
	default:
		return 0;
	}
}
#endif

#if 0
// check if table has change, and the subscriber has the table, then append to the list for send
typedef struct {
	uint32_t	refcnt;
	uint64_t	len;
	xdb_buf_t	row_data;
} xdb_binlog_trans_t;

typedef struct {
	uint32_t				row_len;	// ms4b is type insert, delete, update old, update new, update bitmap + update flds list {fld cnt fld list[], fld bitmap u64[]}
	uint16_t				db_xoid;
	uint16_t				tbl_xoid;
	uint8_t					bin_data[];	// for update, old row, new row, (upd cnt, fid list)4B align
} xdb_binlog_row_t;
#endif

XDB_STATIC int 
xdb_initial_sync (xdb_subscribe_t *pSubscribe)
{
	if (!pSubscribe->bSync) {
		char		*sql_buf = NULL;
		sql_buf = xdb_malloc (XDB_MAX_SQL_BUF);
		if (NULL == sql_buf) {
			xdb_errlog ("Can't alloc memory\n");
			return XDB_E_MEMORY;
		}

		if (pSubscribe->bReplica) {			
			xdb_vec_t *pDbs = &pSubscribe->db_list;
			for (int i = 0; i < pDbs->count; ++i) {
				xdb_dbm_t *pDbm = pDbs->pEle[i];
				int slen = xdb_dump_create_db (pDbm, sql_buf, XDB_MAX_SQL_BUF, XDB_DUMP_EXIST);
				xdb_subscribe_send (pSubscribe, sql_buf, slen);
			}
		}
		
		xdb_vec_t *pTbls = &pSubscribe->tbl_list;
		for (int i = 0; i < pTbls->count; ++i) {
			xdb_tblm_t *pTblm = pTbls->pEle[i];
			if (pSubscribe->bReplica) {
				int slen = xdb_dump_create_table (pTblm, sql_buf, XDB_MAX_SQL_BUF, XDB_DUMP_EXIST|XDB_DUMP_FULLNAME);
				xdb_subscribe_send (pSubscribe, sql_buf, slen);
			}
			xdb_res_t *pRes = xdb_pexec (pSubscribe->pConn, "SELECT * FROM %s.%s", XDB_OBJ_NAME(pTblm->pDbm), XDB_OBJ_NAME(pTblm));
			xdb_row_t	*pRow;
			while (NULL != (pRow = xdb_fetch_row (pRes))) {
				int slen = xdb_row2sql (pRes, pRow, NULL, sql_buf, XDB_MAX_SQL_BUF);
				xdb_subscribe_send (pSubscribe, sql_buf, slen);
			}					
			xdb_free_result (pRes);
		}
		xdb_free (sql_buf);
	}

	XDB_OBJ_WRLOCK (pSubscribe);
	if (pSubscribe->cud_cache.count) {
		xdb_subscribe_send (pSubscribe, (const char*)pSubscribe->cud_cache.pBuf, pSubscribe->cud_cache.count);
		xdb_vec_free (&pSubscribe->cud_cache);
	}
	pSubscribe->bSync = true;
	XDB_OBJ_WRUNLOCK (pSubscribe);

	return XDB_OK;
}

XDB_STATIC int 
xdb_subscribe (xdb_stmt_subscribe_t *pStmt)
{
	xdb_conn_t* pConn = pStmt->pConn;
	xdb_buf_t	dbuf;
	xdb_buf_init (&dbuf);
	bool		bGlobal = false;

	xdb_subscribe_t *pSubscribe = xdb_find_subscriber (pStmt->sub_name);
	if (pSubscribe != NULL) {
		pSubscribe->pConn = pConn;
		return XDB_OK;
	}
	
	pSubscribe = xdb_calloc (sizeof(xdb_subscribe_t));
	XDB_EXPECT (NULL != pSubscribe, XDB_E_MEMORY, "Can't alloc memory");
	xdb_strcpy (XDB_OBJ_NAME(pSubscribe), pStmt->sub_name);
	pSubscribe->pConn = pConn;
	pSubscribe->bReplica = pStmt->bReplica;
	pConn->pSubscribe = pSubscribe;

#if 0
	xdb_pub_t *pPub = xdb_find_pub (pStmt->pub_name);
	if (pPub != NULL) {
		XDB_EXPECT (NULL != pPub, XDB_E_NOTFOUND, "Can't find publication '%s'", pStmt->pub_name);
		pSubscribe->pPub = pPub;
	
		XDB_OBJ_ID(pSubscribe) = -1;
		xdb_objm_add (&pPub->sub_list, pSubscribe);
	} else {
#endif

	if (NULL != pStmt->dbs) {
		xdb_pubsublog ("want dbs: %s\n", pStmt->dbs);
		pSubscribe->dbs = xdb_strdup (pStmt->dbs, 0);
		XDB_EXPECT (NULL != pSubscribe->dbs, XDB_E_MEMORY, "Can't alloc memory");

		xdb_token_t token = XDB_TOK_INIT(pStmt->dbs);
		xdb_token_type type = xdb_next_token (&token);
		while (XDB_TOK_ID == type) {
			xdb_dbm_t *pDbm = xdb_find_db (token.token);;
			if (NULL != pDbm) {
				xdb_vec_add (&pSubscribe->db_list, pDbm);
				xdb_vec_add (&pDbm->sub_list, pSubscribe);
				xdb_pubsublog ("  sub db %s\n", XDB_OBJ_NAME(pDbm));
				for (int t = 0; t < XDB_OBJM_MAX(pDbm->db_objm); ++t) {
					xdb_tblm_t *pTblm = XDB_OBJM_GET(pDbm->db_objm, t);
					xdb_vec_add (&pSubscribe->tbl_list, pTblm);
				}
			}
			type = xdb_next_token (&token);
			if (XDB_TOK_COMMA == type) {
				type = xdb_next_token (&token);
			}
		}

		bGlobal = false;
	}

	if (NULL != pStmt->tables) {
		xdb_pubsublog ("want tables: %s\n", pStmt->tables);
		pSubscribe->tables = xdb_strdup (pStmt->tables, 0);
		XDB_EXPECT (NULL != pSubscribe->tables, XDB_E_MEMORY, "Can't alloc memory");

		xdb_token_t token = XDB_TOK_INIT(pStmt->tables);
		xdb_token_type type = xdb_next_token (&token);
		while (XDB_TOK_ID == type) {
			xdb_tblm_t *pTblm = xdb_parse_dbtblname (pConn, &token);
			if (NULL != pTblm) {
				xdb_pubsublog ("  sub table %s\n", XDB_OBJ_NAME(pTblm));
				xdb_vec_add (&pTblm->sub_list, pSubscribe);
				xdb_vec_add (&pSubscribe->tbl_list, pTblm);
				xdb_vec_add (&pSubscribe->db_list, pTblm->pDbm);
			}
			type = token.tk_type;
			if (XDB_TOK_COMMA == type) {
				type = xdb_next_token (&token);
			}
		}

		bGlobal = false;
	}

	if (bGlobal) {
		for (int i = 0; i < XDB_OBJM_MAX(s_xdb_db_list); ++i) {
			xdb_dbm_t *pDbm = XDB_OBJM_GET(s_xdb_db_list, i);
			if ((NULL != pDbm) && !pDbm->bSysDb) {
				xdb_vec_add (&pSubscribe->db_list, pDbm);
				xdb_vec_add (&pDbm->sub_list, pSubscribe);
				xdb_pubsublog ("  sub db %s\n", XDB_OBJ_NAME(pDbm));
				for (int t = 0; t < XDB_OBJM_MAX(pDbm->db_objm); ++t) {
					xdb_tblm_t *pTblm = XDB_OBJM_GET(pDbm->db_objm, t);
					xdb_vec_add (&pSubscribe->tbl_list, pTblm);
				}
			}
		}
		if (pSubscribe->bReplica) {
			xdb_vec_add (&s_xdb_replicas_global, pSubscribe);
		}
	}

	xdb_sysdb_add_sub (pSubscribe);

	return XDB_OK;

error:
	xdb_free (pSubscribe->dbs);
	xdb_free (pSubscribe->tables);
	xdb_free (pSubscribe);
	return -pConn->conn_res.errcode;
}

#if 0
XDB_STATIC int
xdb_handle_subscribe (xdb_conn_t *pConn)
{
	xdb_subscribe_t *pSubscribe = pConn->pSubscribe;
	
	if (pSubscribe->bCreate) {
		xdb_row_t *pRow, *pRowDb, *pRowTbl;
		xdb_res_t*pRes = xdb_exec (pStmt->pConn, "SELECT database,schema FROM system.databases WHERE database != 'system'");
		while ((pRow = xdb_fetch_row(pRes)) != NULL) {
			// create DB
			int len;
			xdb_subscribe_send (pSubscribe, xdb_column_str2(pRes, pRow, 1, &len), len);
			// use DB
			sprintf (dbuf.buf, "USE %s", xdb_column_str2(pRes, pRow, 1, &len));
			xdb_subscribe_send (pSubscribe, dbuf.buf, strlen(dbuf.buf));
			pResDb = xdb_pexec (pStmt->pConn, "SELECT table,schema FROM system.tables WHERE database='%s'", xdb_column_str(pRes, pRow, 0));
			while ((pRowDb = xdb_fetch_row(pResTbl)) != NULL) {
				// create table
				xdb_subscribe_send (pSubscribe, pRowDb[1], (uint16_t*)(pRowDb[1]-2));
				pResTbl = xdb_pexec (pStmt->pConn, "SELECT * FROM %s", pRowDb[0]);
				xdb_meta_t	*pMeta = (xdb_meta_t*)pResTbl->col_meta;
				xdb_col_t	*pCol;
				while ((pRowTbl = xdb_fetch_row(pResTbl)) != NULL) {
					int len = sprintf (dbuf.buf, "INSERT INTO %s (", );
					for (int i = 0; i < pMeta->col_count; ++i) {
						pCol = xdb_column_meta (pResTbl->col_meta, i);
						xdb_buf_resize (&dbuf, len + pCol->col_nmlen + 16); // for ) VALUES (
						memcpy (buf+len, pCol->col_name, pCol->col_nmlen);
						len += pCol->col_nmlen;
						dbuf.buf[len++] = ',';
					}
					buf[len-1] = ')';
					len += sprintf (buf+len, " VALUES (");
					for (int i = 0; i < pMeta->fld_count; ++i) {
						len = xdb_dsprint_field (pRowTbl, &dbuf, len);
						dbuf.buf[len++] = ',';
					}
					dbuf.buf[len-1] = ')';
					dbuf.buf[len] = '\0';
					// insert row
					xdb_subscribe_send (pSubscribe, buf.buf, len);
				}
			}
			xdb_free_result (pResTbl);
		}
		xdb_free_result (pRes);
	}

	XDB_OBJ_ID(pSubscribe) = -1;
	xdb_objm_add (&pPub->sub_list, pSubscribe);
	xdb_buf_free (&dbuf);
	return XDB_OK;

error:
	xdb_buf_free (&dbuf);
	xdb_free_result (pRes);
	xdb_free_result (pResDb);
	xdb_free_result (pResTbl);
}
#endif

XDB_STATIC int 
xdb_pub_notify (xdb_tblm_t *pTblm, const char *sql, int len)
{
	int count = XDB_OBJM_MAX(s_xdb_pub_list);
	for (int i = 0; i < count; ++i) {
		xdb_pub_t *pPub = XDB_OBJM_GET(s_xdb_pub_list, i);
		if (NULL != pPub) {
			int scount = XDB_OBJM_MAX(pPub->sub_list);
			for (int i = 0; i < scount; ++i) {
				xdb_subscribe_t *pSub = XDB_OBJM_GET(pPub->sub_list, i);
				if (NULL != pSub) {
					xdb_subscribe_send2 (pSub, sql, len);
				}
			}
		}
	}

	for (int i = 0; i < pTblm->sub_list.count; ++i) {
		xdb_pubsublog ("### sent to subscriber\n");
		xdb_subscribe_send2 (pTblm->sub_list.pEle[i], sql, len);
	}

	return 0;
}

#if 0
XDB_STATIC int 
xdb_drop_server (xdb_stmt_svr_t *pStmt)
{
	xdb_server_t *pSvr = xdb_find_server (pStmt->svr_name);
	if (pStmt->bIfExistOrNot && (NULL == pSvr)) {
		return XDB_OK;
	}

	xdb_sysdb_del_svr (pSvr);

	pSvr->bDrop = true;

	int sockfd = xdb_sock_open (AF_INET, SOCK_STREAM, 0);
	if (sockfd >= 0) {
		struct sockaddr_in serverin = {AF_INET, htons(pSvr->svr_port), {inet_addr ("127.0.0.1")}};
		xdb_sock_connect(sockfd, (struct sockaddr*)&serverin, sizeof(struct sockaddr_in));		
		xdb_sock_close (sockfd);
	}

	return XDB_OK;
}
#endif

#define XDB_POLL_BUF	(1024*1024)

const void * xdb_poll (xdb_conn_t *pConn, int *pLen, uint32_t timeout)
{
	int	len, rlen;

	if (xdb_unlikely (NULL == pConn->poll_buf)) {
		pConn->poll_buf = xdb_malloc (XDB_POLL_BUF);
		if (NULL == pConn->poll_buf) {
			return NULL;
		}
		pConn->poll_size = XDB_POLL_BUF;
	}

	len = read (pConn->sockfd, pConn->poll_buf, 16);
	if (xdb_unlikely (len <= 0)) {
		return NULL;
	}
	pConn->poll_buf[len] = '\0';
	xdb_pubsublog ("%d recv %d: %s\n", pConn->sockfd, len, pConn->poll_buf);
	char		*sql = pConn->poll_buf;
	if (xdb_unlikely ('$' != *pConn->poll_buf)) {
		return NULL;
	}

	int slen = 0;
	sql++;
	while (*sql && *sql != '\n') {
		slen = slen * 10 +  (*sql++ - '0');
	}
	if (*sql != '\n') {
		return NULL;
	}
	sql++;
	rlen = len - ((void*)sql - (void*)pConn->poll_buf);
	if (rlen <= slen) {
		int nlen = slen - rlen;
		if (len + nlen + 1 >  pConn->poll_size) {
			int sql_off = sql - pConn->poll_buf;
			char *pBuf = xdb_realloc (pConn->poll_buf, len + nlen + 1);
			if (NULL == pBuf) {
				return NULL;
			}
			pConn->poll_size = len + nlen + 1;
			sql = pBuf + sql_off;
			pConn->poll_buf = pBuf;
		}
		int nn = read (pConn->sockfd, pConn->poll_buf + len, nlen);
		if (nn < nlen) {
			return NULL;
		}
	}
	len = slen;
	sql[len] = '\0';
	xdb_pubsublog ("%d recv %d: %s\n", pConn->sockfd, len, sql);

	*pLen = len;
	return sql;
}

#if 0
XDB_STATIC int 
xdb_ddl_sync (xdb_dbm_t *pDbm, xdb_dbm_t pTblm, const char *sql)
{
	xdb_send
}
#endif
