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

#ifndef xdb_svrlog
#if XDB_LOG_FLAGS & XDB_LOG_SVR
#define xdb_svrlog(...)	xdb_print(__VA_ARGS__)
#else
#define xdb_svrlog(...)
#endif
#endif

XDB_STATIC int
xdb_reconnect (xdb_conn_t *pConn)
{
	int ret = XDB_E_SOCK;

	pConn->sockfd = xdb_sock_open (AF_INET, SOCK_STREAM, 0);
	if (pConn->sockfd < 0) {
		xdb_errlog ("Can't create socket\n");
		goto error;
	}
	struct sockaddr_in serverin;
	xdb_sockaddr_init (&serverin, pConn->port, pConn->host);

	ret = xdb_sock_connect (pConn->sockfd, (struct sockaddr*)&serverin, sizeof(serverin));
	if (0 != ret) {
		xdb_errlog ("Can't connect %s:%d, %s\n", pConn->host, pConn->port, strerror(errno));
		goto error;
	}

	xdb_sock_SetTcpNoDelay (pConn->sockfd, 1);

	xdb_res_t *pRes = xdb_exec (pConn, "SET FORMAT=NATIVELE");
	XDB_RESCHK(pRes, goto error);
	if (*pConn->cur_db != '\0') {
		pRes = xdb_pexec (pConn, "USE %s", pConn->cur_db);
		XDB_RESCHK(pRes, goto error);
	}
	return XDB_OK;

error:
	if (pConn->sockfd >= 0) {
		xdb_sock_close (pConn->sockfd);
		pConn->sockfd = -1;
	}
	return ret;
}

xdb_conn_t*
xdb_connect (const char *host, const char *user, const char *pass, const char *db, uint16_t port)
{
	xdb_init ();

	if ((NULL == host) || ('\0' == *host)) {
		host = "127.0.0.1";
	} else if (0 == port) {
		port = XDB_SVR_PORT;
	}

	if (0 == port) {
		return xdb_open (db);
	}

	xdb_conn_t* pConn = xdb_calloc (sizeof (xdb_conn_t));
	if (NULL == pConn) {
		return NULL;
	}

	xdb_conn_init (pConn);

	if (NULL != db) {
		xdb_strcpy (pConn->cur_db, db);
	}
	xdb_strcpy (pConn->host, host);
	pConn->port = port;
	pConn->conn_client = true;

	int ret = xdb_reconnect (pConn);
	if (XDB_OK == ret) {
		xdb_atomic_inc (&s_xdb_conn_count);
		return pConn;
	}

	xdb_free (pConn);
	return NULL;
}

XDB_STATIC xdb_res_t*
xdb_fetch_res_sock (xdb_conn_t *pConn)
{
	xdb_res_t *pRes = &pConn->conn_res;

	// read result
	uint32_t len = xdb_sock_read (pConn->sockfd, pRes, sizeof(*pRes));
	XDB_EXPECT (len >= sizeof(*pRes), XDB_E_SOCK, "Featch wrong header");

	// read 
	int reslen = sizeof (xdb_queryRes_t) + pRes->data_len;
	if (0 == pRes->meta_len) {
		if (pRes->data_len > 0) {
			len = xdb_sock_read (pConn->sockfd, &pConn->conn_msg, pRes->data_len);
			XDB_EXPECT (len >= pRes->data_len, XDB_E_SOCK, "Featch wrong header");
			pRes->row_data = (uintptr_t)pConn->conn_msg.msg;
		}
	} else {
		int buf_len = reslen + pRes->meta_len + pRes->col_count * 8 + 7;
		pRes = xdb_queryres_alloc (pConn, buf_len);
		if (NULL != pRes) {
			return pRes;
		}
		xdb_queryRes_t *pQueryRes = pConn->pQueryRes;

		pRes = &pQueryRes->res;
		pQueryRes->res = pConn->conn_res;
		pQueryRes->pConn = pConn;
		pQueryRes->pStmt = NULL;

		int64_t rdlen = sizeof (xdb_queryRes_t);
		while (rdlen < reslen) {
			len = xdb_sock_read (pConn->sockfd, (void*)pQueryRes+rdlen, reslen-rdlen);
			if (len > 0) {
				rdlen += len;
			} else {
				return NULL;
			}
		}
		xdb_svrlog ("get response %d from server\n", sizeof(*pRes) + pRes->data_len);
#if XDB_LOG_FLAGS & XDB_LOG_SVR
		//xdb_hexdump (pRes, sizeof(*pRes) + pRes->data_len);
#endif

		xdb_meta_t *pMeta = (xdb_meta_t*)(pRes + 1);
		pRes->col_meta = (uintptr_t)pMeta;
		pMeta->col_list = ((uintptr_t)pQueryRes + sizeof (xdb_queryRes_t) + pRes->data_len + 7) & (~7LL);
		uint64_t *pColList = (uint64_t*)pMeta->col_list;
		xdb_col_t *pCol = (xdb_col_t*)((void*)pMeta + pMeta->cols_off);
		int col_cnt = 0;
		//xdb_hexdump (pMeta, pRes->meta_len);
		while (pCol->col_len) {
			*pColList++ = (uintptr_t)pCol;
			if (++col_cnt >= pMeta->col_count) {
				break;
			}
			pCol = (void*)pCol + pCol->col_len;
			if ((uintptr_t)pCol - (uintptr_t)pRes > buf_len) {
				xdb_free_result (pRes);
				pRes = &pConn->conn_res;
				XDB_EXPECT (0, XDB_E_SOCK, "Featch wrong meta");
			}
		}
		xdb_init_rowlist (pQueryRes);
		pConn->ref_cnt++;
	}

error:
	return pRes;	
}

XDB_STATIC xdb_res_t* 
xdb_exec_client (xdb_conn_t *pConn, const char *sql, int len)
{
	xdb_res_t *pRes = &pConn->conn_res;
	if (xdb_unlikely (pConn->sockfd < 0)) {
		int ret = xdb_reconnect (pConn);
		XDB_EXPECT(ret == XDB_OK, XDB_E_SOCK, "Can't connect server");
	}
	xdb_svrlog ("send: '%s'\n", sql);
	// send socket
	char buf[64];
	int nn = sprintf (buf, "$%d\n", len);
	int wlen = xdb_sock_write (pConn->sockfd, buf, nn);
	XDB_EXPECT_SOCK(wlen == nn, XDB_E_SOCK, "Socket Error write %d of %d", wlen, nn);
	wlen = xdb_sock_write (pConn->sockfd, sql, len);
	XDB_EXPECT_SOCK(wlen == len, XDB_E_SOCK, "Socket Error write %d of %d", wlen, len);
	pRes = xdb_fetch_res_sock (pConn);
	if ((XDB_STMT_USE_DB == pRes->stmt_type) && (0 == pRes->errcode) && pRes->row_data) {
		xdb_strcpy (pConn->cur_db, (char*)pRes->row_data);
	}

error:
	return pRes;
}

// source | dump | shell | help
XDB_STATIC bool 
xdb_is_local_stmt (const char *sql)
{
	switch (sql[0]) {
	case 's':
	case 'S':
		switch (sql[1]) {
		case 'o':
		case 'O':
			if (!strncasecmp (sql, "SOURCE", 6)) {
				return true;
			}
			break;
		case 'h':
		case 'H':
			if (!strncasecmp (sql, "SHELL", 5)) {
				return true;
			}
			break;
		}
		break;
	case 'd':
	case 'D':
		if (!strncasecmp (sql, "DUMP", 4)) {
			return true;
		}
		break;
	case 'h':
	case 'H':
		if (!strncasecmp (sql, "HELP", 4)) {
			return true;
		}
		break;
	}
	return false;
}

