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

static xdb_objm_t	s_xdb_svr_list;

XDB_STATIC const char* 
xdb_ip2str (uint32_t ip)
{
	static char idx = 0;
	static char bufpool[8][16];
	char *buf = bufpool[idx++&7];
	sprintf (buf, "%d.%d.%d.%d", ip>>24, (ip>>16)&0xff, (ip>>8)&0xff, ip&0xff);
	return buf;
}

XDB_STATIC void* 
xdb_handle_client (void *pArg)
{
	int	len, rlen;
	xdb_conn_t*	pConn = pArg;
	int clientfd = pConn->sockfd;
	XDB_BUF_DEF(sqlbuf, 65536);

	xdb_sock_SetTcpNoDelay (pConn->sockfd, 1);

	while ((len = xdb_sock_read (clientfd, sqlbuf, sqlbuf_size - 1)) > 0) {
		sqlbuf[len] = '\0';
		xdb_svrlog ("%d recv %d: %s\n", clientfd, len, sqlbuf);
		char		*sql = sqlbuf;
		if ('$' == *sqlbuf) {
			int slen = 0;
			sql++;
			while (*sql && *sql != '\n') {
				slen = slen * 10 +  (*sql++ - '0');
			}
			if (*sql != '\n') {
				break;
			}
			sql++;
			rlen = len - ((void*)sql - (void*)sqlbuf);
			if (rlen < slen) {
				int nlen = slen - rlen;
				int new_len = len + nlen + 1;
				if (new_len >=  sqlbuf_size) {
					new_len = XDB_ALIGN4K(new_len);
					uint32_t sqloff = sql - sqlbuf;
					XDB_BUF_ALLOC(sqlbuf, new_len);
					if (NULL == sqlbuf) {
						break;
					}
					sql = sqlbuf + sqloff;
				}
				int nn = xdb_sock_read (clientfd, sqlbuf + len, nlen);
				if (nn < nlen) {
					break;
				}
			}
			len = slen;
			sql[len] = '\0';
			xdb_svrlog ("%d run %d: %s\n", clientfd, len, sql);			
		} else {
			if ((!strncasecmp (sqlbuf, "exit", 4) || !strncasecmp (sqlbuf, "quit", 4)) && isspace(sqlbuf[4])) {
				break;
			}
			rlen = 0;
			if (!xdb_is_sql_complete (sqlbuf, false)) {
				while ((rlen = xdb_sock_read (clientfd, sqlbuf + len, sqlbuf_size)) > 0) {
					len += rlen;				
					sqlbuf[len] = '\0';
					if (xdb_is_sql_complete (sqlbuf, false)) {
						break;
					}
				}
				if (rlen <= 0) {
					break;
				}
					
			}
		}
		xdb_exec_out (pConn, sql, len);
	}

	xdb_svrlog ("close %d\n", clientfd);
	XDB_BUF_FREE(sqlbuf);
	xdb_close (pConn);
	return NULL;
}

XDB_STATIC void* 
xdb_run_server (void *pArg)
{
	int	ret = -1;
	xdb_server_t	*pServer = pArg;
	int port = pServer->svr_port;

    struct sockaddr_in serverin;
	xdb_sockaddr_init (&serverin, port, "0.0.0.0");
	unsigned int sock_val = 1;

	int sockfd = xdb_sock_open (AF_INET, SOCK_STREAM, 0);
	if (sockfd < 0) {
		xdb_errlog ("socket() error : %s", strerror(errno));
		ret = -1;
		goto exit;
	}

	setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, (void *)&sock_val, sizeof(sock_val));
#if !defined(_WIN32) && !defined(__CYGWIN__) && !defined(__MSYS__)
	setsockopt(sockfd, SOL_SOCKET, SO_REUSEPORT, (void *)&sock_val, sizeof(sock_val));
#endif

	ret = bind(sockfd, (struct sockaddr*)&serverin, sizeof (serverin));
	if (ret < 0) {
		xdb_errlog ("fd %d %s:%d bind() error %s", sockfd, xdb_ip2str(0), port, strerror(errno));
		goto exit;
	}

	xdb_svrlog ("Run server %s:%d\n", XDB_OBJ_NAME(pServer), port);
	pServer->sockfd = sockfd;

	while (!pServer->bDrop) {
		ret = listen(sockfd, 5);
		if (ret < 0) {
			xdb_errlog ("fd %d %s:%d bind() listen() error %s", sockfd, xdb_ip2str(0), port, strerror(errno));
			goto exit;;
		}

		struct sockaddr_in cliaddr_in;
		socklen_t clilen = sizeof(cliaddr_in);
		int 	clientfd;
		clientfd = accept(sockfd, (struct sockaddr *)&cliaddr_in, &clilen);
		if(clientfd < 0) {
			xdb_errlog ("%d Can't accept: %s\n", sockfd, strerror(errno));
			continue;
		}
		if (pServer->bDrop) {
			close (clientfd);
			goto exit;
		}
		xdb_svrlog ("Accept new client %d %s:%d...\n", clientfd, xdb_ip2str(ntohl(cliaddr_in.sin_addr.s_addr)), ntohs(cliaddr_in.sin_port));

		xdb_thread_t thread;
		xdb_conn_t* pConn = xdb_open (NULL);
		if (NULL != pConn) {
			pConn->sockfd = clientfd;
			pConn->conn_stdout = fdopen (pConn->sockfd, "w");
			if (NULL == pConn->conn_stdout) {
				pConn->conn_stdout = (void*)(uintptr_t)pConn->sockfd;
			}
			pConn->pServer = pServer;
			xdb_create_thread (&thread, NULL, xdb_handle_client, (void*)pConn);
		} else {
			xdb_sock_close (clientfd);
		}
	}

exit:

	xdb_svrlog ("Exit server %s:%d\n", XDB_OBJ_NAME(pServer), port);

	xdb_free (pServer);

	return NULL;
}

XDB_STATIC xdb_server_t* 
xdb_find_server (const char *svr_name)
{
	return xdb_objm_get (&s_xdb_svr_list, svr_name);
}

XDB_STATIC int 
xdb_create_server (xdb_stmt_svr_t *pStmt)
{
	xdb_server_t *pSvr = xdb_find_server (pStmt->svr_name);
	if (pStmt->bIfExistOrNot && (NULL != pSvr)) {
		return XDB_OK;
	}

	xdb_conn_t* pConn = pStmt->pConn;
	xdb_server_t *pServer = xdb_calloc (sizeof(xdb_server_t));
	XDB_EXPECT (NULL != pServer, XDB_E_MEMORY, "Can't alloc memory");
	xdb_strcpy (XDB_OBJ_NAME(pServer), pStmt->svr_name);
	pServer->svr_port = pStmt->svr_port;

	XDB_OBJ_ID(pServer) = -1;
	xdb_objm_add (&s_xdb_svr_list, pServer);

	xdb_sysdb_add_svr (pServer);

	xdb_create_thread (&pServer->tid, NULL, xdb_run_server, pServer);

	return 0;

error:
	xdb_free (pServer);
	return -pConn->conn_res.errcode;
}

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
		struct sockaddr_in serverin;
		xdb_sockaddr_init (&serverin, pSvr->svr_port, "127.0.0.1");
		xdb_sock_connect(sockfd, (struct sockaddr*)&serverin, sizeof(serverin));
		xdb_sock_close (sockfd);
	}

	return XDB_OK;
}

XDB_STATIC void xdb_native_out (xdb_conn_t *pConn, xdb_res_t *pRes)
{
	uint64_t slen = (pRes->len_type & XDB_LEN_MASK) + pRes->data_len;
	xdb_meta_t *pMeta = (xdb_meta_t*)pRes->col_meta;
	memcpy (pRes + 1, pMeta, pRes->meta_len);
	size_t len = xdb_sock_write (pConn->sockfd, pRes, slen);
	if (len < slen) {
		xdb_errlog ("send %"PRIi64" < %"PRIi64"\n", len, slen);
	}
	xdb_svrlog ("send response %d to client\n", len);
#if XDB_LOG_FLAGS & XDB_LOG_SVR
	//xdb_hexdump (pRes, len);
#endif
}

static char *s_xdb_banner_server = 
"   _____                   _____  ____      _          \n"  
"  / ____|                 |  __ \\|  _ \\   _| |_    CrossDB Server v%s\n"
" | |     _ __ ___  ___ ___| |  | | |_) | |_   _|   Port: %d\n"
" | |    | '__/ _ \\/ __/ __| |  | |  _ <    |_|     DataDir: %s\n"
" | |____| | | (_) \\__ \\__ \\ |__| | |_) |          \n"
"  \\_____|_|  \\___/|___/___/_____/|____/            https://crossdb.org\n\n";


int
xdb_start_server (const char *host, uint16_t port, const char *datadir, bool bQuite)
{
	if (!bQuite) {
		xdb_print (s_xdb_banner_server, xdb_version(), port, datadir);
	}
	
	if (NULL == datadir) {
		datadir = "/var/lib/crossdb";
	}
	xdb_mkdir (datadir);
	
	xdb_conn_t *pConn = xdb_open (NULL);
	
	xdb_pexec (pConn, "OPEN DATADIR '%s'", datadir);
	
	xdb_pexec (pConn, "SET DATADIR='%s'", datadir);
	
	xdb_pexec (pConn, "CREATE SERVER crossdb PORT=%d", port);

	if (!bQuite) {
		xdb_shell_loop (pConn, NULL, NULL, true);
	} else {
		while (1) {
			sleep (1000);
		}
	}
	
	return XDB_OK;
}

