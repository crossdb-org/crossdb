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

#include "crossdb.c"

int main (int argc, char **argv)
{
	char		*db = NULL;
	char		ch;
	bool		bServer = false, bNeedPasswd = false, bQuite = false;
	#if (XDB_ENABLE_SERVER == 1)
	char		*datadir = NULL, *svrid = "1", *host = NULL, *user = NULL, *password = NULL;
	int			port = 0;
	#endif
	char		*esql = NULL;

	while ((ch = getopt(argc, argv, "h:u:p:e:P:D:R:I:Sq")) != -1) {
		switch (ch) {
		case '?':
			xdb_print ("Usage: xdb-cli [OPTIONS] [[path]/db_name]\n");
            xdb_print ("  -?                        Show this help\n");
		#if (XDB_ENABLE_SERVER == 1)
            xdb_print ("  -S                        Server: Start in server mode, default port %d\n", XDB_SVR_PORT);
			xdb_print ("  -I                        Server ID: <string>\n");
            xdb_print ("  -h <ip>                   IP address to bind to or connect to\n");
            xdb_print ("  -P <port>                 Port to listen or connect\n");
            xdb_print ("  -D <datadir>              Server: Data directory to store databases, default '%s'\n", XDB_DATA_DIR);
			xdb_print ("  -q                        Server: quite mode.\n");
            xdb_print ("  -u <user>                 Client user\n");
            xdb_print ("  -p                        Client password\n");
		#endif // XDB_ENABLE_SERVER
            xdb_print ("  -e <sql>                  Client: Execute command and quit.\n");
			return -1;
	#if (XDB_ENABLE_SERVER == 1)
		case 'S':
			bServer = true;
			if (0 == port) {
				port = XDB_SVR_PORT;
			}
			if (NULL == datadir) {
				datadir = XDB_DATA_DIR;
			}
			break;
		case 'I':
			svrid = optarg;
			break;
		case 'h':
			host = optarg;
			if (0 == port) {
				port = XDB_SVR_PORT;
			}
			break;
		case 'P':
			port = atoi (optarg);
			if (NULL == host) {
				host = "127.0.0.1";
			}
			break;
		case 'D':
			datadir = optarg;
			break;
		case 'u':
			user = optarg;
			break;
		case 'p':
			bNeedPasswd = true;
			break;
	#endif // XDB_ENABLE_SERVER
		case 'e':
			esql = optarg;
			break;
		case 'q':
			bQuite = true;
			break;
		}
	}

	if (bServer) {
	#if (XDB_ENABLE_SERVER == 1)
		xdb_start_server (host, port, svrid, datadir, bQuite);
	#endif
	} else {
		s_xdb_cli = true;
		if (bNeedPasswd) {
			xdb_print ("Enter password:\n");
		}
		if ((argc > 1) && (*argv[argc-1] != '-') && (argc > 2 ? (*argv[argc-2] != '-') : 1)) {
			db = argv[argc-1];
		}
	#if (XDB_ENABLE_SERVER == 1)
		xdb_conn_t *pConn = xdb_connect (host, user, password, (host || db) ? db : ":memory:", port);
	#else
		xdb_conn_t *pConn = xdb_open (db ? db : ":memory:");
	#endif
		if (NULL != pConn) {
			if (NULL != esql) {
				xdb_shell_process (pConn, esql);
			} else {
				xdb_shell_loop (pConn, NULL, NULL, bQuite);
			}
			xdb_close (pConn);
		}
	}

	return 0;
}
