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

#include "crossdb.c"

int main (int argc, char **argv)
{
	char		*db = NULL;
	char		ch;
	bool		bServer = false, bNeedPasswd = false;
	#ifdef XDB_ENABLE_SERVER
	char		*datadir = NULL, *host = NULL, *user = NULL, *password = NULL;
	int			port = 0;
	#endif
	char		*esql = NULL;

	while ((ch = getopt(argc, argv, "h:u:p:e:P:D:R:S")) != -1) {
		switch (ch) {
		case '?':
			printf ("Usage: xdb-cli [OPTIONS] [[path]/db_name]\n");
            printf ("  -?                        Show this help\n");
		#ifdef XDB_ENABLE_SERVER
            printf ("  -S                        Server: Start in server mode\n");
            printf ("  -h <ip>                   IP address to bind to or connect to\n");
            printf ("  -P <port>                 Port to listen or connect\n");
            printf ("  -D <datadir>              Server: Data directory to store databases\n");
            printf ("  -u <user>                 Client user\n");
            printf ("  -p                        Client password\n");
		#endif // XDB_ENABLE_SERVER
            printf ("  -e <sql>                  Client: Execute command and quit.\n");
			return -1;
	#ifdef XDB_ENABLE_SERVER
		case 'S':
			bServer = true;
			if (0 == port) {
				port = XDB_SVR_PORT;
			}
			if (NULL == datadir) {
				datadir = XDB_DATA_DIR;
			}
			break;
		case 'h':
			host = optarg;
			break;
		case 'P':
			port = atoi (optarg);
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
		}
	}

	if (bServer) {
	#ifdef XDB_ENABLE_SERVER
		xdb_start_server (host, port, datadir, NULL);
	#endif
	} else {
		if (bNeedPasswd) {
			printf ("Enter password:\n");
		}
		if ((argc > 1) && (*argv[argc-1] != '-') && (argc > 2 ? (*argv[argc-2] != '-') : 1)) {
			db = argv[argc-1];
		}
	#ifdef XDB_ENABLE_SERVER
		xdb_conn_t *pConn = xdb_connect (host, user, password, db, port);
	#else
		xdb_conn_t *pConn = xdb_open (db);
	#endif
		if (NULL != pConn) {
			if (NULL != esql) {
				xdb_shell_process (pConn, esql);
			} else {
				xdb_shell_loop (pConn, NULL, NULL);
			}
			xdb_close (pConn);
		}
	}

	return 0;
}