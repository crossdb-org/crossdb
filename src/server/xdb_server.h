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

#ifndef __XDB_SERVER_H__
#define __XDB_SERVER_H__

#define XDB_SVR_PORT	7777
#ifndef _WIN32
#define XDB_DATA_DIR	"/var/xdb_data"
#else
#define XDB_DATA_DIR	"c:/crossdb/xdb_data"
#endif

typedef struct xdb_server_t {
	xdb_obj_t			obj;
	int					svr_port;
	int					sockfd;
	bool				bDrop;
	xdb_thread_t 		tid;
} xdb_server_t;

#if (XDB_ENABLE_PUBSUB == 1)
XDB_STATIC int 
xdb_pub_notify (const char *sql, int len);
#endif

#endif // __XDB_SERVER_H__
