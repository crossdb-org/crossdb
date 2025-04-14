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

#ifndef __XDB_PUBSUB_H__
#define __XDB_PUBSUB_H__

typedef struct xdb_sub_t {
	xdb_obj_t			obj;
	xdb_conn_t			*pConn;
	char				pub_name[XDB_NAME_LEN + 1];
	char				pub_host[XDB_NAME_LEN*2 + 1];
	char				*tables;
	int					pub_port;
	xdb_thread_t		tid;
} xdb_sub_t;

typedef struct xdb_pub_t {
	xdb_obj_t			obj;
	xdb_objm_t			sub_list;
	int					format;	// SQL, JSON
} xdb_pub_t;

typedef struct xdb_subscribe_t {
	xdb_obj_t			obj;
	xdb_conn_t			*pConn;
	bool				bCreate;
	char				*tables;
	char				client[XDB_NAME_LEN + 1];
	xdb_pub_t			*pPub;	
	xdb_vec_t			tbl_list;
	bool				bSync;
	xdb_vec_t			cud_cache;
} xdb_subscribe_t;


#if (XDB_ENABLE_PUBSUB == 1)
XDB_STATIC int 
xdb_pub_notify (xdb_tblm_t *pTblm, const char *sql, int len);
XDB_STATIC int 
xdb_initial_sync (xdb_subscribe_t *pSubscribe);
#endif

#endif // __XDB_PUBSUB_H__
