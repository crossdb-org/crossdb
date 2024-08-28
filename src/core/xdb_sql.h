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

#ifndef __XDB_SQL_H__
#define __XDB_SQL_H__

#define XDB_ROW_BUF_SIZE	(1024*1024)


XDB_STATIC int 
xdb_exec_out (xdb_conn_t *pConn, const char *sql, int len);

XDB_STATIC int 
xdb_str_escape (char *dest, const char *src, int len);

#endif // __XDB_SQL_H__
