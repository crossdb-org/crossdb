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

#ifndef __XDB_SHELL_H__
#define __XDB_SHELL_H__

XDB_STATIC int 
xdb_shell_loop (xdb_conn_t* pConn, const char *prompt, const char *db);

XDB_STATIC void 
xdb_output_table (xdb_conn_t *pConn, xdb_res_t *pRes);

XDB_STATIC bool 
xdb_is_sql_complete (char *sql, bool split);

#endif // __XDB_SHELL_H__
