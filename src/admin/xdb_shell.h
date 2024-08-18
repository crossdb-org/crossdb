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

#ifndef __XDB_SHELL_H__
#define __XDB_SHELL_H__

XDB_STATIC int 
xdb_shell_loop (xdb_conn_t* pConn, const char *prompt, const char *db);

XDB_STATIC void 
xdb_output_table (xdb_conn_t *pConn, xdb_res_t *pRes);

XDB_STATIC bool 
xdb_is_sql_complete (char *sql, bool split);

#endif // __XDB_SHELL_H__