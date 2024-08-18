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

#ifndef __XDB_SQL_H__
#define __XDB_SQL_H__

#define XDB_ROW_BUF_SIZE	(1024*1024)


XDB_STATIC int 
xdb_exec_out (xdb_conn_t *pConn, const char *sql, int len);

XDB_STATIC int 
xdb_str_escape (char *dest, const char *src, int len);

#endif // __XDB_SQL_H__