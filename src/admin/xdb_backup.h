/******************************************************************************
* Copyright (c) 2024-present JC Wang. All rights reserved
*
*   https://crossdb.org
*   https://github.com/crossdb-org/crossdb
*
* This program is free software: you can use, redistribute, and/or modify it under
* the terms of the GNU Lesser General Public License, version 3 or later ("LGPL"),
* as published by the Free Software Foundation.
*
* This program is distributed in the hope that it will be useful, but WITHOUT
* ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS 
* FOR A PARTICULAR PURPOSE. See the GNU published General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public License along with 
* this program. If not, see <http://www.gnu.org/licenses/>.
******************************************************************************/

#ifndef __CROSS_BACKUP_H__
#define __CROSS_BACKUP_H__

XDB_STATIC int 
xdb_source (xdb_conn_t* pConn, const char *file);

XDB_STATIC int 
xdb_dump (xdb_conn_t* pConn, xdb_dbm_t *pDbm, const char *file, bool bNoDrop, bool bNoCreate, bool bNoData);

#endif // __CROSS_BACKUP_H__
