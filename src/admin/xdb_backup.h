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

#ifndef __CROSS_BACKUP_H__
#define __CROSS_BACKUP_H__

XDB_STATIC int 
xdb_source (xdb_conn_t* pConn, const char *file);

XDB_STATIC int 
xdb_dump (xdb_conn_t* pConn, xdb_dbm_t *pDbm, const char *file, bool bNoDrop, bool bNoCreate, bool bNoData);

#endif // __CROSS_BACKUP_H__
