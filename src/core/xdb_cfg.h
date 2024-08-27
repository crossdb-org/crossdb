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

#ifndef __CROSS_CFG_H__
#define __CROSS_CFG_H__

/******************************************************************************
	CrossDB Config
******************************************************************************/

#define XDB_VERSION			"0.7.0"

#define XDB_MAX_DB			1024 // at most 4096
#define XDB_MAX_TBL			4095 // per DB
#define XDB_MAX_COLUMN		1024 // per table
#define XDB_MAX_INDEX		16	 // at most 64 per table
#define XDB_NAME_LEN		64
#define XDB_MAX_MATCH_COL	64
#define XDB_MAX_ROWS		((1U<<31) - 1)
#define XDB_MAX_SQL_BUF		(1024*1024)

#define XDB_PATH_LEN		256


#ifndef XDB_ENABLE_SERVER
#define XDB_ENABLE_SERVER 0
#endif

#endif // __CROSS_CFG_H__
