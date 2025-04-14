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

#ifndef __CROSS_CFG_H__
#define __CROSS_CFG_H__

/******************************************************************************
	CrossDB Config
******************************************************************************/

#define XDB_VERSION			"0.12.0"

#define XDB_MAX_DB			1024 // at most 4096
#define XDB_MAX_TBL			4095 // per DB
#define XDB_MAX_COLUMN		1024 // per table
#define XDB_MAX_INDEX		16	 // at most 64 per table
#define XDB_MAX_FKEY		16
#define XDB_NAME_LEN		64
#define XDB_MAX_MATCH_COL	64
#define XDB_MAX_ROWS		((1U<<31) - 1)
#define XDB_MAX_SQL_BUF		(1024*1024)
#define XDB_MAX_JOIN		8

#define XDB_PATH_LEN		512

#ifndef XDB_ENABLE_SERVER
#define XDB_ENABLE_SERVER	1
#endif

#ifndef XDB_ENABLE_PUBSUB
#define XDB_ENABLE_PUBSUB	1
#endif

#endif // __CROSS_CFG_H__
