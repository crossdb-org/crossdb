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

#undef XDB_STATIC
#define XDB_STATIC static

#include "../include/crossdb.h"
#include "core/xdb_cfg.h"
#include "lib/xdb_lib.c"
#include "core/xdb_common.h"
#include "parser/xdb_stmt.h"
#include "core/xdb_store.h"
#include "core/xdb_wal.h"
#include "core/xdb_db.h"
#include "core/xdb_crud.h"
#include "core/xdb_hash.h"
#include "core/xdb_sql.h"
#include "core/xdb_sysdb.h"
#include "core/xdb_vdata.h"
#include "core/xdb_table.h"
#include "core/xdb_index.h"
#include "core/xdb_trans.h"
#include "core/xdb_trigger.h"
#if (XDB_ENABLE_SERVER == 1)
#include "server/xdb_server.h"
#endif
#include "core/xdb_conn.h"
#include "admin/xdb_shell.h"
#include "admin/xdb_backup.h"
#include "core/xdb_wal.h"

#include "parser/xdb_parser.c"
#include "core/xdb_expr.c"
#include "core/xdb_store.c"
#include "core/xdb_sysdb.c"
#include "core/xdb_db.c"
#include "core/xdb_crud.c"
#include "core/xdb_index.c"
#include "core/xdb_hash.c"
#include "core/xdb_vdata.c"
#include "core/xdb_table.c"
#include "core/xdb_trans.c"
#include "core/xdb_trigger.c"
#include "core/xdb_conn.c"
#if (XDB_ENABLE_SERVER == 1)
#include "server/xdb_client.c"
#include "server/xdb_server.c"
#endif
#if (XDB_ENABLE_PUBSUB == 1)
#include "server/xdb_pubsub.c"
#endif
#include "core/xdb_sql.c"
#include "core/xdb_wal.c"
#if (XDB_ENABLE_JNI == 1)
#include "jni/xdb_jni.c"
#endif
#include "admin/xdb_shell.c"
#include "admin/xdb_backup.c"

static bool s_xdb_vdat[XDB_TYPE_MAX] = {
	[XDB_TYPE_VBINARY  ] = true,
	[XDB_TYPE_VCHAR    ] = true
};

const char* xdb_type2str(xdb_type_t tp) 
{
	const char *id2str[] = {
		[XDB_TYPE_NULL     ] = "NULL",
		[XDB_TYPE_BOOL	   ] = "BOOL",
		[XDB_TYPE_TINYINT  ] = "TINYINT",
		[XDB_TYPE_SMALLINT ] = "SMALLINT",
		[XDB_TYPE_INT      ] = "INT",
		[XDB_TYPE_BIGINT   ] = "BIGINT",
		[XDB_TYPE_UTINYINT ] = "UTINYINT",
		[XDB_TYPE_USMALLINT] = "USMALLINT",
		[XDB_TYPE_UINT     ] = "UINT",
		[XDB_TYPE_UBIGINT  ] = "UBIGINT",
		[XDB_TYPE_FLOAT    ] = "FLOAT",
		[XDB_TYPE_DOUBLE   ] = "DOUBLE",
		[XDB_TYPE_TIMESTAMP] = "TIMESTAMP",
		[XDB_TYPE_CHAR     ] = "CHAR",
		[XDB_TYPE_VCHAR    ] = "VARCHAR",
		[XDB_TYPE_BINARY   ] = "BINARY",
		[XDB_TYPE_VBINARY  ] = "VARBINARY",
		[XDB_TYPE_INET	   ] = "INET",
		[XDB_TYPE_MAC	   ] = "MAC",
	};
	return tp <= XDB_ARY_LEN(id2str) ? id2str[tp] : "Unkonwn";
}

static xdb_type_t s_xdb_prompt_type[] = {
	[XDB_TYPE_BOOL	  ] 	= XDB_TYPE_BIGINT,
	[XDB_TYPE_TINYINT  ] 	= XDB_TYPE_BIGINT,
	[XDB_TYPE_SMALLINT  ] 	= XDB_TYPE_BIGINT,
	[XDB_TYPE_INT	] 		= XDB_TYPE_BIGINT,
	[XDB_TYPE_BIGINT	]	= XDB_TYPE_BIGINT,
	[XDB_TYPE_FLOAT	]		= XDB_TYPE_DOUBLE,
	[XDB_TYPE_DOUBLE	]	= XDB_TYPE_DOUBLE,
	[XDB_TYPE_CHAR	]		= XDB_TYPE_CHAR,
	[XDB_TYPE_VCHAR	]		= XDB_TYPE_VCHAR,
	[XDB_TYPE_BINARY ]		= XDB_TYPE_BINARY,
	[XDB_TYPE_VBINARY ]		= XDB_TYPE_VBINARY,
	[XDB_TYPE_TIMESTAMP] 	= XDB_TYPE_BIGINT,
	[XDB_TYPE_INET]			= XDB_TYPE_INET,
	[XDB_TYPE_MAC]			= XDB_TYPE_MAC,
};

static volatile bool s_xdb_bInit = false;
static bool s_xdb_cli = false;

int 
xdb_init ()
{
	if (!s_xdb_bInit) {
#if (XDB_ENABLE_SERVER == 1)
		xdb_sock_init ();
#endif
		s_xdb_bInit = true;
	}
	xdb_vdat_init ();
	xdb_hex_init ();
	xdb_bgtask_init ();
	
	return XDB_OK;
}

int 
xdb_exit ()
{
	// Close all opened DBs
	s_xdb_bInit = false;
	while (s_xdb_bg_run) {
		xdb_yield ();
	}
	xdb_close_all_db (NULL);
	xdb_sysdb_exit ();

	return XDB_OK;
}

const char* xdb_errmsg (xdb_res_t *pRes)
{
	if (NULL == pRes) {
		return "";
	}
	return pRes->row_data ? (const char*)pRes->row_data : "OK";
}

const char* xdb_version ()
{
	return XDB_VERSION;
}
