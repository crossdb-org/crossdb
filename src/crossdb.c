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
#include "core/xdb_db.h"
#include "core/xdb_crud.h"
#include "core/xdb_hash.h"
#include "core/xdb_sql.h"
#include "core/xdb_sysdb.h"
#include "core/xdb_vdata.h"
#include "core/xdb_table.h"
#include "core/xdb_index.h"
#include "core/xdb_trans.h"
#if (XDB_ENABLE_SERVER == 1)
#include "server/xdb_server.h"
#endif
#include "core/xdb_conn.h"
#include "admin/xdb_shell.h"
#include "admin/xdb_backup.h"
#if (XDB_ENABLE_WAL == 1)
#include "core/xdb_wal.h"
#endif

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
#include "core/xdb_conn.c"
#if (XDB_ENABLE_SERVER == 1)
#include "server/xdb_client.c"
#include "server/xdb_server.c"
#endif
#include "core/xdb_sql.c"
#if (XDB_ENABLE_WAL == 1)
#include "core/xdb_wal.c"
#endif
#if (XDB_ENABLE_JNI == 1)
#include "jni/xdb_jni.c"
#endif
#include "admin/xdb_shell.c"
#include "admin/xdb_backup.c"

const char* xdb_type2str(xdb_type_t tp) 
{
	const char *id2str[] = {
		[XDB_TYPE_NULL     ] = "NULL",
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
	};
	return tp <= XDB_ARY_LEN(id2str) ? id2str[tp] : "Unkonwn";
}

static xdb_type_t s_xdb_prompt_type[] = {
	[XDB_TYPE_TINYINT  ] 	= XDB_TYPE_BIGINT,
	[XDB_TYPE_SMALLINT  ] 	= XDB_TYPE_BIGINT,
	[XDB_TYPE_INT	] 		= XDB_TYPE_BIGINT,
	[XDB_TYPE_BIGINT	]	= XDB_TYPE_BIGINT,
	[XDB_TYPE_FLOAT	]		= XDB_TYPE_DOUBLE,
	[XDB_TYPE_DOUBLE	]	= XDB_TYPE_DOUBLE,
	[XDB_TYPE_CHAR	]		= XDB_TYPE_CHAR,
	[XDB_TYPE_VCHAR	]		= XDB_TYPE_VCHAR,
	[XDB_TYPE_BINARY ]		= XDB_TYPE_BINARY,
	[XDB_TYPE_VBINARY ]		= XDB_TYPE_VBINARY
};

static bool s_xdb_bInit = false;

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
	return XDB_OK;
}

int 
xdb_exit ()
{
	// Close all opened DBs
	s_xdb_bInit = false;
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
