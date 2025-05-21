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

#ifndef __CROSS_DB_H__
#define __CROSS_DB_H__

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <inttypes.h>

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
	CrossDB Error
******************************************************************************/

typedef enum {
	XDB_OK			= 0,
	XDB_ERROR		= 1,
	XDB_E_PARAM		= 2,
	XDB_E_STMT		= 3,
	XDB_E_NODB		= 4,
	XDB_E_NOTFOUND	= 5,
	XDB_E_EXISTS	= 6,
	XDB_E_FULL		= 7,
	XDB_E_CONSTRAINT= 8,
	XDB_E_AUTH		= 9,
	XDB_E_MEMORY	= 10,
	XDB_E_FILE		= 11,
	XDB_E_SOCK		= 12,
} xdb_errno_e;

// return - value of xdb_errno_e
typedef int xdb_ret;


/******************************************************************************
	CrossDB Data Types
******************************************************************************/

typedef enum {
	XDB_TYPE_NULL       = 0,  // 1 bit
	XDB_TYPE_TINYINT	= 1,  // 1 byte
	XDB_TYPE_SMALLINT   = 2,  // 2 bytes
	XDB_TYPE_INT        = 3,  // 4 bytes
	XDB_TYPE_BIGINT     = 4,  // 8 bytes
	XDB_TYPE_UTINYINT   = 5,  // 1 byte
	XDB_TYPE_USMALLINT  = 6,  // 2 bytes
	XDB_TYPE_UINT       = 7,  // 4 bytes
	XDB_TYPE_UBIGINT    = 8,  // 8 bytes
	XDB_TYPE_FLOAT      = 9,  // 4 bytes
	XDB_TYPE_DOUBLE     = 10, // 8 bytes
	XDB_TYPE_TIMESTAMP	= 11, // 8 bytes
	XDB_TYPE_CHAR       = 12, // fixed-length string(at most 65535 byte)
	XDB_TYPE_BINARY     = 13, // fixed-length binary(at most 65535 byte)
	XDB_TYPE_VCHAR      = 14, // varied-length string(at most 65535 byte)
	XDB_TYPE_VBINARY    = 15, // varied-length binary(at most 65535 byte)
	XDB_TYPE_BOOL		= 16, // 1 byte
	XDB_TYPE_INET		= 17, // 18 bytes
	XDB_TYPE_MAC		= 18, // 6 bytes
	XDB_TYPE_JSON		= 19, // varied-length binary(at most 65535 byte)
	XDB_TYPE_ARRAY		= 20, // array(TDB)
	XDB_TYPE_MAX        = 32
} xdb_type_t;

typedef struct {
	uint8_t		family;	// 4=ipv4, 6=ipv6 
	uint8_t		mask;
	uint8_t		addr[16];
} xdb_inet_t;

typedef struct {
	uint8_t		addr[6];
} xdb_mac_t;

typedef int	xdb_rowid;


/******************************************************************************
	CrossDB Reuslt
******************************************************************************/

typedef void xdb_row_t;

typedef struct xdb_res_t xdb_res_t;

typedef struct xdb_conn_t xdb_conn_t;

typedef struct xdb_stmt_t xdb_stmt_t;


/**************************************
 Connection
***************************************/

xdb_conn_t*
xdb_open (const char *path);

xdb_conn_t*
xdb_connect (const char *host, const char *user, const char *pass, const char *db, uint16_t port);

void
xdb_close (xdb_conn_t *pConn);

const char*
xdb_curdb (xdb_conn_t* pConn);


/**************************************
 SQL
***************************************/

xdb_res_t*
xdb_exec (xdb_conn_t *pConn, const char *sql);

xdb_res_t*
xdb_exec2 (xdb_conn_t *pConn, const char *sql, int len);

xdb_res_t*
xdb_bexec (xdb_conn_t *pConn, const char *sql, ...);

xdb_res_t*
xdb_vbexec (xdb_conn_t *pConn, const char *sql, va_list ap);

xdb_res_t*
xdb_pexec (xdb_conn_t *pConn, const char *sql, ...);

xdb_res_t*
xdb_next_result (xdb_conn_t *pConn);

bool
xdb_more_result (xdb_conn_t *pConn);

void
xdb_free_result (xdb_res_t *pRes);

typedef int (*xdb_row_callback) (xdb_res_t* pRes, xdb_row_t *pRow, void *pArg);

#if 0
xdb_res_t*
xdb_bexec_cb (xdb_conn_t *pConn, xdb_row_callback callback, void *pArg, const char *sql, ...);

xdb_res_t*
xdb_vbexec_cb (xdb_conn_t *pConn, xdb_row_callback callback, void *pArg, const char *sql, va_list ap);
#endif

const void * 
xdb_poll (xdb_conn_t *pConn, int *pLen, uint32_t timeout);


/**************************************
 Result
***************************************/

xdb_errno_e
xdb_errcode (xdb_res_t *pRes);

const char*
xdb_errmsg (xdb_res_t *pRes);

int
xdb_column_count (xdb_res_t *pRes);

xdb_type_t 
xdb_column_type (xdb_res_t *pRes, uint16_t iCol);

const char* 
xdb_column_name (xdb_res_t *pRes, uint16_t iCol);

const char* 
xdb_table_name (xdb_res_t *pRes, uint16_t id);

int 
xdb_column_id (xdb_res_t *pRes, const char *name);

xdb_row_t*
xdb_fetch_row (xdb_res_t *pRes);

xdb_rowid
xdb_row_count (xdb_res_t *pRes);

xdb_rowid
xdb_affected_rows (xdb_res_t *pRes);

bool 
xdb_column_null (xdb_res_t *pRes, xdb_row_t *pRow, uint16_t iCol);

bool 
xdb_column_bool (xdb_res_t *pRes, xdb_row_t *pRow, uint16_t iCol);

int 
xdb_column_int (xdb_res_t *pRes, xdb_row_t *pRow, uint16_t iCol);

uint32_t 
xdb_column_uint (xdb_res_t *pRes, xdb_row_t *pRow, uint16_t iCol);

int64_t 
xdb_column_int64 (xdb_res_t *pRes, xdb_row_t *pRow, uint16_t iCol);

uint64_t 
xdb_column_uint64 (xdb_res_t *pRes, xdb_row_t *pRow, uint16_t iCol);

float
xdb_column_float (xdb_res_t *pRes, xdb_row_t *pRow, uint16_t iCol);

double
xdb_column_double (xdb_res_t *pRes, xdb_row_t *pRow, uint16_t iCol);

const char*
xdb_column_str (xdb_res_t *pRes, xdb_row_t *pRow, uint16_t iCol);

const char*
xdb_column_str2 (xdb_res_t *pRes, xdb_row_t *pRow, uint16_t iCol, int *pLen);

const void*
xdb_column_blob (xdb_res_t *pRes, xdb_row_t *pRow, uint16_t iCol, int *pLen);

const xdb_mac_t*
xdb_column_mac (xdb_res_t *pRes, xdb_row_t *pRow, uint16_t iCol);

const xdb_inet_t*
xdb_column_inet (xdb_res_t *pRes, xdb_row_t *pRow, uint16_t iCol);

bool 
xdb_col_null (xdb_res_t *pRes, xdb_row_t *pRow, const char *name);

bool 
xdb_col_bool (xdb_res_t *pRes, xdb_row_t *pRow, const char *name);

int 
xdb_col_int (xdb_res_t *pRes, xdb_row_t *pRow, const char *name);

int64_t 
xdb_col_int64 (xdb_res_t *pRes, xdb_row_t *pRow, const char *name);

float
xdb_col_float (xdb_res_t *pRes, xdb_row_t *pRow, const char *name);

double
xdb_col_double (xdb_res_t *pRes, xdb_row_t *pRow, const char *name);

const char*
xdb_col_str (xdb_res_t *pRes, xdb_row_t *pRow, const char *name);

const char*
xdb_col_str2 (xdb_res_t *pRes, xdb_row_t *pRow, const char *name, int *pLen);

const void*
xdb_col_blob (xdb_res_t *pRes, xdb_row_t *pRow, const char *name, int *pLen);

const xdb_mac_t*
xdb_col_mac (xdb_res_t *pRes, xdb_row_t *pRow, const char *name);

const xdb_inet_t*
xdb_col_inet (xdb_res_t *pRes, xdb_row_t *pRow, const char *name);


/**************************************
 Prepared Statment
***************************************/

xdb_stmt_t*
xdb_stmt_prepare (xdb_conn_t *pConn, const char *sql);

xdb_ret
xdb_bind_int (xdb_stmt_t *pStmt, uint16_t para_id, int val);

xdb_ret
xdb_bind_int64 (xdb_stmt_t *pStmt, uint16_t para_id, int64_t val);

xdb_ret
xdb_bind_float (xdb_stmt_t *pStmt, uint16_t para_id, float val);

xdb_ret
xdb_bind_double (xdb_stmt_t *pStmt, uint16_t para_id, double val);

xdb_ret
xdb_bind_str (xdb_stmt_t *pStmt, uint16_t para_id, const char *str);

xdb_ret
xdb_bind_str2 (xdb_stmt_t *pStmt, uint16_t para_id, const char *str, int len);

xdb_ret
xdb_clear_bindings (xdb_stmt_t *pStmt);

xdb_res_t*
xdb_stmt_exec (xdb_stmt_t *pStmt);

xdb_res_t*
xdb_stmt_bexec (xdb_stmt_t *pStmt, ...);

xdb_res_t*
xdb_stmt_vbexec (xdb_stmt_t *pStmt, va_list ap);

void
xdb_stmt_close (xdb_stmt_t *pStmt);

#if 0
xdb_res_t*
xdb_stmt_exec_cb (xdb_stmt_t *pStmt, xdb_row_callback callback, void *pArg);

xdb_res_t*
xdb_stmt_bexec_cb (xdb_stmt_t *pStmt, xdb_row_callback callback, void *pArg, ...);

xdb_res_t*
xdb_stmt_vbexec_cb (xdb_stmt_t *pStmt, xdb_row_callback callback, void *pArg, va_list ap);
#endif


/**************************************
 Transaction
***************************************/

xdb_ret
xdb_begin (xdb_conn_t* pConn);

xdb_ret
xdb_commit (xdb_conn_t* pConn);

xdb_ret
xdb_rollback (xdb_conn_t* pConn);


/**************************************
 Trigger
***************************************/

typedef enum {
	XDB_TRIG_BEF_INS,
	XDB_TRIG_AFT_INS,
	XDB_TRIG_BEF_UPD,
	XDB_TRIG_AFT_UPD,
	XDB_TRIG_BEF_DEL,
	XDB_TRIG_AFT_DEL,
	XDB_TRIG_MAX,
} xdb_trig_e;

typedef int (*xdb_trig_callback) (xdb_conn_t *pConn, xdb_res_t *pRes, xdb_trig_e type, xdb_row_t *pNewRow, xdb_row_t *pOldRow, void *pArg);

typedef enum {
	XDB_FUNC_TRIG,
	XDB_FUNC_MAX,
} xdb_func_e;

xdb_ret 
xdb_create_func (const char *name, xdb_func_e type, const char *lang, void *cb_func, void *pArg);


/**************************************
 Types Utilis
***************************************/

int 
xdb_inet_sprintf (const xdb_inet_t *pInet, char *buf, int size);

bool 
xdb_inet_scanf (xdb_inet_t *pInet, const char *addr);

int 
xdb_mac_sprintf (const xdb_mac_t *pMac, char *buf, int size);

bool 
xdb_mac_scanf (xdb_mac_t *pMac, const char *addr);

int 
xdb_timestamp_sprintf (uint64_t timestamp, char *buf, int size);

int64_t 
xdb_timestamp_scanf (const char *time_str);


/**************************************
 Misc
***************************************/

#if defined(__GNUC__) || defined(__INTEL_COMPILER) || defined(__clang__)
#define xdb_likely(x)			__builtin_expect(x, 1)
#define xdb_unlikely(x)			__builtin_expect(x, 0)
#else
#define xdb_likely(x)			(x)
#define xdb_unlikely(x)			(x)
#endif

#define XDB_CHECK(expr, ...)	if (xdb_unlikely(!(expr))) { __VA_ARGS__; }
#define XDB_RESCHK(pRes, ...)	if (xdb_unlikely(xdb_errcode(pRes))) { fprintf (stderr, "==== ERROR %d: %s\n", xdb_errcode(pRes), xdb_errmsg(pRes)); __VA_ARGS__; }

const char*
xdb_type2str (xdb_type_t type);

int 
xdb_print_row (xdb_res_t *pRes, xdb_row_t *pRow, int format);

const char*
xdb_version();

#ifdef __cplusplus
}
#endif

#endif // __CROSS_DB_H__
