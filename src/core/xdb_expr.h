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

#if 0
typedef enum XDB_TOK_t {
	XDB_TOK_NONE,

	XDB_TOK_OR,
	XDB_TOK_AND,

	XDB_TOK_EQ 	= XDB_TOK_EQ,	// = 6
	XDB_TOK_LT 	= XDB_TOK_LT, 	// <
	XDB_TOK_LE 	= XDB_TOK_LE, 	// <=
	XDB_TOK_GT 	= XDB_TOK_GT, 	// >
	XDB_TOK_GE 	= XDB_TOK_GE, 	// >=
	XDB_TOK_NE 	= XDB_TOK_NE, 	// != <>
	XDB_OPLOG_MAX	= XDB_TOK_NE,

	XDB_TOK_ADD 	= XDB_TOK_ADD,
	XDB_TOK_SUB 	= XDB_TOK_SUB,
	XDB_TOK_MUL	= XDB_TOK_MUL,
	XDB_TOK_DIV	= XDB_TOK_DIV,

	XDB_TOK_BIGINT,
	XDB_TOK_DOUBLE,
	XDB_TOK_CHAR,

	XDB_TOK_COUNT,
	XDB_TOK_SUM,
	XDB_TOK_MIN,
	XDB_TOK_MAX,
	XDB_TOK_AVG,
} XDB_TOK_t;
#endif

#define XDB_TYPE_FIELD 128

typedef struct xdb_value_t {
	uint8_t			val_type;	// xdb_type_t int, uint, double, string, binary, field, function | =, !=, >, <, >=, <=, AND, OR, + - * / 
	uint8_t			fld_type;
	uint8_t			sup_type;
	xdb_field_t 	*pField;
	char			*pExtract;
	uint8_t			reftbl_id;
	xdb_str_t		val_str;	// raw text value field
	xdb_str_t		val_str2;	// raw text value	table
	union {
		double		fval;
		uint64_t	uval;
		int64_t		ival;
		xdb_str_t	str;	// str/binary
		xdb_inet_t	inet;
		xdb_mac_t	mac;
	};
	void		*pExpr;
} xdb_value_t;

typedef struct xdb_expr_t {
	xdb_value_t		val[XDB_MAX_MATCH_COL];
} xdb_expr_t;
