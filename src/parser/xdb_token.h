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

typedef enum {
	XDB_TOK_ERR = -1, // make enum signed
	XDB_TOK_ID = 0,	// normal char
	XDB_TOK_STR,	// ' " `
	XDB_TOK_HEX,	//
	XDB_TOK_NUM,	// 0-9, 0xA-Z,a-z 
	XDB_TOK_ESC,	/* \ */
	XDB_TOK_SP,		// \t \n \r sp
	XDB_TOK_EQ, 	// =
	XDB_TOK_LT,		// <
	XDB_TOK_LE, 	// <=
	XDB_TOK_GT,		// >
	XDB_TOK_GE, 	// >=
	XDB_TOK_LIKE,
	XDB_TOK_BTWN,
	XDB_TOK_IN,
	XDB_TOK_NE, 	// != <>
	XDB_TOK_ADD, 	// +
	XDB_TOK_SUB, 	// -
	XDB_TOK_MUL, 	// *
	XDB_TOK_DIV, 	// /
	XDB_TOK_MOD, 	// %
	XDB_TOK_BAND, 	// &
	XDB_TOK_BOR,  	// |
	XDB_TOK_BXOR, 	// ^
	XDB_TOK_NOT,   	// ~
	XDB_TOK_NEG,   	// !
	XDB_TOK_DOT, 	// .
	XDB_TOK_COMMA, 	// ,
	XDB_TOK_COLON,	// :
	XDB_TOK_LP,		// ( Parentheses
	XDB_TOK_RP,		// )
	XDB_TOK_LB, 	// { Braces
	XDB_TOK_RB, 	// }
	XDB_TOK_LBK,	// [ Brackets
	XDB_TOK_RBK,	// ] 
	XDB_TOK_QM,		// ?

	XDB_TOK_BIGINT,
	XDB_TOK_DOUBLE,

	// AGG function
	XDB_TOK_COUNT,
	XDB_TOK_SUM,
	XDB_TOK_MIN,
	XDB_TOK_MAX,
	XDB_TOK_AVG,

	XDB_TOK_AND,
	XDB_TOK_OR,

	XDB_TOK_NONE,

	XDB_TOK_INV,	// invalid
	XDB_TOK_END,	// ; Semi-colon
	XDB_TOK_EOF,	// \0
} xdb_token_type;
