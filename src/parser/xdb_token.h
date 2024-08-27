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

typedef enum {
	XDB_TOK_ERR = -1, // make enum signed
	XDB_TOK_ID  = 0,	// normal char
	XDB_TOK_STR,	// ' " `
	XDB_TOK_NUM,	// 0-9, 0xA-Z,a-z 
	XDB_TOK_ESC,	/* \ */
	XDB_TOK_SP,		// \t \n \r sp
	XDB_TOK_EQ, 	// =
	XDB_TOK_LT,		// <
	XDB_TOK_LE, 	// <=
	XDB_TOK_GT,		// >
	XDB_TOK_GE, 	// >=
	XDB_TOK_NE, 	// != <>
	XDB_TOK_ADD, 	// +
	XDB_TOK_SUB, 	// -
	XDB_TOK_MUL, 	// *
	XDB_TOK_DIV, 	// /
	XDB_TOK_MOD, 	// %
	XDB_TOK_AND, 	// &
	XDB_TOK_OR,  	// |
	XDB_TOK_XOR, 	// ^
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
	XDB_TOK_INV,	// invalid
	XDB_TOK_END,	// ; Semi-colon
	XDB_TOK_EOF,	// \0
} xdb_token_type;
