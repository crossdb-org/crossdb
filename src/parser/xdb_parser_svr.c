/******************************************************************************
* Copyright (c) 2024-present JC Wang. All rights reserved
*
*   https://crossdb.org
*   https://github.com/crossdb-org/crossdb
*
* This program is free software: you can redistribute it and/or modify it under
* the terms of the GNU General Public License, version 3 or later, as published 
* by the Free Software Foundation.
*
* This program is distributed in the hope that it will be useful, but WITHOUT
* ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS 
* FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License along with 
* this program. If not, see <http://www.gnu.org/licenses/>.
******************************************************************************/

XDB_STATIC xdb_stmt_t* 
xdb_parse_create_server (xdb_conn_t* pConn, xdb_token_t *pTkn)
{
	xdb_stmt_svr_t *pStmt = &pConn->stmt_union.svr_stmt;
	pStmt->stmt_type = XDB_STMT_CREATE_SVR;

	xdb_token_type type = xdb_next_token (pTkn);
	XDB_EXPECT (XDB_TOK_STR>=type, XDB_E_STMT, "Miss server name");

	if ((XDB_TOK_ID==type) && !strcasecmp (pTkn->token, "IF")) {
		type = xdb_next_token (pTkn);
		XDB_EXPECT ((XDB_TOK_ID==type) && !strcasecmp (pTkn->token, "NOT"), XDB_E_STMT, "Miss NOT");
		type = xdb_next_token (pTkn);
		XDB_EXPECT ((XDB_TOK_ID==type) && !strcasecmp (pTkn->token, "EXISTS"), XDB_E_STMT, "Miss EXISTS");
		type = xdb_next_token (pTkn);
		XDB_EXPECT (XDB_TOK_ID==type, XDB_E_STMT, "Miss server name");
		pStmt->bIfExistOrNot = true;
	}
	pStmt->svr_name	= pTkn->token;
	pStmt->svr_port = XDB_SVR_PORT;
	type = xdb_next_token (pTkn);
	if (XDB_TOK_ID == type) {
		if (0 == strcasecmp (pTkn->token, "PORT")) {
			type = xdb_next_token (pTkn);
			XDB_EXPECT (XDB_TOK_EQ==type, XDB_E_STMT, "Miss =");
			type = xdb_next_token (pTkn);
			XDB_EXPECT (XDB_TOK_NUM==type, XDB_E_STMT, "Miss Port");
			pStmt->svr_port = atoi (pTkn->token);
			XDB_EXPECT (pStmt->svr_port>0 && pStmt->svr_port<65536, XDB_E_STMT, "Wrong Port number should be in range (0, 65536)");
		}
	}

	return (xdb_stmt_t*)pStmt;

error:
	xdb_stmt_free ((xdb_stmt_t*)pStmt);
	return NULL;
}

XDB_STATIC xdb_stmt_t* 
xdb_parse_drop_server (xdb_conn_t* pConn, xdb_token_t *pTkn)
{
	xdb_stmt_svr_t *pStmt = &pConn->stmt_union.svr_stmt;
	pStmt->stmt_type = XDB_STMT_DROP_SVR;

	xdb_token_type type = xdb_next_token (pTkn);
	XDB_EXPECT (XDB_TOK_STR>=type, XDB_E_STMT, "Miss server name");

	return (xdb_stmt_t*)pStmt;

error:
	xdb_stmt_free ((xdb_stmt_t*)pStmt);
	return NULL;
}

