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

XDB_STATIC xdb_stmt_t* 
xdb_parse_create_pub (xdb_conn_t* pConn, xdb_token_t *pTkn)
{
	xdb_stmt_pub_t *pStmt = &pConn->stmt_union.pub_stmt;
	memset (pStmt, 0, sizeof (*pStmt));
	pStmt->stmt_type = XDB_STMT_CREATE_PUB;

	xdb_token_type type = xdb_next_token (pTkn);
	XDB_EXPECT (XDB_TOK_STR>=type, XDB_E_STMT, "Miss publication name");

	if ((XDB_TOK_ID==type) && !strcasecmp (pTkn->token, "IF")) {
		type = xdb_next_token (pTkn);
		XDB_EXPECT ((XDB_TOK_ID==type) && !strcasecmp (pTkn->token, "NOT"), XDB_E_STMT, "Miss NOT");
		type = xdb_next_token (pTkn);
		XDB_EXPECT ((XDB_TOK_ID==type) && !strcasecmp (pTkn->token, "EXISTS"), XDB_E_STMT, "Miss EXISTS");
		type = xdb_next_token (pTkn);
		XDB_EXPECT (XDB_TOK_ID==type, XDB_E_STMT, "Miss publication name");
		pStmt->bIfExistOrNot = true;
	}
	pStmt->pub_name	= pTkn->token;

	return (xdb_stmt_t*)pStmt;

error:
	xdb_stmt_free ((xdb_stmt_t*)pStmt);
	return NULL;
}

XDB_STATIC xdb_stmt_t* 
xdb_parse_create_sub (xdb_conn_t* pConn, xdb_token_t *pTkn)
{
	xdb_stmt_sub_t *pStmt = &pConn->stmt_union.sub_stmt;
	memset (pStmt, 0, sizeof (*pStmt));
	pStmt->stmt_type = XDB_STMT_CREATE_SUB;

	xdb_token_type type = xdb_next_token (pTkn);
	XDB_EXPECT (XDB_TOK_STR>=type, XDB_E_STMT, "Miss subscription name");

	if ((XDB_TOK_ID==type) && !strcasecmp (pTkn->token, "IF")) {
		type = xdb_next_token (pTkn);
		XDB_EXPECT ((XDB_TOK_ID==type) && !strcasecmp (pTkn->token, "NOT"), XDB_E_STMT, "Miss NOT");
		type = xdb_next_token (pTkn);
		XDB_EXPECT ((XDB_TOK_ID==type) && !strcasecmp (pTkn->token, "EXISTS"), XDB_E_STMT, "Miss EXISTS");
		type = xdb_next_token (pTkn);
		XDB_EXPECT (XDB_TOK_ID==type, XDB_E_STMT, "Miss subscription name");
		pStmt->bIfExistOrNot = true;
	}
	pStmt->sub_name	= pTkn->token;
	pStmt->pub_port = XDB_SVR_PORT;

	do {
		type = xdb_next_token (pTkn);
		if (XDB_TOK_END <= type) {
			break;
		}
		XDB_EXPECT (XDB_TOK_ID==type, XDB_E_STMT, "Expect ID");

		const char *var = pTkn->token;
		type = xdb_next_token (pTkn);
		XDB_EXPECT (XDB_TOK_EQ==type, XDB_E_STMT, "Expect EQ");
		type = xdb_next_token (pTkn);
	
		if (!strcasecmp (var, "PUBLICATION")) {
			XDB_EXPECT (XDB_TOK_ID == type, XDB_E_STMT, "Expect publication name");
			pStmt->pub_name = pTkn->token;				
		} else if (!strcasecmp (var, "TABLES")) {
			XDB_EXPECT (XDB_TOK_STR == type, XDB_E_STMT, "Expect TABLES");
			pStmt->tables = pTkn->token;
		} else if (!strcasecmp (var, "HOST")) {
			XDB_EXPECT (XDB_TOK_STR == type, XDB_E_STMT, "Expect HOST");
			pStmt->pub_host = pTkn->token;
		} else if (!strcasecmp (var, "PORT")) {
			XDB_EXPECT (XDB_TOK_NUM == type, XDB_E_STMT, "Expect number");
			pStmt->pub_port = atoi (pTkn->token);
		}
		type = xdb_next_token (pTkn);
	} while (XDB_TOK_COMMA == type);

	XDB_EXPECT (pStmt->pub_name != NULL, XDB_E_STMT, "Expect publication name");
	XDB_EXPECT (pStmt->pub_host != NULL, XDB_E_STMT, "Expect HOST");

	return (xdb_stmt_t*)pStmt;

error:
	xdb_stmt_free ((xdb_stmt_t*)pStmt);
	return NULL;
}

#if 0
XDB_STATIC xdb_stmt_t* 
xdb_parse_drop_pub (xdb_conn_t* pConn, xdb_token_t *pTkn)
{
	xdb_stmt_svr_t *pStmt = &pConn->stmt_union.svr_stmt;
	memset (pStmt, 0, sizeof (*pStmt));
	pStmt->stmt_type = XDB_STMT_DROP_SVR;

	xdb_token_type type = xdb_next_token (pTkn);
	XDB_EXPECT (XDB_TOK_STR>=type, XDB_E_STMT, "Miss server name");
	pStmt->svr_name	= pTkn->token;

	return (xdb_stmt_t*)pStmt;

error:
	xdb_stmt_free ((xdb_stmt_t*)pStmt);
	return NULL;
}
#endif

XDB_STATIC xdb_stmt_t* 
xdb_parse_subscribe (xdb_conn_t* pConn, xdb_token_t *pTkn)
{
	xdb_stmt_subscribe_t *pStmt = &pConn->stmt_union.subscribe_stmt;
	memset (pStmt, 0, sizeof (*pStmt));
	pStmt->stmt_type = XDB_STMT_SUBSCRIBE;

	xdb_token_type type = xdb_next_token (pTkn);
	XDB_EXPECT (XDB_TOK_STR>=type, XDB_E_STMT, "Miss publication name");

	pStmt->pub_name	= pTkn->token;

	do {
		type = xdb_next_token (pTkn);
		if (XDB_TOK_END <= type) {
			break;
		}
		XDB_EXPECT (XDB_TOK_ID==type, XDB_E_STMT, "Expect ID");
		const char *var = pTkn->token;
		type = xdb_next_token (pTkn);
		XDB_EXPECT (XDB_TOK_EQ==type, XDB_E_STMT, "Expect EQ");
		type = xdb_next_token (pTkn);

		if (0 == strcasecmp (var, "CLIENT")) {
			XDB_EXPECT (XDB_TOK_STR==type, XDB_E_STMT, "Miss Client Name");
			pStmt->sub_name = pTkn->token;
		} else if (0 == strcasecmp (var, "TABLES")) {
			pStmt->tables = pTkn->token;
		} else if (0 == strcasecmp (var, "CREATE")) {
			pStmt->bCreate = atoi (pTkn->token);
		}
		type = xdb_next_token (pTkn);
	} while (XDB_TOK_COMMA == type);

	return (xdb_stmt_t*)pStmt;

error:
	xdb_stmt_free ((xdb_stmt_t*)pStmt);
	return NULL;
}

