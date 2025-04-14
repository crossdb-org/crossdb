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
xdb_parse_create_trigger (xdb_conn_t* pConn, xdb_token_t *pTkn)
{
	xdb_token_type type = xdb_next_token (pTkn);
	xdb_stmt_trig_t *pStmt = &pConn->stmt_union.trig_stmt;
	pStmt->stmt_type = XDB_STMT_CREATE_TRIG;
	pStmt->pSql = NULL;

	XDB_EXPECT (XDB_TOK_ID==type, XDB_E_STMT, "Miss Trigger name");

	pStmt->trig_name	= pTkn->token;
	bool	bAft = false;

	type = xdb_next_token (pTkn);
	XDB_EXPECT ((XDB_TOK_ID==type), XDB_E_STMT, "Expect { BEFORE | AFTER }");
	if (!strcasecmp (pTkn->token, "AFTER")) {
		bAft = true;
	} else {
		XDB_EXPECT (!strcasecmp (pTkn->token, "BEFORE"), XDB_E_STMT, "Expect BEFORE or AFTER");
	}
	type = xdb_next_token (pTkn);
	XDB_EXPECT ((XDB_TOK_ID==type), XDB_E_STMT, "Expect { INSERT | UPDATE | DELETE }");
	if (!strcasecmp (pTkn->token, "INSERT")) {
		pStmt->trig_type = XDB_TRIG_BEF_INS + bAft;
	} else if (!strcasecmp (pTkn->token, "UPDATE")) {
		pStmt->trig_type = XDB_TRIG_BEF_UPD + bAft;
	} else if (!strcasecmp (pTkn->token, "DELETE")) {
		pStmt->trig_type = XDB_TRIG_BEF_DEL + bAft;
	} else {
		XDB_EXPECT (0, XDB_E_STMT, "Expect { INSERT | UPDATE | DELETE }");
	}

	type = xdb_next_token (pTkn);
	XDB_EXPECT ((XDB_TOK_ID==type) && !strcasecmp (pTkn->token, "ON"), XDB_E_STMT, "Expect ON");

	type = xdb_next_token (pTkn);
	XDB_EXPECT (XDB_TOK_STR>=type, XDB_E_STMT, "Miss table name");
	XDB_PARSE_DBTBLNAME();

	XDB_EXPECT ((XDB_TOK_ID==type) && !strcasecmp (pTkn->token, "CALL"), XDB_E_STMT, "Expect CALL");

	type = xdb_next_token (pTkn);
	XDB_EXPECT (XDB_TOK_STR>=type, XDB_E_STMT, "Miss func name");
	pStmt->func_name = pTkn->token;
	
	return (xdb_stmt_t*)pStmt;

error:
	xdb_stmt_free ((xdb_stmt_t*)pStmt);
	return NULL;
}
