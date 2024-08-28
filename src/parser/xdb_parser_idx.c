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

XDB_STATIC int 
xdb_parse_create_idx_def (xdb_conn_t* pConn, xdb_token_t *pTkn, xdb_stmt_idx_t *pStmt)
{
	int type = pTkn->tk_type;
	
	XDB_EXPECT (XDB_TOK_LP==type, XDB_E_STMT, "Index Miss (");

	// col list
	pStmt->fld_count = 0;
	do {
		type = xdb_next_token (pTkn);
		if (XDB_TOK_ID == type) {
			pStmt->idx_col[pStmt->fld_count++] = pTkn->token;
		} else {
			break;
		}
		type = xdb_next_token (pTkn);
	} while (XDB_TOK_COMMA == type);
	
	XDB_EXPECT (XDB_TOK_RP==type, XDB_E_STMT, "Index Miss )");

	type = xdb_next_token (pTkn);
	if (XDB_TOK_ID==type && !strcasecmp (pTkn->token, "XOID")) {
		type = xdb_next_token (pTkn);
		XDB_EXPECT (XDB_TOK_EQ==type, XDB_E_STMT, "Index option Expect EQ");
		type = xdb_next_token (pTkn);
		XDB_EXPECT (XDB_TOK_NUM >= type, XDB_E_STMT, "Index option Expect STRING");
		pStmt->xoid = atoi (pTkn->token);
		type = xdb_next_token (pTkn);
		//xdb_dbglog ("index xoid = %d\n", pStmt->xoid);
	}

	return type;

error:
	return -XDB_E_STMT;
}

XDB_STATIC xdb_stmt_t* 
xdb_parse_create_index (xdb_conn_t* pConn, xdb_token_t *pTkn, bool bUnique)
{
	xdb_token_type type = xdb_next_token (pTkn);
	xdb_stmt_idx_t *pStmt = &pConn->stmt_union.idx_stmt;;
	pStmt->stmt_type = XDB_STMT_CREATE_IDX;
	pStmt->pSql = NULL;

	XDB_EXPECT (XDB_TOK_ID==type, XDB_E_STMT, "Miss Index name");

	pStmt->idx_name	= pTkn->token;
	pStmt->idx_type = XDB_IDX_HASH;
	pStmt->xoid = -1;
	pStmt->bUnique = bUnique;

	type = xdb_next_token (pTkn);

	XDB_EXPECT ((XDB_TOK_ID==type) && !strcasecmp (pTkn->token, "ON"), XDB_E_STMT, "Expect ON");

	type = xdb_next_token (pTkn);

	XDB_EXPECT (XDB_TOK_STR>=type, XDB_E_STMT, "Miss table name");

	XDB_PARSE_DBTBLNAME();

	xdb_idxm_t *pIdxm = xdb_find_index (pStmt->pTblm, pStmt->idx_name);
	XDB_EXPECT (NULL == pIdxm, XDB_E_EXISTS, "Index '%s' already exists", pStmt->idx_name);

#ifndef XDB_DEBUG
	if (pStmt->pTblm->pDbm->bSysDb) {
		XDB_EXPECT (pConn == s_xdb_sysdb_pConn, XDB_E_CONSTRAINT, "Can't create index '%s' on table '%s' in system database", pStmt->idx_name, XDB_OBJ_NAME(pStmt->pTblm));
	}
#endif

	int rc = xdb_parse_create_idx_def (pConn, pTkn, pStmt);
	XDB_EXPECT2 (rc >= XDB_OK);
	
	return (xdb_stmt_t*)pStmt;

error:
	xdb_stmt_free ((xdb_stmt_t*)pStmt);
	return NULL;
}

XDB_STATIC xdb_stmt_t* 
xdb_parse_drop_index (xdb_conn_t* pConn, xdb_token_t *pTkn)
{
	xdb_stmt_idx_t *pStmt = &pConn->stmt_union.idx_stmt;
	pStmt->stmt_type = XDB_STMT_DROP_IDX;
	pStmt->pSql = NULL;

	xdb_token_type type = xdb_next_token (pTkn);
	XDB_EXPECT (type == XDB_TOK_ID, XDB_E_STMT, "Miss INDEX name: "XDB_SQL_DROP_IDX_STMT);

	pStmt->idx_name	= pTkn->token;

	type = xdb_next_token (pTkn);
	XDB_EXPECT ((XDB_TOK_ID == type) && !strcasecmp (pTkn->token, "ON"), XDB_E_STMT, "Expect ON: "XDB_SQL_DROP_IDX_STMT);

	type = xdb_next_token (pTkn);
	XDB_EXPECT (type <= XDB_TOK_STR, XDB_E_STMT, "Miss table name: "XDB_SQL_DROP_IDX_STMT);

	XDB_PARSE_DBTBLNAME();

	pStmt->pIdxm = xdb_find_index (pStmt->pTblm, pStmt->idx_name);
	XDB_EXPECT (NULL!=pStmt->pIdxm, XDB_E_NOTFOUND, "Index '%s' doesn't exist", pStmt->idx_name);

	if ((NULL != pStmt->pIdxm) && pStmt->pIdxm->pTblm->pDbm->bSysDb) {
		XDB_EXPECT (pConn == s_xdb_sysdb_pConn, XDB_E_CONSTRAINT, "Can't drop table '%s' index '%s' in system database", XDB_OBJ_NAME(pStmt->pIdxm->pTblm), XDB_OBJ_NAME(pStmt->pIdxm));
	}

	return (xdb_stmt_t*)pStmt;
error:
	xdb_stmt_free ((xdb_stmt_t*)pStmt);
	return NULL;
}
