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
xdb_parse_create_db (xdb_conn_t* pConn, xdb_token_t *pTkn)
{
	xdb_stmt_db_t *pStmt = &pConn->stmt_union.db_stmt;
	pStmt->stmt_type = XDB_STMT_CREATE_DB;
	pStmt->pSql = NULL;
	xdb_token_type type = xdb_next_token (pTkn);

	XDB_EXPECT (type<=XDB_TOK_STR, XDB_E_STMT, "Miss database name");
	pStmt->lock_mode = XDB_LOCK_THREAD;
	pStmt->bMemory = false;

	if ((XDB_TOK_ID==type) && !strcasecmp (pTkn->token, "IF")) {
		type = xdb_next_token (pTkn);
		XDB_EXPECT ((XDB_TOK_ID==type) && !strcasecmp (pTkn->token, "NOT"), XDB_E_STMT, "Miss NOT");
		type = xdb_next_token (pTkn);
		XDB_EXPECT ((XDB_TOK_ID==type) && !strcasecmp (pTkn->token, "EXISTS"), XDB_E_STMT, "Miss EXISTS");
		type = xdb_next_token (pTkn);
		XDB_EXPECT (type<=XDB_TOK_STR, XDB_E_STMT, "Create DB Miss database name");
		pStmt->bIfExistOrNot = true;
	}
	pStmt->db_name	= pTkn->token;

	pStmt->pDbm = xdb_find_db (pStmt->db_name);
	XDB_EXPECT (pStmt->bIfExistOrNot || (NULL == pStmt->pDbm), XDB_E_EXISTS, "Database '%s' already exists", pStmt->db_name);

	// parse db options
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
	
		if (!strcasecmp (var, "LOCKMODE")) {
			XDB_EXPECT (XDB_TOK_ID == type, XDB_E_STMT, "Expect ID");
			if (!strcasecmp (pTkn->token, "THREAD")) {
				pStmt->lock_mode = XDB_LOCK_THREAD;
			} else if (!strcasecmp (pTkn->token, "PROCESS")) {
				pStmt->lock_mode = XDB_LOCK_PROCESS;
			} else if (!strcasecmp (pTkn->token, "NOLOCK")) {
				pStmt->lock_mode = XDB_LOCK_NOLOCK;
			} else {
				XDB_EXPECT (0, XDB_E_STMT, "Unknown lockmode '%s'", pTkn->token);
			}
		} else if (!strcasecmp (var, "ENGINE")) {
			XDB_EXPECT ((XDB_TOK_ID == type) && !strcasecmp (pTkn->token, "MEMORY"), XDB_E_STMT, "Expect MEMORY");
			pStmt->bMemory = true;
		}
		type = xdb_next_token (pTkn);
	} while (XDB_TOK_COMMA == type);

	if (pStmt->bMemory && (XDB_LOCK_PROCESS == pStmt->lock_mode)) {
		pStmt->lock_mode = XDB_LOCK_THREAD;
	}

	return (xdb_stmt_t*)pStmt;

error:
	xdb_stmt_free ((xdb_stmt_t*)pStmt);
	return NULL;
}

XDB_STATIC xdb_stmt_t* 
xdb_parse_drop_db (xdb_conn_t* pConn, xdb_token_t *pTkn)
{
	xdb_stmt_db_t *pStmt = &pConn->stmt_union.db_stmt;
	xdb_token_type type = xdb_next_token (pTkn);
	pStmt->stmt_type = XDB_STMT_DROP_DB;
	pStmt->pSql = NULL;

	XDB_EXPECT (type<=XDB_TOK_STR, XDB_E_STMT, "Miss database name");

	pStmt->bIfExistOrNot = false;

	if ((XDB_TOK_ID==type) && !strcasecmp (pTkn->token, "IF")) {
		type = xdb_next_token (pTkn);
		XDB_EXPECT ((XDB_TOK_ID==type) && !strcasecmp (pTkn->token, "EXISTS"), XDB_E_STMT, "Miss EXISTS: "XDB_SQL_DROP_TBL_STMT);
		type = xdb_next_token (pTkn);
		XDB_EXPECT (type<=XDB_TOK_STR, XDB_E_STMT, "Miss DB name: "XDB_SQL_DROP_TBL_STMT);
		pStmt->bIfExistOrNot = true;
	}

	pStmt->db_name	= pTkn->token;

	pStmt->pDbm = xdb_find_db (pStmt->db_name);

	XDB_EXPECT (pStmt->bIfExistOrNot || (NULL != pStmt->pDbm), XDB_E_NOTFOUND, "Can't drop database '%s', database doesn't exists", pStmt->db_name);

	if ((NULL != pStmt->pDbm) && pStmt->pDbm->bSysDb) {
		XDB_EXPECT (pConn == s_xdb_sysdb_pConn, XDB_E_CONSTRAINT, "Can't drop system database");
	}

	return (xdb_stmt_t*)pStmt;

error:
	xdb_stmt_free ((xdb_stmt_t*)pStmt);
	return NULL;

}

XDB_STATIC xdb_stmt_t* 
xdb_parse_open_datadir (xdb_conn_t* pConn, xdb_token_t *pTkn)
{
	xdb_stmt_db_t *pStmt = &pConn->stmt_union.db_stmt;
	xdb_token_type type = xdb_next_token (pTkn);
	pStmt->stmt_type = XDB_STMT_OPEN_DATADIR;
	pStmt->pSql = NULL;

	XDB_EXPECT (XDB_TOK_STR==type, XDB_E_STMT, "Miss datadir");

	pStmt->db_name	= pTkn->token;
	return (xdb_stmt_t*)pStmt;

error:
	xdb_stmt_free ((xdb_stmt_t*)pStmt);
	return NULL;
}

XDB_STATIC xdb_stmt_t* 
xdb_parse_open_db (xdb_conn_t* pConn, xdb_token_t *pTkn)
{
	xdb_stmt_db_t *pStmt = &pConn->stmt_union.db_stmt;
	xdb_token_type type = xdb_next_token (pTkn);
	pStmt->stmt_type = XDB_STMT_OPEN_DB;
	pStmt->pSql = NULL;

	XDB_EXPECT (XDB_TOK_STR>=type, XDB_E_STMT, "Open DB miss database name");

	pStmt->db_name	= pTkn->token;
	pStmt->lock_mode = 0;
	return (xdb_stmt_t*)pStmt;

error:
	xdb_stmt_free ((xdb_stmt_t*)pStmt);
	return NULL;
}

XDB_STATIC xdb_stmt_t* 
xdb_parse_close_db (xdb_conn_t* pConn, xdb_token_t *pTkn)
{
	xdb_stmt_db_t *pStmt = &pConn->stmt_union.db_stmt;
	xdb_token_type type = xdb_next_token (pTkn);
	pStmt->stmt_type = XDB_STMT_CLOSE_DB;
	pStmt->pSql = NULL;

	XDB_EXPECT (type<=XDB_TOK_STR, XDB_E_STMT, "Miss database name");

	pStmt->bIfExistOrNot = false;

	pStmt->db_name	= pTkn->token;

	pStmt->pDbm = xdb_find_db (pStmt->db_name);

	XDB_EXPECT (NULL != pStmt->pDbm, XDB_E_NOTFOUND, "Can't close database '%s', database doesn't exists", pStmt->db_name);

	if ((NULL != pStmt->pDbm) && pStmt->pDbm->bSysDb) {
		XDB_EXPECT (pConn == s_xdb_sysdb_pConn, XDB_E_CONSTRAINT, "Can't close system database");
	}

	return (xdb_stmt_t*)pStmt;

error:
	xdb_stmt_free ((xdb_stmt_t*)pStmt);
	return NULL;

}

XDB_STATIC xdb_stmt_t* 
xdb_parse_dump_db (xdb_conn_t* pConn, xdb_token_t *pTkn)
{
	xdb_stmt_backup_t *pStmt = &pConn->stmt_union.backup_stmt;
	xdb_token_type type = xdb_next_token (pTkn);

	pStmt->stmt_type 	= XDB_STMT_DUMP_DB;
	pStmt->file 		= NULL;
	pStmt->bNoDrop 		= false;
	pStmt->bNoCreate 	= false;
	pStmt->bNoData 		= false;

	if (type >= XDB_TOK_END) {
		XDB_EXPECT (pConn->pCurDbm != NULL, XDB_E_NODB, XDB_SQL_NO_DB_ERR);
		pStmt->pDbm = pConn->pCurDbm;
	} else {
		pStmt->db_name	= pTkn->token;
		pStmt->pDbm = xdb_find_db (pStmt->db_name);
		XDB_EXPECT (NULL != pStmt->pDbm, XDB_E_NOTFOUND, "Can't dump database '%s', database doesn't exists", pStmt->db_name);
		type = xdb_next_token (pTkn);
		while (XDB_TOK_ID == type) {
			if (!strcasecmp (pTkn->token, "INTO")) {
				type = xdb_next_token (pTkn);
				XDB_EXPECT (XDB_TOK_STR == type, XDB_E_STMT, "Miss file name");
				pStmt->file = pTkn->token;
			} else if (!strcasecmp (pTkn->token, "NODROP")) {
				pStmt->bNoDrop 		= true;
			} else if (!strcasecmp (pTkn->token, "NOCREATE")) {
				pStmt->bNoCreate		= true;
			} else if (!strcasecmp (pTkn->token, "NODATA")) {
				pStmt->bNoData		= true;
			} 
			type = xdb_next_token (pTkn);			
		}
	}

	return (xdb_stmt_t*)pStmt;

error:
	xdb_stmt_free ((xdb_stmt_t*)pStmt);
	return NULL;

}
