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

#include "xdb_token.c"

#define XDB_PARSE_DBTBLNAME()	\
	pStmt->pTblm = xdb_parse_dbtblname (pConn, pTkn);	\
	type = pTkn->tk_type;	\
	XDB_EXPECT2 (pStmt->pTblm != NULL)

XDB_STATIC xdb_tblm_t* 
xdb_parse_dbtblname (xdb_conn_t *pConn, xdb_token_t *pTkn)
{
	xdb_tblm_t *pTblm;

	xdb_dbm_t *pDbm = pConn->pCurDbm;
	char *tbl_name = pTkn->token;
	int type = xdb_next_token (pTkn);
	if (XDB_TOK_DOT == type) {
		pDbm = xdb_find_db (tbl_name);
		XDB_EXPECT (NULL != pDbm, XDB_E_NOTFOUND, "Database '%s' doesn't exist", tbl_name);
		type = xdb_next_token (pTkn);
		XDB_EXPECT (type <= XDB_TOK_STR, XDB_E_STMT, "Miss table name");
		tbl_name = pTkn->token;
		type = xdb_next_token (pTkn);
	} else {
		XDB_EXPECT (NULL != pDbm, XDB_E_NODB, XDB_SQL_NO_DB_ERR);
	}
	pTblm = xdb_find_table (pDbm, tbl_name);
	XDB_EXPECT (NULL != pTblm, XDB_E_NOTFOUND, "Table '%s' doesn't exist", tbl_name);

	return pTblm;

error:
	return NULL;
}

#include "xdb_parser_db.c"
#include "xdb_parser_idx.c"
#include "xdb_parser_tbl.c"
#include "xdb_parser_dml.c"
#if (XDB_ENABLE_SERVER == 1)
#include "xdb_parser_svr.c"
#endif

XDB_STATIC xdb_stmt_t* 
xdb_parse_use (xdb_conn_t* pConn, xdb_token_t *pTkn)
{
	xdb_token_type	type = xdb_next_token (pTkn);
	xdb_stmt_db_t *pStmt = &pConn->stmt_union.db_stmt;;
	pStmt->stmt_type = XDB_STMT_USE_DB;
	pStmt->pSql = NULL;

	XDB_EXPECT (XDB_TOK_ID == type, XDB_E_NOTFOUND, "Use DB miss database name");

	pStmt->db_name	= pTkn->token;
	return (xdb_stmt_t*)pStmt;

error:
	xdb_stmt_free ((xdb_stmt_t*)pStmt);
	return NULL;
}

XDB_STATIC xdb_stmt_t* 
xdb_parse_create (xdb_conn_t* pConn, xdb_token_t *pTkn)
{
	xdb_token_type	type = xdb_next_token (pTkn);

	if (XDB_TOK_ID == type) {
		switch (pTkn->token[0]) {
		case 'D':
		case 'd':
			if (!strcasecmp (pTkn->token, "DATABASE")) {
				return xdb_parse_create_db (pConn, pTkn);
			}
			break;

		case 'T':
		case 't':
			if (!strcasecmp (pTkn->token, "TABLE")) {
				return xdb_parse_create_table (pConn, pTkn);
			}
			break;

		case 'I':
		case 'i':
			if (!strcasecmp (pTkn->token, "INDEX")) {
				return xdb_parse_create_index (pConn, pTkn, false);
			}
			break;

		case 'S':
		case 's':
			if (!strcasecmp (pTkn->token, "SERVER")) {
			#if (XDB_ENABLE_SERVER == 1)
				return xdb_parse_create_server (pConn, pTkn);
			#endif
			}
			break;
		case 'U':
		case 'u':
			if (!strcasecmp (pTkn->token, "UNIQUE")) {
				type = xdb_next_token (pTkn);
				XDB_EXPECT ((XDB_TOK_ID == type) && !strcasecmp (pTkn->token, "INDEX"), XDB_E_STMT, "Expect INDEX"XDB_SQL_CREATE_IDX_STMT);
				return xdb_parse_create_index (pConn, pTkn, true);
			}
			break;
		}
	}

error:
	return NULL;
}

XDB_STATIC xdb_stmt_t* 
xdb_parse_open (xdb_conn_t* pConn, xdb_token_t *pTkn)
{
	xdb_token_type	type = xdb_next_token (pTkn);

	if (XDB_TOK_ID == type) {
		switch (pTkn->token[0]) {
		case 'D':
		case 'd':
			if (!strcasecmp (pTkn->token, "DATABASE")) {
				return xdb_parse_open_db (pConn, pTkn);
			} else if (!strcasecmp (pTkn->token, "DATADIR")) {
				return xdb_parse_open_datadir (pConn, pTkn);
			}
			break;
		}
	}

	return NULL;
}

XDB_STATIC xdb_stmt_t* 
xdb_parse_set (xdb_conn_t* pConn, xdb_token_t *pTkn)
{
	xdb_stmt_set_t *pStmt = &pConn->stmt_union.set_stmt;
	pStmt->stmt_type = XDB_STMT_SET;
	pStmt->pSql = NULL;
	xdb_token_type	type;

	do {
		type = xdb_next_token (pTkn);
		XDB_EXPECT (XDB_TOK_ID==type, XDB_E_STMT, "Expect ID");

		const char *var = pTkn->token;
		type = xdb_next_token (pTkn);
		XDB_EXPECT (XDB_TOK_EQ==type, XDB_E_STMT, "Expect EQ");
		type = xdb_next_token (pTkn);
		XDB_EXPECT (XDB_TOK_STR>=type, XDB_E_STMT, "Expect STRING");

		//xdb_print ("var: %s\n", var);
		if (!strcasecmp (var, "DATADIR")) {
			pStmt->datadir = pTkn->token;
		} else if (!strcasecmp (var, "FORMAT")) {
			pStmt->format = pTkn->token;
		}
		type = xdb_next_token (pTkn);
	} while (XDB_TOK_COMMA == type);

	return (xdb_stmt_t*)pStmt;

error:
	xdb_stmt_free ((xdb_stmt_t*)pStmt);
	return NULL;
}

XDB_STATIC xdb_stmt_t* 
xdb_parse_source (xdb_conn_t* pConn, xdb_token_t *pTkn)
{
	xdb_stmt_backup_t *pStmt = &pConn->stmt_union.backup_stmt;
	xdb_token_type type = xdb_next_token (pTkn);
	pStmt->stmt_type = XDB_STMT_SOURCE;
	pStmt->pSql = NULL;

	XDB_EXPECT (XDB_TOK_STR>=type, XDB_E_STMT, "Miss file name");

	pStmt->file = pTkn->token;
	return (xdb_stmt_t*)pStmt;

error:
	xdb_stmt_free ((xdb_stmt_t*)pStmt);
	return NULL;
}

XDB_STATIC xdb_stmt_t* 
xdb_parse_shell (xdb_conn_t* pConn, xdb_token_t *pTkn)
{
	xdb_stmt_t *pStmt = &pConn->stmt_union.stmt;

	pStmt->stmt_type = XDB_STMT_SHELL;
	pStmt->pSql = NULL;

	return (xdb_stmt_t*)pStmt;
}

XDB_STATIC xdb_stmt_t* 
xdb_parse_desc (xdb_conn_t* pConn, xdb_token_t *pTkn)
{
	xdb_stmt_tbl_t *pStmt = &pConn->stmt_union.tbl_stmt;
	pStmt->stmt_type = XDB_STMT_DESC;
	pStmt->pSql = NULL;
	xdb_token_type	type = xdb_next_token (pTkn);

	XDB_EXPECT (XDB_TOK_ID==type, XDB_E_STMT, "Expect ID");

	XDB_PARSE_DBTBLNAME();

	return (xdb_stmt_t*)pStmt;

error:
	xdb_stmt_free ((xdb_stmt_t*)pStmt);
	return NULL;
}

XDB_STATIC xdb_stmt_t* 
xdb_parse_show (xdb_conn_t* pConn, xdb_token_t *pTkn)
{
	// show databases; show tables, show index, show columns, show create table
	xdb_stmt_select_t *pStmt = &pConn->stmt_union.select_stmt;
	pStmt->stmt_type = XDB_STMT_INVALID;
	pStmt->pSql = NULL;
	xdb_token_type	type = xdb_next_token (pTkn);

	XDB_EXPECT (XDB_TOK_ID==type, XDB_E_STMT, "Expect ID");

	if (!strcasecmp (pTkn->token, "DATABASES")) {
		pStmt->stmt_type = XDB_STMT_SHOW_DB;
	} else if (!strcasecmp (pTkn->token, "TABLES")) {
		XDB_EXPECT (pConn->pCurDbm != NULL, XDB_E_NODB, XDB_SQL_NO_DB_ERR);
		pStmt->stmt_type = XDB_STMT_SHOW_TBL;
	} else if (!strcasecmp (pTkn->token, "COLUMNS")) {
		XDB_EXPECT (pConn->pCurDbm != NULL, XDB_E_NODB, XDB_SQL_NO_DB_ERR);
		pStmt->stmt_type = XDB_STMT_SHOW_COL;
	} else if (!strcasecmp (pTkn->token, "INDEXES") || !strcasecmp (pTkn->token, "INDEX") || !strcasecmp (pTkn->token, "KEYS")) {
		XDB_EXPECT (pConn->pCurDbm != NULL, XDB_E_NODB, XDB_SQL_NO_DB_ERR);
		pStmt->stmt_type = XDB_STMT_SHOW_IDX;
	} else if (!strcasecmp (pTkn->token, "CREATE")) {
		type = xdb_next_token (pTkn);
		XDB_EXPECT (XDB_TOK_ID == type, XDB_E_NODB, "Miss show object");
		if (!strcasecmp (pTkn->token, "TABLE")) {
			pStmt->stmt_type = XDB_STMT_SHOW_CREATE_TBL;
			type = xdb_next_token (pTkn);
			XDB_PARSE_DBTBLNAME ();
		}
	} else {
		XDB_SETERR (XDB_E_STMT, "Unknown show object '%s'", pTkn->token);
		return NULL;
	}
	return (xdb_stmt_t*)pStmt;

error:
	xdb_stmt_free ((xdb_stmt_t*)pStmt);
	return NULL;
}

XDB_STATIC xdb_stmt_t* 
xdb_parse_drop (xdb_conn_t* pConn, xdb_token_t *pTkn)
{
	xdb_token_type	type = xdb_next_token (pTkn);

	if (XDB_TOK_ID == type) {
		switch (pTkn->token[0]) {
		case 'D':
		case 'd':
			if (!strcasecmp (pTkn->token, "DATABASE")) {
				return xdb_parse_drop_db (pConn, pTkn);
			}
			break;

		case 'T':
		case 't':
			if (!strcasecmp (pTkn->token, "TABLE")) {
				return xdb_parse_drop_table (pConn, pTkn);
			}
			break;

		case 'I':
		case 'i':
			if (!strcasecmp (pTkn->token, "INDEX")) {
				return xdb_parse_drop_index (pConn, pTkn);
			}
			break;

		case 'S':
		case 's':
			if (!strcasecmp (pTkn->token, "SERVER")) {
				#if (XDB_ENABLE_SERVER == 1)
				return xdb_parse_drop_server (pConn, pTkn);
				#endif
			}
			break;
		}
	}

	XDB_SETERR (XDB_E_STMT, "Unknown drop object '%s'", pTkn->token);
	return NULL;
}

XDB_STATIC xdb_stmt_t* 
xdb_parse_close (xdb_conn_t* pConn, xdb_token_t *pTkn)
{
	xdb_token_type	type = xdb_next_token (pTkn);

	if (XDB_TOK_ID == type) {
		switch (pTkn->token[0]) {
		case 'D':
		case 'd':
			if (!strcasecmp (pTkn->token, "DATABASE")) {
				return xdb_parse_close_db (pConn, pTkn);
			}
			break;
		}
	}

	XDB_SETERR (XDB_E_STMT, "Unknown close object '%s'", pTkn->token);
	return NULL;
}

XDB_STATIC xdb_stmt_t* 
xdb_parse_dump (xdb_conn_t* pConn, xdb_token_t *pTkn)
{
	xdb_token_type	type = xdb_next_token (pTkn);

	if (XDB_TOK_ID == type) {
		switch (pTkn->token[0]) {
		case 'D':
		case 'd':
			if (!strcasecmp (pTkn->token, "DATABASE")) {
				return xdb_parse_dump_db (pConn, pTkn);
			}
			break;
		}
	}

	XDB_SETERR (XDB_E_STMT, "Unknown close object '%s'", pTkn->token);
	return NULL;
}

XDB_STATIC xdb_stmt_t* 
xdb_parse_lock (xdb_conn_t* pConn, xdb_token_t *pTkn)
{
	xdb_stmt_lock_t *pStmt = &pConn->stmt_union.lock_stmt;
	pStmt->stmt_type = XDB_STMT_LOCK;
	pStmt->pSql = NULL;
	XDB_EXPECT(pConn->bInTrans, XDB_E_CONSTRAINT, "Not in transaction");

	// TBD
	pStmt->tbl_num = 0;
#if 0
	xdb_token_type	type = xdb_next_token (pTkn);

	XDB_EXPECT(type<=XDB_TOK_STR, XDB_E_STMT, "Miss table name");

	XDB_PARSE_DBTBLNAME();
#endif
	return (xdb_stmt_t*)pStmt;

error:
	xdb_stmt_free ((xdb_stmt_t*)pStmt);
	return NULL;
}

XDB_STATIC xdb_stmt_t* 
xdb_parse_help (xdb_conn_t* pConn, xdb_token_t *pTkn)
{
	xdb_stmt_t *pStmt = &pConn->stmt_union.stmt;;
	xdb_token_type	type = xdb_next_token (pTkn);
	pStmt->stmt_type = XDB_STMT_HELP;
	pStmt->pSql = NULL;

	if (type >= XDB_TOK_END) {
		fprintf (pConn->conn_stdout, "  CREATE\n  DROP\n  ALTER\n  INSERT\n  SELECT\n  UPDATE\n  DELETE\n  SHOW\n");
	}

	return pStmt;
}

XDB_STATIC xdb_stmt_t* 
xdb_parse_explain (xdb_conn_t* pConn, xdb_token_t *pTkn)
{
	xdb_token_type	type = xdb_next_token (pTkn);
	XDB_EXPECT (type == XDB_TOK_ID, XDB_E_STMT, "Miss SELECT");

	if (! strcasecmp (pTkn->token, "SELECT")) {
		xdb_stmt_t *pStmt = xdb_parse_select (pConn, pTkn, false);
		if (NULL != pStmt) {
			pStmt->stmt_type = XDB_STMT_EXPLAIN;
			pStmt->pSql = NULL;
			return pStmt;
		}
		XDB_SETERR (XDB_E_STMT, "Invalid SELECT statement");
	} else {
		XDB_SETERR (XDB_E_STMT, "Only SELECT is supported");
	}

error:
	return NULL;
}

XDB_STATIC xdb_stmt_t* 
xdb_sql_parse (xdb_conn_t* pConn, char **ppSql, bool bPStmt)
{
	char *pSql = *ppSql;

	xdb_token_t token = XDB_TOK_INIT(pSql);
	xdb_stmt_t *pStmt = NULL;

	xdb_token_type	type = xdb_next_token (&token);
	if (XDB_TOK_ID == type) {
		switch (token.token[0]) {
		case 'S':
		case 's':
			if (! strcasecmp (token.token, "SELECT")) {
				pStmt = xdb_parse_select (pConn, &token, bPStmt);
			} else if (! strcasecmp (token.token, "SHOW")) {
				pStmt = xdb_parse_show (pConn, &token);
			} else if (! strcasecmp (token.token, "SET")) {
				pStmt = xdb_parse_set (pConn, &token);
			} else if (! strcasecmp (token.token, "SOURCE")) {
				pStmt = xdb_parse_source (pConn, &token);
			} else if (! strcasecmp (token.token, "SHELL")) {
				pStmt = xdb_parse_shell (pConn, &token);
			} else {
				goto error;
			}
			break;
		case 'D':
		case 'd':
			if (! strcasecmp (token.token, "DELETE")) {
				pStmt = xdb_parse_delete (pConn, &token, bPStmt);
			} else if (! strcasecmp (token.token, "DROP")) {
				pStmt = xdb_parse_drop (pConn, &token);
			} else if (! strcasecmp (token.token, "DESC") || ! strcasecmp (token.token, "DESCRIBE")) {
				pStmt = xdb_parse_desc (pConn, &token);
			} else if (! strcasecmp (token.token, "DUMP")) {
				pStmt = xdb_parse_dump (pConn, &token);
			} else {
				goto error;
			}
			break;
		case 'I':
		case 'i':
			if (! strcasecmp (token.token, "INSERT")) {
				pStmt = xdb_parse_insert (pConn, &token, bPStmt);
			} else {
				goto error;
			}
			break;
		case 'C':
		case 'c':
			if (! strcasecmp (token.token, "CREATE")) {
				pStmt = xdb_parse_create (pConn, &token);
			} else if (! strcasecmp (token.token, "COMMIT")) {
				pStmt = &pConn->stmt_union.stmt;
				pStmt->stmt_type = XDB_STMT_COMMIT;
				pStmt->pSql = NULL;
			} else if (! strcasecmp (token.token, "CLOSE")) {
				pStmt = xdb_parse_close (pConn, &token);;
			} else {
				goto error;
			}
			break;
		case 'U':
		case 'u':
			if (! strcasecmp (token.token, "UPDATE")) {
				pStmt = xdb_parse_update (pConn, &token, bPStmt);
			} else if (! strcasecmp (token.token, "USE")) {
				pStmt = xdb_parse_use (pConn, &token);
			} else {
				goto error;
			}
			break;
		case 'B':
		case 'b':
			if (! strcasecmp (token.token, "BEGIN")) {
				pStmt = &pConn->stmt_union.stmt;
				pStmt->stmt_type = XDB_STMT_BEGIN;
				pStmt->pSql = NULL;
			} else {
				goto error;
			}
			break;
		case 'R':
		case 'r':
			if (! strcasecmp (token.token, "ROLLBACK")) {
				pStmt = &pConn->stmt_union.stmt;
				pStmt->stmt_type = XDB_STMT_ROLLBACK;
				pStmt->pSql = NULL;
			} else {
				goto error;
			}
			break;
		case 'L':
		case 'l':
			if (! strcasecmp (token.token, "LOCK")) {
				pStmt = xdb_parse_lock (pConn, &token);
				
			} else {
				goto error;
			}
			break;
		case 'O':
		case 'o':
			if (! strcasecmp (token.token, "OPEN")) {
				pStmt = xdb_parse_open (pConn, &token);
			} else {
				goto error;
			}
			break;

		case 'E':
		case 'e':
			if (! strcasecmp (token.token, "EXPLAIN")) {
				pStmt = xdb_parse_explain (pConn, &token);
			} else {
				goto error;
			}
			break;
		case 'H':
		case 'h':
			if (! strcasecmp (token.token, "HELP")) {
				pStmt = xdb_parse_help (pConn, &token);
			} else {
				goto error;
			}
			break;
		default:
			goto error;
		}
	} else {
		XDB_SETERR(XDB_E_STMT, "No query specified");
	}

exit:
	if (NULL != pStmt) {
		pStmt->pConn = pConn;
	} else {
		xdb_res_t*	pRes = &pConn->conn_res;
		pRes->row_data = (uintptr_t)pConn->conn_msg.msg;
		pRes->data_len = sizeof (xdb_msg_t) + pConn->conn_msg.len + 1;
	}

	// move to end of token for next stmt
	type = token.tk_type;
	if (xdb_unlikely (type < XDB_TOK_END)) {
		do {
			type = xdb_next_token (&token);
		} while (type < XDB_TOK_END);
	}
	
	pSql = token.tk_sql;
	if ((XDB_TOK_EOF == type) || (*pSql == '\0') || (*(pSql+1) == '\0')) {
		*ppSql = NULL;
	} else {
		*ppSql	= pSql;
	}

	return pStmt;

error:
	XDB_SETERR (XDB_E_STMT, "Unkown SQL command %s", token.token);
	goto exit;
}

XDB_STATIC xdb_stmt_t* 
xdb_sql_parse_alloc (xdb_conn_t* pConn, const char *sql, bool bPStmt)
{
	int len = strlen (sql);
	while (len && isspace((int)sql[len-1])) {
		len--;
	}
	char *pSql = xdb_strdup (sql, len);
	if (xdb_unlikely (NULL == pSql)) {
		return NULL;
	}
	char *pSql2 = pSql;
	xdb_stmt_t *pStmt = xdb_sql_parse (pConn, &pSql2, bPStmt);
	if (xdb_likely (NULL != pStmt)) {
		pStmt->pSql = pSql;
		// only support single STMT
		if (pSql2 != NULL) {
			// pSql is free here
			xdb_stmt_free (pStmt);
			return NULL;
		}
	}
	return pStmt;
}

// note: parser should set allocated resources to null in case wrong free
XDB_STATIC void 
xdb_stmt_free (xdb_stmt_t *pStmt)
{
	if (xdb_likely (NULL == pStmt)) {
		return;
	}
	switch (pStmt->stmt_type) {
	case XDB_STMT_INSERT:
		{
			xdb_stmt_insert_t* pStmtIns = (xdb_stmt_insert_t*)pStmt;
			if (pStmtIns->pRowsBuf != pStmtIns->row_buf) {
				xdb_free (pStmtIns->pRowsBuf);
			}
		}
		break;
	case XDB_STMT_SELECT:
		{
			xdb_stmt_select_t* pStmtSel = (xdb_stmt_select_t*)pStmt;
			if ((pStmtSel->meta_size) > 0 && ((void*)pStmtSel->pMeta != (void*)pStmtSel->set_flds)) {
				xdb_free (pStmtSel->pMeta);
			}
		}
		break;
	default:
		break;
	}
	if (xdb_unlikely ((pStmt->pSql != NULL) && (pStmt->pSql != pStmt->pConn->sql_buf))) {
		xdb_free (pStmt->pSql);
	}
}


#ifdef XDB_MOD_TEST
/*
gcc xdb_parser.c -DXDB_MOD_TEST -g
*/
	
static const char* xdb_tok2str(xdb_token_type tp) 
{
	const char *id2str[] = {
		[XDB_TOK_ID   ] = "ID",
		[XDB_TOK_NUM  ] = "NUM",
		[XDB_TOK_ESC  ] = "ESC",
		[XDB_TOK_STR  ] = "STR",
		[XDB_TOK_SP   ] = "SP",
		[XDB_TOK_EQ   ] = "EQ",
		[XDB_TOK_LT   ] = "LT",
		[XDB_TOK_GT   ] = "GT",
		[XDB_TOK_ADD  ] = "ADD",
		[XDB_TOK_SUB  ] = "SUB",
		[XDB_TOK_MUL  ] = "MUL",
		[XDB_TOK_DIV  ] = "DIV",
		[XDB_TOK_MOD  ] = "MOD",
		[XDB_TOK_AND  ] = "AND",
		[XDB_TOK_OR   ] = "OR",
		[XDB_TOK_XOR  ] = "XOR",
		[XDB_TOK_NOT  ] = "NOT",
		[XDB_TOK_NEG  ] = "NEG",
		[XDB_TOK_COMMA] = "COMMA",
		[XDB_TOK_DOT  ] = "DOT",
		[XDB_TOK_LP   ] = "LP",
		[XDB_TOK_RP   ] = "RP",
		[XDB_TOK_END  ] = "END",
		[XDB_TOK_EOF  ] = "EOF",
		[XDB_TOK_INV  ] = "INV"
	};
	return tp <= XDB_ARY_LEN(id2str)? id2str[tp] : "Unkonwn";
}

int main ()
{
	char sql[] = "INSERT INTO student(name,age)VALUES('jack',10),('tom',11)";
	xdb_token_t token = XDB_TOK_INIT(sql);

	xdb_token_type	type;
	while (XDB_TOK_EOF != (type = xdb_next_token (&token))) {
		printf ("%s %s |", xdb_tok2str(type), XDB_TOK_ID==type||XDB_TOK_NUM==type||XDB_TOK_STR==type?token.token:"");
	}
	printf ("\n");
#if 0
	xdb_stmt_t *pStmt = xdb_parse_sql ("CREATE DATABASE basic", 0);
	if (NULL != pStmt) {
		if (XDB_STMT_CREATE_DB == pStmt->stmt_type) {
			xdb_stmt_db_t *pStmtDb = (xdb_stmt_db_t*)pStmt;
			printf ("ok create db '%s'\n", pStmtDb->db_name);
		}
	}
#endif
	return 0;
}
#endif
