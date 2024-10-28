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
xdb_parse_field (xdb_conn_t* pConn, xdb_token_t *pTkn, xdb_stmt_tbl_t *pStmt)
{
	xdb_field_t *pFld = &pStmt->stmt_flds[pStmt->fld_count];
	memset (pFld, 0, sizeof (*pFld));
	memset (pFld->idx_fid, 0xFF, sizeof(pFld->idx_fid));
	pFld->fld_id = pStmt->fld_count++;
	pFld->obj.nm_len = pTkn->tk_len;
	xdb_strcpy (pFld->obj.obj_name, pTkn->token);
	XDB_OBJ_ID(pFld) = pFld->fld_id;
	pFld->fld_off = -1;

	xdb_token_type type = xdb_next_token (pTkn);
	XDB_EXPECT (XDB_TOK_ID == type, XDB_E_STMT, "Miss field type");
	if (0 == strcasecmp (pTkn->token, "INT")) {
		pFld->fld_type = XDB_TYPE_INT;
		pFld->sup_type = XDB_TYPE_BIGINT;
	} else if (0 == strcasecmp (pTkn->token, "CHAR")) {
		pFld->fld_type = XDB_TYPE_CHAR;
		pFld->sup_type = XDB_TYPE_CHAR;
		pFld->fld_len = 1;
	} else if (0 == strcasecmp (pTkn->token, "VARCHAR")) {
		pFld->fld_type = XDB_TYPE_VCHAR;
		pFld->sup_type = XDB_TYPE_VCHAR;
		pFld->fld_vid = pStmt->vfld_count++;
		pFld->fld_len = 65535;
	} else if (0 == strcasecmp (pTkn->token, "BINARY")) {
		pFld->fld_type = XDB_TYPE_BINARY;
		pFld->sup_type = XDB_TYPE_BINARY;
		pFld->fld_len = 1;
	} else if (0 == strcasecmp (pTkn->token, "VARBINARY")) {
		pFld->fld_type = XDB_TYPE_VBINARY;
		pFld->sup_type = XDB_TYPE_VBINARY;
		pFld->fld_vid = pStmt->vfld_count++;
		pFld->fld_len = 65535;
	} else if (0 == strcasecmp (pTkn->token, "TINYINT")) {
		pFld->fld_type = XDB_TYPE_TINYINT;
		pFld->sup_type = XDB_TYPE_BIGINT;
	} else if (0 == strcasecmp (pTkn->token, "SMALLINT")) {
		pFld->fld_type = XDB_TYPE_SMALLINT;
		pFld->sup_type = XDB_TYPE_BIGINT;
	} else if (0 == strcasecmp (pTkn->token, "BIGINT")) {
		pFld->fld_type = XDB_TYPE_BIGINT;
		pFld->sup_type = XDB_TYPE_BIGINT;
	} else if (0 == strcasecmp (pTkn->token, "FLOAT")) {
		pFld->fld_type = XDB_TYPE_FLOAT;
		pFld->sup_type = XDB_TYPE_DOUBLE;
	} else if (0 == strcasecmp (pTkn->token, "DOUBLE")) {
		pFld->fld_type = XDB_TYPE_DOUBLE;
		pFld->sup_type = XDB_TYPE_DOUBLE;
	} else {
		XDB_EXPECT (0, XDB_E_STMT, "unkown data type '%s'", pTkn->token);
	}
	type = xdb_next_token (pTkn);
	if (XDB_TOK_LP == type) {
		type = xdb_next_token (pTkn);
		XDB_EXPECT (XDB_TOK_NUM == type, XDB_E_STMT, "Miss type length");
		pFld->fld_len = atoi (pTkn->token);
		type = xdb_next_token (pTkn);
		XDB_EXPECT (XDB_TOK_RP == type, XDB_E_STMT, "Miss )");
		type = xdb_next_token (pTkn);
	}
	if ((XDB_TOK_ID==type) && !strcasecmp (pTkn->token, "COLLATE")) {
		type = xdb_next_token (pTkn);
		if ((XDB_TOK_ID==type) && !strcasecmp (pTkn->token, "BINARY")) {
			pFld->fld_flags |= XDB_FLD_BINARY;
		} else {
			XDB_EXPECT ((XDB_TOK_ID==type) && !strcasecmp (pTkn->token, "NOCASE"), XDB_E_STMT, "Expect COLLATE {BINARY | NOCASE}");
		}
		type = xdb_next_token (pTkn);
	}

	do {
		if ((XDB_TOK_COMMA == type) || (XDB_TOK_RP == type)) {
			return type;
		}
		XDB_EXPECT (XDB_TOK_ID==type, XDB_E_STMT, "Expect ID for filed option");
		// pare option
		if (!strcasecmp (pTkn->token, "PRIMARY") || !strcasecmp (pTkn->token, "KEY") || !strcasecmp (pTkn->token, "UNIQUE")) {
			xdb_stmt_idx_t *pStmtIdx = &pStmt->stmt_idx[pStmt->idx_count++];
			pStmtIdx->idx_type = XDB_IDX_HASH;
			pStmtIdx->xoid	= -1;
			
			const char *idx_type = pTkn->token;			
			type = xdb_next_token (pTkn);
			if (!strcasecmp (idx_type, "PRIMARY") || !strcasecmp (idx_type, "KEY")) {
				if (!strcasecmp (idx_type, "PRIMARY")) {
					XDB_EXPECT ((XDB_TOK_ID==type) && !strcasecmp (pTkn->token, "KEY"), XDB_E_STMT, "PRIMARY Miss KEY");
					type = xdb_next_token (pTkn);
					pFld->fld_flags |= XDB_FLD_NOTNULL;
				}
				XDB_EXPECT (pStmt->pkey_idx < 0, XDB_E_STMT, "Duplicate PRIMARY KEY");
				pStmt->pkey_idx = pStmt->idx_count - 1;
				pStmtIdx->idx_name = "PRIMARY";
				pStmtIdx->bUnique = true;
				pStmtIdx->bPrimary = true;
			} else {
				pStmtIdx->bUnique = true;
				if ((XDB_TOK_ID==type) && !strcasecmp (pTkn->token, "KEY")) {
					type = xdb_next_token (pTkn);
				}
				char buf[XDB_NAME_LEN * 2];
				xdb_sprintf (buf, "%s_%d", XDB_OBJ_NAME(pFld), pStmt->idx_count);
				memcpy (pStmtIdx->idxName, buf, XDB_NAME_LEN);
				pStmtIdx->idxName[XDB_NAME_LEN] = '\0';
				pStmtIdx->idx_name = pStmtIdx->idxName;
			}
			pStmtIdx->fld_count = 1;
			pStmtIdx->idx_col[0] = XDB_OBJ_NAME(pFld);
		} else if (0 == strcasecmp (pTkn->token, "NOT")) {
			type = xdb_next_token (pTkn);
			XDB_EXPECT ((XDB_TOK_ID==type) && !strcasecmp (pTkn->token, "NULL"), XDB_E_STMT, "Expect NOT NULL");
			pFld->fld_flags |= XDB_FLD_NOTNULL;
			type = xdb_next_token (pTkn);
		} else if (0 == strcasecmp (pTkn->token, "XOFFSET")) {
			type = xdb_next_token (pTkn);
			XDB_EXPECT (XDB_TOK_NUM == type, XDB_E_STMT, "Miss offset value");
			pFld->fld_off = atoi (pTkn->token);
			type = xdb_next_token (pTkn);
		}
	} while (1);

error:
	return -1;
}

XDB_STATIC xdb_stmt_t* 
xdb_parse_create_table (xdb_conn_t* pConn, xdb_token_t *pTkn)
{
	int rc;
	xdb_stmt_tbl_t *pStmt = &pConn->stmt_union.tbl_stmt;
	pStmt->stmt_type = XDB_STMT_CREATE_TBL;
	pStmt->pSql = NULL;

	XDB_EXPECT (NULL != pConn->pCurDbm, XDB_E_NODB, XDB_SQL_NO_DB_ERR);

	xdb_token_type type = xdb_next_token (pTkn);

	XDB_EXPECT (type<=XDB_TOK_STR, XDB_E_STMT, "Miss table name");

	pStmt->bIfExistOrNot = false;
	if ((XDB_TOK_ID==type) && !strcasecmp (pTkn->token, "IF")) {
		type = xdb_next_token (pTkn);
		XDB_EXPECT ((XDB_TOK_ID==type) && !strcasecmp (pTkn->token, "NOT"), XDB_E_STMT, "Miss NOT");
		type = xdb_next_token (pTkn);
		XDB_EXPECT ((XDB_TOK_ID==type) && !strcasecmp (pTkn->token, "EXISTS"), XDB_E_STMT, "Miss EXISTS");
		type = xdb_next_token (pTkn);
		XDB_EXPECT (type<=XDB_TOK_STR, XDB_E_STMT, "Miss table name");
		pStmt->bIfExistOrNot = true;
	}

	pStmt->pDbm = pConn->pCurDbm;
	char *tbl_name = pTkn->token;
	type = xdb_next_token (pTkn);
	if (XDB_TOK_DOT == type) {
		pStmt->pDbm = xdb_find_db (tbl_name);
		XDB_EXPECT (NULL != pStmt->pDbm, XDB_E_NOTFOUND, "Database '%s' doesn't exist", tbl_name);
		type = xdb_next_token (pTkn);
		XDB_EXPECT (type <= XDB_TOK_STR, XDB_E_STMT, "Miss table name");
		tbl_name = pTkn->token;
		type = xdb_next_token (pTkn);
	} else {
		XDB_EXPECT (NULL != pStmt->pDbm, XDB_E_NODB, XDB_SQL_NO_DB_ERR);
	}
	pStmt->pTblm = xdb_find_table (pStmt->pDbm, tbl_name);
	XDB_EXPECT (pStmt->bIfExistOrNot || (NULL == pStmt->pTblm), XDB_E_EXISTS, "Table '%s' already exists", tbl_name);

	if (pStmt->pDbm->bSysDb) {
		XDB_EXPECT (pConn == s_xdb_sysdb_pConn, XDB_E_CONSTRAINT, "Can't create table '%s' in system database", tbl_name);
	}

	pStmt->tbl_name = tbl_name;
	pStmt->bMemory	= pStmt->pDbm->bMemory;
	pStmt->xoid 	= -1;
	pStmt->stmt_type = XDB_STMT_CREATE_TBL;
	pStmt->pSql = NULL;
	pStmt->fld_count = 0;
	pStmt->vfld_count = 0;
	pStmt->idx_count = 0;
	pStmt->sql = pTkn->tk_sql;
	pStmt->pkey_idx = -1;
	pStmt->row_size = 0;

	XDB_EXPECT (XDB_TOK_LP==type, XDB_E_STMT, "Expect (");

	do {
		type = xdb_next_token (pTkn);
		XDB_EXPECT (XDB_TOK_ID==type, XDB_E_STMT, "Expect ID for create def %d %s", type, pTkn->token);
		if (!strcasecmp (pTkn->token, "PRIMARY") || !strcasecmp (pTkn->token, "UNIQUE") || !strcasecmp (pTkn->token, "INDEX") || !strcasecmp (pTkn->token, "KEY")) {
			xdb_stmt_idx_t *pStmtIdx = &pStmt->stmt_idx[pStmt->idx_count++];
			pStmtIdx->idx_type = XDB_IDX_HASH;
			pStmtIdx->xoid	= -1;
			pStmtIdx->idx_name = NULL;
			pStmtIdx->bUnique = false;

			const char *idx_type = pTkn->token;
			type = xdb_next_token (pTkn);
			if (!strcasecmp (idx_type, "PRIMARY")) {
				XDB_EXPECT ((XDB_TOK_ID==type) && !strcasecmp (pTkn->token, "KEY"), XDB_E_STMT, "PRIMARY Miss KEY");
				XDB_EXPECT (pStmt->pkey_idx < 0, XDB_E_STMT, "Duplicate PRIMARY KEY");
				pStmt->pkey_idx = pStmt->idx_count - 1;
				pStmtIdx->idx_name = "PRIMARY";
				pStmtIdx->bUnique = true;
				pStmtIdx->bPrimary = true;
				type = xdb_next_token (pTkn);
			} else if (!strcasecmp (idx_type, "UNIQUE")) {
				pStmtIdx->bUnique = true;
				if (!strcasecmp (pTkn->token, "INDEX") || !strcasecmp (pTkn->token, "KEY")) {
					type = xdb_next_token (pTkn);
				}
			}
			if (XDB_TOK_ID == type) {
				XDB_EXPECT (strcasecmp (idx_type, "PRIMARY"), XDB_E_STMT, "PRIMARY KEY can't give name");
				pStmtIdx->idx_name = pTkn->token;
				type = xdb_next_token (pTkn);
			}
			rc = xdb_parse_create_idx_def (pConn, pTkn, pStmtIdx);
			if (NULL == pStmtIdx->idx_name) {
				xdb_sprintf (pStmtIdx->idxName, "%s_%d", pStmtIdx->idx_col[0], pStmt->idx_count);
				pStmtIdx->idx_name = pStmtIdx->idxName;
			}
		} else {
			// field
			rc = xdb_parse_field (pConn, pTkn, pStmt);
		}
		XDB_EXPECT2 (rc >= XDB_OK);
		if (rc == XDB_TOK_RP) {
			break;
		}
	} while (1);

	// parse table options
	do {
		type = xdb_next_token (pTkn);
		if (XDB_TOK_END <= type) {
			break;
		}
		XDB_EXPECT (XDB_TOK_ID==type, XDB_E_STMT, "Expect ID for table option");

		const char *var = pTkn->token;
		type = xdb_next_token (pTkn);
		XDB_EXPECT (XDB_TOK_EQ==type, XDB_E_STMT, "Expect EQ");
		type = xdb_next_token (pTkn);
	
		if (!strcasecmp (var, "XOID")) {
			XDB_EXPECT (XDB_TOK_NUM == type, XDB_E_STMT, "Expect number");
			pStmt->xoid = atoi (pTkn->token);
			//xdb_dbglog ("table xoid = %d\n", pStmt->xoid);
		} else if (!strcasecmp (var, "ENGINE")) {
			XDB_EXPECT ((XDB_TOK_ID == type) && !strcasecmp (pTkn->token, "MEMORY"), XDB_E_STMT, "Expect MEMORY");
			pStmt->bMemory = true;
		}
		type = xdb_next_token (pTkn);
	} while (XDB_TOK_COMMA == type);

	//XDB_EXPECT (pStmt->pkey_idx >= 0, XDB_E_STMT, "Table must have PRIMARY KEY");

	return (xdb_stmt_t*)pStmt;

error:
	xdb_stmt_free ((xdb_stmt_t*)pStmt);	
	return NULL;
}

XDB_STATIC xdb_stmt_t* 
xdb_parse_drop_table (xdb_conn_t* pConn, xdb_token_t *pTkn)
{
	xdb_stmt_tbl_t *pStmt = &pConn->stmt_union.tbl_stmt;
	pStmt->stmt_type = XDB_STMT_DROP_TBL;
	pStmt->pSql = NULL;

	XDB_EXPECT (NULL != pConn->pCurDbm, XDB_E_NODB, XDB_SQL_NO_DB_ERR);

	xdb_token_type type = xdb_next_token (pTkn);
	XDB_EXPECT (type<=XDB_TOK_STR, XDB_E_STMT, "Miss table name: "XDB_SQL_DROP_TBL_STMT);

	pStmt->bIfExistOrNot = false;

	if ((XDB_TOK_ID==type) && !strcasecmp (pTkn->token, "IF")) {
		type = xdb_next_token (pTkn);
		XDB_EXPECT ((XDB_TOK_ID==type) && !strcasecmp (pTkn->token, "EXISTS"), XDB_E_STMT, "Miss EXISTS: "XDB_SQL_DROP_TBL_STMT);
		type = xdb_next_token (pTkn);
		XDB_EXPECT (type<=XDB_TOK_STR, XDB_E_STMT, "Miss table name: "XDB_SQL_DROP_TBL_STMT);
		pStmt->bIfExistOrNot = true;
	}

	pStmt->pTblm = xdb_parse_dbtblname (pConn, pTkn);
	XDB_EXPECT2 (pStmt->pTblm != NULL || pStmt->bIfExistOrNot);

	if ((NULL != pStmt->pTblm) && pStmt->pTblm->pDbm->bSysDb) {
		XDB_EXPECT (pConn == s_xdb_sysdb_pConn, XDB_E_CONSTRAINT, "Can't drop table '%s' in system database", XDB_OBJ_NAME(pStmt->pTblm));
	}

	return (xdb_stmt_t*)pStmt;

error:
	xdb_stmt_free ((xdb_stmt_t*)pStmt);
	return NULL;
}
