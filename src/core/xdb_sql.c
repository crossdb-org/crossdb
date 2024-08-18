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

XDB_STATIC int 
xdb_sql_set (xdb_stmt_set_t *pStmt)
{
	xdb_conn_t		*pConn = pStmt->pConn;

	if (NULL != pStmt->datadir) {
		xdb_strcpy (s_xdb_datadir, pStmt->datadir);
	}

	if (NULL != pStmt->format) {
		if (pConn->res_format >= XDB_FMT_NATIVELE) {
			XDB_EXPECT_RETE(pConn->res_format < XDB_FMT_NATIVELE, XDB_E_CONSTRAINT, 
				"Native format is not allowed to change");
		}
		if (!strcasecmp ("JSON", pStmt->format)) {
			pConn->res_format = XDB_FMT_JSON;
		} else if (!strcasecmp ("TABLE", pStmt->format)) {
			pConn->res_format = XDB_FMT_TABLE;
		} else if (!strcasecmp ("VERTICAL", pStmt->format)) {
			pConn->res_format = XDB_FMT_VERTICAL;
		} else if (!strcasecmp ("NATIVELE", pStmt->format)) {
			pConn->res_format = XDB_FMT_NATIVELE;
		} else if (!strcasecmp ("NATIVEBE", pStmt->format)) {
			pConn->res_format = XDB_FMT_NATIVEBE;
		}
		//xdb_dbglog ("set format %d\n", pConn->res_format);
	}

	return XDB_OK;
}

xdb_res_t*
xdb_stmt_exec (xdb_stmt_t *pStmt)
{
	int 	rc = 0;
	xdb_res_t*	pRes;
	xdb_conn_t		*pConn = pStmt->pConn;

	if (xdb_likely (XDB_STMT_SELECT == pStmt->stmt_type)) {
		pRes = xdb_sql_select ((xdb_stmt_select_t*)pStmt);
	} else {
		pRes = &pConn->conn_res;
		pRes->errcode = 0;
		pRes->col_meta  = 0;
		pRes->row_count = 0;
		pRes->affected_rows = 0;
		pRes->data_len	= 0;
		pConn->conn_msg.len = 0;
		switch (pStmt->stmt_type) {
		case XDB_STMT_UPDATE: 
			pRes->affected_rows = xdb_sql_update ((xdb_stmt_select_t*)pStmt);
			break;
		case XDB_STMT_INSERT:
			pRes->affected_rows = xdb_sql_insert ((xdb_stmt_insert_t*)pStmt);
			break;
		case XDB_STMT_DELETE:
			pRes->affected_rows = xdb_sql_delete ((xdb_stmt_select_t*)pStmt);
			break;
		case XDB_STMT_BEGIN:
			rc = xdb_begin2 (pConn, false);
			break;
		case XDB_STMT_COMMIT:
			rc = xdb_commit (pConn);
			break;
		case XDB_STMT_ROLLBACK:
			rc = xdb_rollback (pConn);
			break;
		case XDB_STMT_CREATE_DB:
			rc = xdb_create_db ((xdb_stmt_db_t*)pStmt);
			break;
		case XDB_STMT_DROP_DB:
			{
				xdb_stmt_db_t *pStmtDb = (xdb_stmt_db_t*)pStmt;
				rc = xdb_drop_db (pStmtDb->pConn, pStmtDb->pDbm);
			}
			break;
		case xdb_stmt_free_DB:
			{
				xdb_stmt_db_t *pStmtDb = (xdb_stmt_db_t*)pStmt;
				rc = xdb_close_db (pStmtDb->pConn, pStmtDb->pDbm);
			}
			break;
		#ifdef XDB_EANBLE_SERVER
		case XDB_STMT_CREATE_SVR:
			rc = xdb_create_server ((xdb_stmt_svr_t*)pStmt);
			break;
		#endif
		case XDB_STMT_USE_DB:
			rc = xdb_use_db ((xdb_stmt_db_t*)pStmt);
			if (XDB_OK == rc) {
				pConn->conn_msg.len = strlen (XDB_OBJ_NAME(pConn->pCurDbm));
				memcpy (pConn->conn_msg.msg, XDB_OBJ_NAME(pConn->pCurDbm), pConn->conn_msg.len+1);
				pConn->conn_msg.len_type = (XDB_RET_MSG<<28) | pConn->conn_msg.len;
			}
			break;
		case XDB_STMT_CREATE_TBL:
			rc = xdb_create_table ((xdb_stmt_tbl_t*)pStmt);
			break;
		case XDB_STMT_DROP_TBL:
			{
				xdb_stmt_tbl_t *pStmtTbl = (xdb_stmt_tbl_t*)pStmt;
				if (NULL != pStmtTbl->pTblm) {
					rc = xdb_drop_table (pStmtTbl->pTblm);
				}
			}
			break;
		case XDB_STMT_CREATE_IDX:
			rc = xdb_create_index ((xdb_stmt_idx_t*)pStmt, false);
			break;
		case XDB_STMT_DROP_IDX:
			rc = xdb_drop_index (((xdb_stmt_idx_t*)pStmt)->pIdxm);
			break;
		case XDB_STMT_OPEN_DB:
			rc = xdb_create_db ((xdb_stmt_db_t*)pStmt);
			break;
		case XDB_STMT_OPEN_DATADIR:
			rc = xdb_open_datadir ((xdb_stmt_db_t*)pStmt);
			break;
		case XDB_STMT_SET:
			rc = xdb_sql_set ((xdb_stmt_set_t*)pStmt);
			break;
		case XDB_STMT_EXPLAIN:
			{
				xdb_stmt_select_t *pStmtSel = (xdb_stmt_select_t*)pStmt;
				if (NULL != pStmtSel->pIdxm) {
					pConn->conn_msg.len = sprintf (pConn->conn_msg.msg, "Use INDEX %s", XDB_OBJ_NAME(pStmtSel->pIdxm));
				} else {
					pConn->conn_msg.len = sprintf (pConn->conn_msg.msg, "Scan Table");
				}
				pConn->conn_msg.len_type = (XDB_RET_MSG<<28) | pConn->conn_msg.len;
			}
			rc = XDB_OK;
			break;
		case XDB_STMT_SHOW_DB:
			pRes = xdb_exec (pStmt->pConn, "SELECT database,engine FROM system.databases");
			break;
		case XDB_STMT_SHOW_TBL:
			pRes = xdb_pexec (pStmt->pConn, "SELECT table,engine FROM system.tables WHERE database='%s'", XDB_OBJ_NAME(pConn->pCurDbm));
			break;
		case XDB_STMT_SHOW_CREATE_TBL:
			{
				xdb_stmt_select_t *pStmtSel = (xdb_stmt_select_t*)pStmt;
				pRes = xdb_pexec (pStmt->pConn, "SELECT schema FROM system.tables WHERE database='%s' and table='%s'", 
								XDB_OBJ_NAME(pStmtSel->pTblm->pDbm), XDB_OBJ_NAME(pStmtSel->pTblm));
			}
			break;
		case XDB_STMT_SHOW_COL:
			pRes = xdb_pexec (pStmt->pConn, "SELECT table,column,type,len FROM system.columns WHERE database='%s'", XDB_OBJ_NAME(pConn->pCurDbm));
			break;
		case XDB_STMT_SHOW_IDX:
			pRes = xdb_pexec (pStmt->pConn, "SELECT table,idx_key,type,col_list FROM system.indexes WHERE database='%s'", XDB_OBJ_NAME(pConn->pCurDbm));
			break;
		case XDB_STMT_DESC:
			{
				xdb_stmt_tbl_t *pStmtTbl = (xdb_stmt_tbl_t*)pStmt;
				pRes = xdb_pexec (pStmt->pConn, "SELECT column,type,len FROM system.columns WHERE database='%s' and table='%s'", 
								XDB_OBJ_NAME(pStmtTbl->pTblm->pDbm), XDB_OBJ_NAME(pStmtTbl->pTblm));
			}
			break;
		case XDB_STMT_SHELL:
			{
				char cur_db[XDB_NAME_LEN+2];
				xdb_strcpy (cur_db, xdb_curdb(pConn));
				rc = xdb_shell_loop (pStmt->pConn, NULL, xdb_curdb(pConn));
				// reover current db
				if (*cur_db != '\0') {
					pRes = xdb_pexec (pStmt->pConn, "USE %s", cur_db);
				}
			}
			break;
		case XDB_STMT_SOURCE:
			rc = xdb_source (pStmt->pConn, ((xdb_stmt_backup_t*)pStmt)->file);
			break;
		case XDB_STMT_DUMP_DB:
			{
				xdb_stmt_backup_t *pStmtBak = (xdb_stmt_backup_t*)pStmt;
				rc = xdb_dump (pStmtBak->pConn, pStmtBak->pDbm, pStmtBak->file, pStmtBak->bNoDrop, pStmtBak->bNoCreate, pStmtBak->bNoData);
				// type will be overwritten by xdb_dump
			}
			pStmt->stmt_type = XDB_STMT_DUMP_DB;
			break;
		case XDB_STMT_HELP:
			break;
		default:
			rc = XDB_E_STMT;
			break;
		}

		if (pConn->bAutoTrans) {
			xdb_commit (pStmt->pConn);
		}

		// extra msg set
		if (xdb_unlikely (pConn->conn_msg.len > 0)) {
			pRes->data_len = sizeof (xdb_msg_t) + pConn->conn_msg.len + 1;
			pRes->row_data = (uintptr_t)pConn->conn_msg.msg;
		}
	}

	pRes->stmt_type = pStmt->stmt_type;
	return pRes;	
}

xdb_res_t*
xdb_exec2 (xdb_conn_t *pConn, const char *sql, int len)
{
	xdb_assert(NULL != pConn);
	xdb_assert(NULL != sql);
	xdb_res_t	*pRes;

#ifdef XDB_SQL_DBG
	printf ("Run SQL: %s\n", sql);
#endif

	if (xdb_unlikely (0 == len)) {
		len = strlen (sql);
	}

#ifdef XDB_EANBLE_SERVER
	bool bLocal = true;
	if (xdb_unlikely (pConn->conn_client)) {
		// source | dump | shell | help
		bLocal = xdb_is_local_stmt (sql);
	}
	if (xdb_unlikely (!bLocal)) {
		pRes = xdb_exec_client ();
	} else 
#endif
	{
		pRes = &pConn->conn_res;
		pRes->status = 0;
		xdb_stmt_t *pStmt;
		if (xdb_likely (NULL == pConn->pNxtSql)) {
			if (pConn->sqlbuf_len <= len) {
				int blen = (len + 1023) & (~1023);
				char *pBuf = xdb_realloc (pConn->pSql == pConn->sql_buf? NULL : pConn->pSql, blen);
				XDB_EXPECT(pBuf != NULL, XDB_E_MEMORY, "Can't alloc memory");
				pConn->sqlbuf_len = blen;
				pConn->pSql = pBuf;
			}
			while (len && isspace((int)sql[len-1])) {
				len--;
			}
			memcpy (pConn->pSql, sql, len);
			pConn->pSql[len] = '\0';
			pConn->pNxtSql = pConn->pSql;
			pStmt = xdb_sql_parse (pConn, &pConn->pNxtSql, false);
		} else {
			pStmt = xdb_sql_parse2 (pConn, sql, false);
		}
		if (xdb_likely(NULL != pStmt)) {
			pRes = xdb_stmt_exec (pStmt);
			if (pRes->row_count) {
				xdb_queryRes_t *pQueryRes = (void*)pRes - XDB_OFFSET(xdb_queryRes_t, res);
				pQueryRes->pStmt = (xdb_stmt_select_t*)pStmt;
			} else {
				xdb_stmt_free (pStmt);
			}
		} else {
			pRes->stmt_type = 0;
		}
		if (NULL != pConn->pNxtSql) {
			pRes->status |= XDB_STATUS_MORE_RESULTS;
		}
	}

error:
	// copy server/stmt status
	pConn->status = pRes->status;
	return pRes;
}

xdb_res_t*
xdb_exec (xdb_conn_t *pConn, const char *sql)
{
	return xdb_exec2 (pConn, sql, 0);
}

xdb_res_t*
xdb_next_result (xdb_conn_t *pConn)
{
	xdb_res_t	*pRes;

	if (xdb_unlikely (pConn->conn_client)) {
	#ifdef XDB_ENABLE_SERVER
		if (pConn->status & XDB_STATUS_MORE_RESULTS) {
			pRes = xdb_fetch_res_sock (pConn);
		} else {
			return NULL;
		}
	#else
		return NULL;
	#endif // XDB_ENABLE_SERVER
	} else {
		char *pSql = pConn->pNxtSql;
		if (NULL == pSql) {
			return NULL;
		}
		pRes = &pConn->conn_res;
		xdb_stmt_t *pStmt = xdb_sql_parse (pConn, &pConn->pNxtSql, false);
		if (xdb_likely(NULL != pStmt)) {
			pRes = xdb_stmt_exec (pStmt);
			if (pRes->row_count) {
				xdb_queryRes_t *pQueryRes = (void*)pRes - XDB_OFFSET(xdb_queryRes_t, res);
				pQueryRes->pStmt = (xdb_stmt_select_t*)pStmt;
			} else {
				xdb_stmt_free (pStmt);
			}
		}
		if (NULL != pConn->pNxtSql) {
			pRes->status |= XDB_STATUS_MORE_RESULTS;
		}
	}

	pConn->status = pRes->status;
	return pRes;
}

bool
xdb_more_result (xdb_conn_t *pConn)
{
	return pConn->status & XDB_STATUS_MORE_RESULTS;
}

xdb_res_t*
xdb_pexec (xdb_conn_t *pConn, const char *sql, ...)
{
    va_list 	vargs;

	XDB_BUF_DEF(pSql,4096);

	va_start(vargs, sql);
	int len = vsnprintf ((char*)pSql, pSql_size-1, sql, vargs);
	va_end(vargs);
	
	if (len >= pSql_size) {
		XDB_BUF_ALLOC(pSql,len+1);
		XDB_EXPECT (NULL != pSql, XDB_E_MEMORY, "Can't alloc memory");
		va_start(vargs, sql);
		len = vsnprintf (pSql, len, sql, vargs);
		va_end(vargs);
	}
	pSql[pSql_size-1] = '\0';
	
	xdb_res_t *pRes = xdb_exec (pConn, pSql);

	XDB_BUF_FREE(pSql);

	return pRes;

error:
	return &pConn->conn_res;
}

XDB_STATIC int 
xdb_str_escape (char *dest, const char *src, int len)
{
	char *tmp = dest, ch;

	while ((ch = *src++) && len--) {
		switch (ch) {
		case '\'':
		case '"':
		case '`':
		case '\\':
		case '%':
		case '_':
			*dest++ = '\\';
			*dest++ = ch;
			break;
		case '\n':
			*dest++ = '\\';
			*dest++ = 'n';
			break;
		case '\r':
			*dest++ = '\\';
			*dest++ = 'r';
			break;
		case '\t':
			*dest++ = '\t';
			break;
		default:
			*dest++ = ch;
			break;
		}
	}

	return dest - tmp;
}

XDB_STATIC int 
xdb_exec_out (xdb_conn_t *pConn, const char *sql, int len)
{
	int64_t ts = xdb_timestamp_us();
	xdb_res_t *pRes = xdb_exec2 (pConn, sql, len);
	ts = xdb_timestamp_us() - ts;
	do {
		switch (pConn->res_format) {
		#ifdef XDB_ENABLE_SERVER
		case XDB_FMT_NATIVELE:
		case XDB_FMT_NATIVEBE:
			xdb_native_out (pConn, pRes);
			break;
		#endif
		#ifdef XDB_ENABLE_JSON
		case XDB_FMT_JSON:
			xdb_json_out (pConn, pRes);
			break;
		#endif
		case XDB_FMT_TABLE:
		default:
			{
				if (pRes->errcode > 0) {
					fprintf (pConn->conn_stdout, "ERROR %d: %s\n\n", pRes->errcode, xdb_errmsg(pRes));
				} else {
					if (pRes->row_count) {
						xdb_output_table (pConn, pRes);
					}
					if (pRes->meta_len > 0) {
						fprintf (pConn->conn_stdout, "%"PRId64" row%s in set (%d.%03d ms)\n\n", pRes->row_count, pRes->row_count>1?"s":"", (int)(ts/1000), (int)(ts%1000));
					} else if (1 == pRes->stmt_type) {
						fprintf (pConn->conn_stdout, "Database changed\n\n");
					} else if (pRes->stmt_type < 200) {
						if (isatty(STDIN_FILENO)) {
							if (pRes->data_len) {
								fprintf (pConn->conn_stdout, "%s\n", xdb_errmsg(pRes));
							}
							fprintf (pConn->conn_stdout, "Query OK, %" PRId64 " row%s affected (%d.%03d ms)\n\n", pRes->affected_rows, pRes->affected_rows>1?"s":"", (int)(ts/1000), (int)(ts%1000));
						}
					}
				}
				fflush (pConn->conn_stdout);
			}
			break;
		}
	
		xdb_free_result (pRes);
	} while ((pRes = xdb_next_result(pConn)) != NULL);

	return XDB_OK;
}

xdb_stmt_t*
xdb_stmt_prepare (xdb_conn_t *pConn, const char *sql)
{
	return xdb_sql_parse2 (pConn, sql, true);
}

xdb_ret
xdb_bind_int64 (xdb_stmt_t *pStmt, uint16_t para_id, int64_t val)
{
	xdb_stmt_select_t *pStmtSel;
	switch (pStmt->stmt_type) {
	case XDB_STMT_SELECT:
	case XDB_STMT_UPDATE:
	case XDB_STMT_DELETE:
		pStmtSel = (void*)pStmt;
		if (xdb_unlikely (para_id > pStmtSel->bind_count)) {
			return -XDB_E_PARAM;
		}
		pStmtSel->pBind[para_id-1]->val.ival = val;
		break;
	default:
		break;
	}
	return XDB_OK;
}

xdb_ret
xdb_bind_int (xdb_stmt_t *pStmt, uint16_t para_id, int val)
{
	return xdb_bind_int64 (pStmt, para_id, val);
}

xdb_ret
xdb_bind_double (xdb_stmt_t *pStmt, uint16_t para_id, double val)
{
	xdb_stmt_select_t *pStmtSel;
	switch (pStmt->stmt_type) {
	case XDB_STMT_SELECT:
	case XDB_STMT_UPDATE:
	case XDB_STMT_DELETE:
		pStmtSel = (void*)pStmt;
		if (xdb_unlikely (para_id >= pStmtSel->bind_count)) {
			return -XDB_E_PARAM;
		}
		pStmtSel->pBind[para_id-1]->val.fval = val;
		break;
	default:
		break;
	}
	return XDB_OK;
}

xdb_ret
xdb_bind_float (xdb_stmt_t *pStmt, uint16_t para_id, float val)
{
	return xdb_bind_double (pStmt, para_id, val);
}

xdb_ret
xdb_bind_str2 (xdb_stmt_t *pStmt, uint16_t para_id, const char *str, int len)
{
	xdb_stmt_select_t *pStmtSel;
	switch (pStmt->stmt_type) {
	case XDB_STMT_SELECT:
	case XDB_STMT_UPDATE:
	case XDB_STMT_DELETE:
		pStmtSel = (void*)pStmt;
		if (xdb_unlikely (para_id >= pStmtSel->bind_count)) {
			return -XDB_E_PARAM;
		}
		pStmtSel->pBind[para_id-1]->val.str.ptr = (char*)str;
		pStmtSel->pBind[para_id-1]->val.str.len = len > 0 ? len : strlen (str);
		break;
	default:
		break;
	}
	return XDB_OK;
}

xdb_ret
xdb_bind_str (xdb_stmt_t *pStmt, uint16_t id, const char *str)
{
	return xdb_bind_str2 (pStmt, id, str, 0);
}

// TBD
int
xdb_stmt_bind (xdb_stmt_t *pStmt, ...);

void xdb_stmt_close (xdb_stmt_t *pStmt)
{
	xdb_stmt_free (pStmt);
	xdb_free (pStmt);
}