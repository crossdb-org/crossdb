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

XDB_STATIC xdb_res_t*
xdb_stmt_vbexec2 (xdb_stmt_t *pStmt, va_list ap);

XDB_STATIC int 
xdb_sql_set (xdb_stmt_set_t *pStmt)
{
	xdb_conn_t		*pConn = pStmt->pConn;

	if (NULL != pStmt->datadir) {
		xdb_strcpy (s_xdb_datadir, pStmt->datadir);
	}

	if (NULL != pStmt->svrid) {
		xdb_strcpy (s_xdb_svrid, pStmt->svrid);
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
	xdb_tblm_t		*pTblm;

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
		case XDB_STMT_INSERT:
		case XDB_STMT_REPLACE:
		case XDB_STMT_DELETE:
			if (xdb_unlikely (!pConn->bInTrans)) {
				xdb_begin2 (pConn, pConn->bAutoCommit);
			}
			if (xdb_unlikely ((XDB_STMT_INSERT == pStmt->stmt_type) || (XDB_STMT_REPLACE == pStmt->stmt_type))) {
				pTblm = ((xdb_stmt_insert_t*)pStmt)->pTblm;
			} else {
				pTblm = ((xdb_stmt_select_t*)pStmt)->pTblm;
			}
			if (xdb_unlikely (!(pTblm->bMemory && pConn->bAutoTrans))) {
				xdb_wrlock_table (pConn, pTblm);
			}
			if (xdb_likely (XDB_STMT_UPDATE == pStmt->stmt_type)) {
				pRes->affected_rows = xdb_sql_update ((xdb_stmt_select_t*)pStmt);
			} else if ((XDB_STMT_INSERT == pStmt->stmt_type) || (XDB_STMT_REPLACE == pStmt->stmt_type)) {
				pRes->affected_rows = xdb_sql_insert ((xdb_stmt_insert_t*)pStmt);
			} else {
				pRes->affected_rows = xdb_sql_delete ((xdb_stmt_select_t*)pStmt);
			}
			if (xdb_likely (pConn->bAutoTrans)) {
				if (xdb_likely (pTblm->bMemory)) {
					pConn->bInTrans = false;
					pConn->bAutoTrans = false;			
				} else {
					xdb_commit (pConn);
				}
			}
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
				if (NULL != pStmtDb->pDbm) {
					xdb_commit (pConn);
					rc = xdb_drop_db (pStmtDb->pConn, pStmtDb->pDbm);
				}
			}
			break;
		case XDB_STMT_CLOSE_DB:
			{
				xdb_stmt_db_t *pStmtDb = (xdb_stmt_db_t*)pStmt;
				rc = xdb_close_db (pStmtDb->pConn, pStmtDb->pDbm);
			}
			break;
		#if (XDB_ENABLE_SERVER == 1)
		case XDB_STMT_CREATE_SVR:
			rc = xdb_create_server ((xdb_stmt_svr_t*)pStmt);
			break;
		case XDB_STMT_DROP_SVR:
			rc = xdb_drop_server ((xdb_stmt_svr_t*)pStmt);
			break;
		#endif

		#if (XDB_ENABLE_PUBSUB == 1)
		case XDB_STMT_CREATE_PUB:
			rc = xdb_create_pub ((xdb_stmt_pub_t*)pStmt);
			break;

		case XDB_STMT_CREATE_REPLICA:
			rc = xdb_create_replica ((xdb_stmt_replica_t*)pStmt);
			break;

		case XDB_STMT_SUBSCRIBE:
			rc = xdb_subscribe ((xdb_stmt_subscribe_t*)pStmt);
			break;
		#endif
		
		case XDB_STMT_USE_DB:
			rc = xdb_use_db ((xdb_stmt_db_t*)pStmt);
			if (XDB_OK == rc) {
				pConn->conn_msg.len = strlen (XDB_OBJ_NAME(pConn->pCurDbm));
				memcpy (pConn->conn_msg.msg, XDB_OBJ_NAME(pConn->pCurDbm), pConn->conn_msg.len+1);
				pConn->conn_msg.len_type = (XDB_RET_MSG<<28) | (pConn->conn_msg.len + 7);
			}
			break;
		case XDB_STMT_CREATE_TBL:
			rc = xdb_create_table ((xdb_stmt_tbl_t*)pStmt);
			break;
		case XDB_STMT_DROP_TBL:
			{
				xdb_stmt_tbl_t *pStmtTbl = (xdb_stmt_tbl_t*)pStmt;
				if (NULL != pStmtTbl->pTblm) {
					xdb_commit (pConn);
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
		case XDB_STMT_CREATE_TRIG:
			rc = xdb_create_trigger ((xdb_stmt_trig_t*)pStmt);
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
				xdb_reftbl_t *pRefTbl = &pStmtSel->ref_tbl[0];
				if (pRefTbl->bUseIdx) {
					pConn->conn_msg.len = sprintf (pConn->conn_msg.msg, "Query table '%s' with INDEX ", XDB_OBJ_NAME(pRefTbl->pRefTblm));
					for (int i = 0; i < pRefTbl->or_count; ++i) {
						xdb_idxfilter_t 	*pIdxFilter = pRefTbl->or_list[i].pIdxFilter;
						xdb_idxm_t	*pIdxm = pIdxFilter ? pIdxFilter->pIdxm : NULL;
						pConn->conn_msg.len += sprintf (pConn->conn_msg.msg+pConn->conn_msg.len, " %s", XDB_OBJ_NAME(pIdxm));
					}
				} else {
					pConn->conn_msg.len = sprintf (pConn->conn_msg.msg, "Scan table '%s' Table", XDB_OBJ_NAME(pRefTbl->pRefTblm));
				}
				pConn->conn_msg.len_type = (XDB_RET_MSG<<28) | (pConn->conn_msg.len + 7);
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
		case XDB_STMT_SHOW_SVR:
			pRes = xdb_pexec (pStmt->pConn, "SELECT * FROM system.servers");
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
				char cur_db[XDB_NAME_LEN+2], prompt[XDB_NAME_LEN+4];
				xdb_strcpy (cur_db, xdb_curdb(pConn));
				if (NULL != pStmt->pArg) {
					xdb_strcpy (prompt, pStmt->pArg);
				}
				rc = xdb_shell_loop (pStmt->pConn, pStmt->pArg?prompt:NULL, xdb_curdb(pConn), true);
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
			pStmt->pSql = NULL;
			break;
		case XDB_STMT_DUMP_WAL:
			xdb_wal_dump (pConn->pCurDbm);
			break;
		case XDB_STMT_FLUSH_DB:
			xdb_flush_db (pConn->pCurDbm, 0);
			break;
		case XDB_STMT_REPAIR_DB:
			xdb_repair_db (pConn->pCurDbm, 0);
			break;
		case XDB_STMT_AUDIT_MARK:
			xdb_audit_mark ((xdb_stmt_select_t*)pStmt);
			break;
		case XDB_STMT_AUDIT_SWEEP:
			xdb_audit_sweep ((xdb_stmt_select_t*)pStmt);
			break;
		case XDB_STMT_HELP:
			break;
		default:
			rc = XDB_E_STMT;
			break;
		}

		// extra msg set
		if (xdb_unlikely (pConn->conn_msg.len > 0)) {
			pRes->data_len = sizeof (xdb_msg_t) - sizeof(pConn->conn_msg.msg) + pConn->conn_msg.len + 1;
			pRes->row_data = (uintptr_t)pConn->conn_msg.msg;
		} else if (rc != XDB_OK) {
			XDB_SETERR (rc, "Failed");
		}
	}

	pRes->stmt_type = pStmt->stmt_type;
	return pRes;	
}

XDB_STATIC xdb_stmt_t*
xdb_stmt_parse (xdb_conn_t *pConn, const char *sql, int len)
{
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
		pStmt = xdb_sql_parse_alloc (pConn, sql, false);
	}
	return pStmt;
error:
	return NULL;
}

xdb_res_t*
xdb_exec2 (xdb_conn_t *pConn, const char *sql, int len)
{
	xdb_assert(NULL != pConn);
	xdb_assert(NULL != sql);
	xdb_res_t	*pRes;

#ifdef XDB_SQL_DBG
	xdb_print ("Run SQL: %s\n", sql);
#endif

	if (xdb_unlikely (0 == len)) {
		len = strlen (sql);
	}

#if (XDB_ENABLE_SERVER == 1)
	bool bLocal = true;
	if (xdb_unlikely (pConn->conn_client)) {
		// source | dump | shell | help
		bLocal = xdb_is_local_stmt (sql);
	}
	if (xdb_unlikely (!bLocal)) {
		pRes = xdb_exec_client (pConn, sql, len);
	} else 
#endif
	{
		pRes = &pConn->conn_res;
		pRes->status = 0;
		xdb_stmt_t *pStmt = xdb_stmt_parse (pConn, sql, len);
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
	#if (XDB_ENABLE_SERVER == 1)
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

xdb_res_t*
xdb_vbexec_cb (xdb_conn_t *pConn, xdb_row_callback callback, void *pArg, const char *sql, va_list ap)
{
	xdb_stmt_t *pStmt = xdb_stmt_parse (pConn, sql, strlen(sql));
	if (NULL != pStmt) {
		if (XDB_STMT_SELECT == pStmt->stmt_type) {
			xdb_stmt_select_t *pStmtSel = (xdb_stmt_select_t*)pStmt;
			pStmtSel->callback 	= callback;
			pStmtSel->pCbArg	= pArg;
		}
		return xdb_stmt_vbexec2 (pStmt, ap);
	} else {
		return &pConn->conn_res;
	}
}

xdb_res_t*
xdb_bexec_cb (xdb_conn_t *pConn, xdb_row_callback callback, void *pArg, const char *sql, ...)
{
	va_list ap;
	va_start (ap, sql);
	xdb_res_t* pRes = xdb_vbexec_cb (pConn, callback, pArg, sql, ap);
	va_end (ap);
	return pRes;
}

xdb_res_t*
xdb_vbexec (xdb_conn_t *pConn, const char *sql, va_list ap)
{
	xdb_stmt_t *pStmt = xdb_stmt_parse (pConn, sql, strlen(sql));
	if (NULL != pStmt) {
		return xdb_stmt_vbexec (pStmt, ap);
	} else {
		return &pConn->conn_res;
	}
}

xdb_res_t*
xdb_bexec (xdb_conn_t *pConn, const char *sql, ...)
{
	if (xdb_unlikely (pConn->conn_client)) {
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
	}

	va_list ap;
	va_start (ap, sql);
	xdb_res_t* pRes = xdb_vbexec (pConn, sql, ap);
	va_end (ap);
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
		#if (XDB_ENABLE_SERVER == 1)
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
					xdb_fprintf (pConn->conn_stdout, "ERROR %d: %s\n\n", pRes->errcode, xdb_errmsg(pRes));
				} else {
					if (pRes->row_count) {
						xdb_output_table (pConn, pRes);
					}
					if (pRes->meta_len > 0) {
						xdb_fprintf (pConn->conn_stdout, "%"PRId64" row%s in set (%d.%03d ms)\n\n", pRes->row_count, pRes->row_count>1?"s":"", (int)(ts/1000), (int)(ts%1000));
					} else if (1 == pRes->stmt_type) {
						xdb_fprintf (pConn->conn_stdout, "Database changed\n\n");
					} else if (pRes->stmt_type < 200) {
						if (isatty(STDIN_FILENO)) {
							if (pRes->data_len) {
								xdb_fprintf (pConn->conn_stdout, "%s\n", xdb_errmsg(pRes));
							}
							xdb_fprintf (pConn->conn_stdout, "Query OK, %" PRId64 " row%s affected (%d.%03d ms)\n\n", pRes->affected_rows, pRes->affected_rows>1?"s":"", (int)(ts/1000), (int)(ts%1000));
						}
					}
				}
				xdb_fflush (pConn->conn_stdout);
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
	return xdb_sql_parse_alloc (pConn, sql, true);
}

xdb_ret
xdb_bind_int64 (xdb_stmt_t *pStmt, uint16_t para_id, int64_t val)
{
	switch (pStmt->stmt_type) {
	case XDB_STMT_SELECT:
	case XDB_STMT_UPDATE:
	case XDB_STMT_DELETE:
		{
			xdb_stmt_select_t *pStmtSel = (void*)pStmt;
			if (xdb_unlikely (--para_id >= pStmtSel->bind_count)) {
				return -XDB_E_PARAM;
			}
			pStmtSel->pBind[para_id]->ival = val;
		}
		break;
	case XDB_STMT_INSERT:
	case XDB_STMT_REPLACE:
		{
			xdb_stmt_insert_t 	*pStmtIns = (void*)pStmt;
			if (xdb_unlikely (--para_id > pStmtIns->bind_count)) {
				return -XDB_E_PARAM;
			}
			xdb_field_t	*pField = pStmtIns->pBind[para_id];
			xdb_fld_setInt (pStmtIns->pBindRow[para_id] + pField->fld_off, pField->fld_type, val);
		}
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
	switch (pStmt->stmt_type) {
	case XDB_STMT_SELECT:
	case XDB_STMT_UPDATE:
	case XDB_STMT_DELETE:
		{
			xdb_stmt_select_t *pStmtSel = (void*)pStmt;
			if (xdb_unlikely (--para_id >= pStmtSel->bind_count)) {
				return -XDB_E_PARAM;
			}
			pStmtSel->pBind[para_id]->fval = val;
		}
		break;
	case XDB_STMT_INSERT:
	case XDB_STMT_REPLACE:
		{
			xdb_stmt_insert_t	*pStmtIns = (void*)pStmt;
			if (xdb_unlikely (--para_id > pStmtIns->bind_count)) {
				return -XDB_E_PARAM;
			}
			xdb_field_t *pField = pStmtIns->pBind[para_id];
			xdb_fld_setFloat (pStmtIns->pBindRow[para_id] + pField->fld_off, pField->fld_type, val);
		}
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
	xdb_ret rc = XDB_OK;
	switch (pStmt->stmt_type) {
	case XDB_STMT_SELECT:
	case XDB_STMT_UPDATE:
	case XDB_STMT_DELETE:
		{
			xdb_stmt_select_t *pStmtSel = (void*)pStmt;
			if (xdb_unlikely (--para_id >= pStmtSel->bind_count)) {
				return -XDB_E_PARAM;
			}
			pStmtSel->pBind[para_id]->str.str = (char*)str;
			pStmtSel->pBind[para_id]->str.len = len > 0 ? len : strlen (str);
		}
		break;
	case XDB_STMT_INSERT:
	case XDB_STMT_REPLACE:
		{
			xdb_stmt_insert_t	*pStmtIns = (void*)pStmt;
			if (xdb_unlikely (--para_id > pStmtIns->bind_count)) {
				return -XDB_E_PARAM;
			}
			xdb_field_t *pField = pStmtIns->pBind[para_id];
			rc = xdb_fld_setStr (pStmt->pConn, pField, pStmtIns->pBindRow[para_id], str, len);
		}
		break;
	default:
		break;
	}
	return rc;
}

xdb_ret
xdb_bind_str (xdb_stmt_t *pStmt, uint16_t para_id, const char *str)
{
	return xdb_bind_str2 (pStmt, para_id, str, 0);
}

xdb_ret
xdb_bind_blob (xdb_stmt_t *pStmt, uint16_t para_id, const void *blob, int len)
{
	return xdb_bind_str2 (pStmt, para_id, blob, 0);
}

XDB_STATIC xdb_res_t*
xdb_stmt_vbexec2 (xdb_stmt_t *pStmt, va_list ap)
{
	xdb_conn_t	*pConn = pStmt->pConn;
	char		*str;
	int 		len = 0;

	switch (pStmt->stmt_type) {
	case XDB_STMT_SELECT:
	case XDB_STMT_UPDATE:
	case XDB_STMT_DELETE:
		{
			xdb_stmt_select_t *pStmtSel = (void*)pStmt;
			for (int i = 0; i < pStmtSel->bind_count; ++i) {
				xdb_value_t *pVal = pStmtSel->pBind[i];
				switch (pVal->fld_type) {
				case XDB_TYPE_INT:
				case XDB_TYPE_SMALLINT:
				case XDB_TYPE_TINYINT:
				case XDB_TYPE_BOOL:
					pVal->ival = va_arg (ap, int);
					break;
				case XDB_TYPE_UINT:
				case XDB_TYPE_USMALLINT:
				case XDB_TYPE_UTINYINT:
					pVal->uval = va_arg (ap, uint32_t);
					break;
				case XDB_TYPE_BIGINT:
				case XDB_TYPE_TIMESTAMP:
					pVal->ival = va_arg (ap, int64_t);
					break;
				case XDB_TYPE_UBIGINT:
					pVal->uval = va_arg (ap, uint64_t);
					break;
				case XDB_TYPE_FLOAT:
				case XDB_TYPE_DOUBLE:
					pVal->fval = va_arg (ap, double);
					break;
				case XDB_TYPE_CHAR:
				case XDB_TYPE_VCHAR:
				case XDB_TYPE_JSON:
					pVal->str.str = va_arg (ap, char *);
					pVal->str.len = strlen (pVal->str.str);
					break;
				case XDB_TYPE_BINARY:
				case XDB_TYPE_VBINARY:
					pVal->str.len = va_arg (ap, int);
					pVal->str.str = va_arg (ap, char *);
					break;
				case XDB_TYPE_INET:
					pVal->inet = *(xdb_inet_t*)va_arg (ap, void *);
					break;
				case XDB_TYPE_MAC:
					pVal->mac = *(xdb_mac_t*)va_arg (ap, void *);
					break;
				default:
					break;
				}
			}
		}
		break;

	case XDB_STMT_INSERT:
	case XDB_STMT_REPLACE:
		{
			xdb_stmt_insert_t *pStmtIns = (void*)pStmt;
			for (int i = 0; i < pStmtIns->bind_count; ++i) {
				xdb_field_t *pFld = pStmtIns->pBind[i];
				void		*pAddr = pStmtIns->pBindRow[i] + pFld->fld_off;
				switch (pFld->fld_type) {
				case XDB_TYPE_INT:
					*(int32_t*)pAddr = va_arg (ap, int);
					break;
				case XDB_TYPE_UINT:
					*(uint32_t*)pAddr = va_arg (ap, uint32_t);
					break;
				case XDB_TYPE_SMALLINT:
				case XDB_TYPE_USMALLINT:
					*(int16_t*)pAddr = va_arg (ap, int);
					break;
				case XDB_TYPE_TINYINT:
				case XDB_TYPE_UTINYINT:
				case XDB_TYPE_BOOL:
					*(int8_t*)pAddr = va_arg (ap, int);
					break;
				case XDB_TYPE_BIGINT:
				case XDB_TYPE_TIMESTAMP:
					*(int64_t*)pAddr = va_arg (ap, int64_t);
					break;
				case XDB_TYPE_UBIGINT:
					*(uint64_t*)pAddr = va_arg (ap, uint64_t);
					break;
				case XDB_TYPE_FLOAT:
					*(float*)pAddr = va_arg (ap, double);
					break;
				case XDB_TYPE_DOUBLE:
					*(double*)pAddr = va_arg (ap, double);
					break;
				case XDB_TYPE_CHAR:
					str = va_arg (ap, char *);
					len = strlen (str);
					if (xdb_unlikely (len > pFld->fld_len)) {
						XDB_SETERR(XDB_E_PARAM, "Field '%s' max len %d < input %d", XDB_OBJ_NAME(pFld), pFld->fld_len, len);
						return &pConn->conn_res;
					}
					*(uint16_t*)(pAddr - 2) = len;
					memcpy (pAddr, str, len + 1);
					break;
				case XDB_TYPE_BINARY:
					len = va_arg (ap, int);
					str = va_arg (ap, char *);
					if (xdb_unlikely (len > pFld->fld_len)) {
						XDB_SETERR(XDB_E_PARAM, "Field '%s' max len %d < input %d", XDB_OBJ_NAME(pFld), pFld->fld_len, len);
						return &pConn->conn_res;
					}
					*(uint16_t*)(pAddr - 2) = len;
					memcpy (pAddr, str, len);
					break;
				case XDB_TYPE_VBINARY:
					len = va_arg (ap, int);
					// fall through
				case XDB_TYPE_VCHAR:
				case XDB_TYPE_JSON:
					str = va_arg (ap, char *);
					if (xdb_unlikely (NULL == str)) {
						len = 0;
					} else if ((XDB_TYPE_VCHAR == pFld->fld_type) || (XDB_TYPE_JSON == pFld->fld_type)) {
						len = strlen (str);
					}
					if (xdb_unlikely (len > pFld->fld_len)) {
						XDB_SETERR(XDB_E_PARAM, "Field '%s' max len %d < input %d", XDB_OBJ_NAME(pFld), pFld->fld_len, len);
						return &pConn->conn_res;
					}
					{
						xdb_str_t *pVstr = pStmtIns->pBindRow[i] + pStmtIns->pTblm->row_size;
						xdb_str_t *pStr = &pVstr[pFld->fld_vid];
						pStr->len = len;
						pStr->str = str;
					}
					break;
				case XDB_TYPE_INET:
					memcpy (pAddr, va_arg (ap, void *), sizeof (xdb_inet_t));
					break;
				case XDB_TYPE_MAC:
					memcpy (pAddr, va_arg (ap, void *), sizeof (xdb_mac_t));
					break;
				}
			}
		}
		break;

	default:
		break;
	}

	return xdb_stmt_exec (pStmt);

//error:
//	return &pConn->conn_res;	
}

xdb_res_t*
xdb_stmt_vbexec (xdb_stmt_t *pStmt, va_list ap)
{
	if (XDB_STMT_SELECT == pStmt->stmt_type) {
		((xdb_stmt_select_t*)pStmt)->callback	= NULL;
	}	
	return xdb_stmt_vbexec2 (pStmt, ap);
}

xdb_res_t*
xdb_stmt_bexec (xdb_stmt_t *pStmt, ...)
{
	va_list ap;
	va_start (ap, pStmt);
	xdb_res_t* pRes = xdb_stmt_vbexec (pStmt, ap);
	va_end (ap);
	return pRes;
}

xdb_res_t*
xdb_stmt_vbexec_cb (xdb_stmt_t *pStmt, xdb_row_callback callback, void *pArg, va_list ap)
{
	if (XDB_STMT_SELECT == pStmt->stmt_type) {
		xdb_stmt_select_t *pStmtSel = (xdb_stmt_select_t*)pStmt;
		pStmtSel->callback	= callback;
		pStmtSel->pCbArg	= pArg;
	}
	return xdb_stmt_vbexec2 (pStmt, ap);
}

xdb_res_t*
xdb_stmt_bexec_cb (xdb_stmt_t *pStmt, xdb_row_callback callback, void *pArg, ...)
{
	va_list ap;
	va_start (ap, pArg);
	xdb_res_t* pRes = xdb_stmt_vbexec_cb (pStmt, callback, pArg, ap);
	va_end (ap);
	return pRes;
}

xdb_ret
xdb_clear_bindings (xdb_stmt_t *pStmt)
{
	return XDB_OK;
}

void xdb_stmt_close (xdb_stmt_t *pStmt)
{
	xdb_stmt_free (pStmt);
	xdb_free (pStmt);
}
