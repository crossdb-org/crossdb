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

XDB_STATIC void 
xdb_res2sql (FILE *pFile, xdb_res_t *pRes, const char *tbl_name, char *buf, int len)
{
	xdb_row_t	*pRow;
	xdb_meta_t	*pMeta = xdb_fetch_meta (pRes);	
	xdb_col_t	**pCol = (xdb_col_t**)pMeta->col_list;

	while (NULL != (pRow = xdb_fetch_row (pRes))) {
		int len = 0;
		for (int i = 0; i < pMeta->col_count; ++i) {
			void *pVal = (void*)pRow[i];
			if (NULL == pVal) {
				len += sprintf (buf+len, "NULL,");
				continue;
			}
			switch (pCol[i]->col_type) {
			case XDB_TYPE_INT:
				len += sprintf (buf+len, "%d,", *(int32_t*)pVal);
				break;
			case XDB_TYPE_BOOL:
				len += sprintf (buf+len, "%s,", *(int8_t*)pVal ? "true" : "false");
				break;
			case XDB_TYPE_TINYINT:
				len += sprintf (buf+len, "%d,", *(int8_t*)pVal);
				break;
			case XDB_TYPE_SMALLINT:
				len += sprintf (buf+len, "%d,", *(int16_t*)pVal);
				break;
			case XDB_TYPE_BIGINT:
				len += sprintf (buf+len, "%"PRIi64",", *(int64_t*)pVal);
				break;
			case XDB_TYPE_FLOAT:
				len += sprintf (buf+len, "%f,", *(float*)pVal);
				break;
			case XDB_TYPE_DOUBLE:
				len += sprintf (buf+len, "%f,", *(double*)pVal);
				break;
			case XDB_TYPE_CHAR:
			case XDB_TYPE_VCHAR:
				*(buf + len++) = '\'';
				len += xdb_str_escape (buf+len, (char*)pVal, *(uint16_t*)(pVal-2));
				*(buf + len++) = '\'';
				*(buf + len++) = ',';
				break;
			case XDB_TYPE_BINARY:
			case XDB_TYPE_VBINARY:
				*(buf + len++) = 'X';
				*(buf + len++) = '\'';
				for (int h = 0; h < *(uint16_t*)(pVal-2); ++h) {
					*(buf + len++) = s_xdb_hex_2_str[((uint8_t*)pVal)[h]][0];
					*(buf + len++) = s_xdb_hex_2_str[((uint8_t*)pVal)[h]][1];
				}
				*(buf + len++) = '\'';
				*(buf + len++) = ',';
				break;
			}
		}
		buf[--len] = '\0';
		fprintf (pFile, "INSERT INTO %s VALUES (%s);\n", tbl_name, buf);
	}
}

XDB_STATIC int 
xdb_dump (xdb_conn_t* pConn, xdb_dbm_t *pDbm, const char *file, bool bNoDrop, bool bNoCreate, bool bNoData)
{
	int 		rc = -1;
	xdb_res_t 	*pRes = NULL;
	char 		*sql_buf = NULL;
	FILE 		*pFile = file != NULL ? fopen (file, "wt") : pConn->conn_stdout;

	XDB_EXPECT (NULL != pFile, XDB_E_NOTFOUND, "Can't open '%s', file doesn't exist", file);

	fprintf (pFile, "-- CrossDB Dump Database: %s\n", XDB_OBJ_NAME(pDbm));

	pRes = xdb_pexec (pConn, "SELECT table,schema FROM system.tables WHERE database='%s'", XDB_OBJ_NAME(pDbm));
	XDB_EXPECT(pRes->errcode == XDB_OK, pRes->errcode, "%s", xdb_errmsg(pRes));

	sql_buf = xdb_malloc (XDB_MAX_SQL_BUF);
	XDB_EXPECT (NULL != sql_buf, XDB_E_NOTFOUND, "Can't malloc memory");

	xdb_row_t *pRow;
	while (NULL != (pRow = xdb_fetch_row (pRes))) {
		const char *tbl_name = (char*)pRow[0];
		const char *tbl_schema = (char*)pRow[1];

		fprintf (pFile, "\n--\n");
		fprintf (pFile, "-- Dump Table: %s\n", tbl_name);
		fprintf (pFile, "--\n");

		if (!bNoDrop) {
			fprintf (pFile, "DROP TABLE IF EXISTS %s;\n", tbl_name);
		}
		if (!bNoCreate) {
			fprintf (pFile, "%s\n", tbl_schema);
		}
		if (!bNoData) {
			xdb_res_t *pResTbl = xdb_pexec (pConn, "SELECT * FROM %s.%s", XDB_OBJ_NAME(pDbm), tbl_name);
			XDB_EXPECT(pResTbl->errcode == XDB_OK, pResTbl->errcode, "%s", xdb_errmsg(pResTbl));
			xdb_res2sql (pFile, pResTbl, tbl_name, sql_buf, XDB_MAX_SQL_BUF);
			xdb_free_result (pResTbl);
		}
	}
	rc = XDB_OK;

error:
	if (pConn->conn_stdout != pFile) {
		xdb_fclose (pFile);
	}
	xdb_free (sql_buf);
	xdb_free_result (pRes);
	return rc;
}

XDB_STATIC int 
xdb_source (xdb_conn_t* pConn, const char *file)
{
	int 		rc = -1, offset = 0;
	char 		*sql, *sql_buf = NULL;
	FILE 		*pFile = fopen (file, "rt");
	uint64_t	lineno = 0;

	XDB_EXPECT (NULL != pFile, XDB_E_NOTFOUND, "Can't open '%s', file doesn't exist", file);

	sql_buf = xdb_malloc (XDB_MAX_SQL_BUF);
	XDB_EXPECT (NULL != sql_buf, XDB_E_NOTFOUND, "Can't malloc memory");

    while ((sql = fgets(sql_buf+offset, XDB_MAX_SQL_BUF-offset, pFile))) {
		lineno++;
		int len = strlen (sql);
		sql_buf[XDB_MAX_SQL_BUF - 1] = '\0';
        for (len += offset; (len > 0) && (('\n'==sql_buf[len-1]) || ('\r'==sql_buf[len-1])); --len)
			{ sql_buf[len-1] = '\0'; }
		if (sql_buf[0] == '-') {
			continue;
		}
		if (len <= 1) {
			continue;
		}
		if (sql_buf[len-1] != ';') {
			offset = len;
			continue;
		}
		offset = 0;
		sql_buf[len-1] = '\0';

		xdb_res_t *pRes = xdb_exec (pConn, sql_buf);
		if (pRes->errcode != 0) {
			xdb_errlog ("%"PRIu64": '%s' ERROR %d : %s\n", lineno, sql_buf, pRes->errcode, xdb_errmsg(pRes));
		}
	}

	rc = XDB_OK;
error:
	xdb_fclose (pFile);
	xdb_free (sql_buf);
	return rc;
}
