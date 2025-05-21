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
xdb_row2sql (xdb_res_t *pRes, xdb_row_t *pRow, const char *tbl_name, char *buf, int size)
{
	int 	len = sprintf (buf, "INSERT INTO %s VALUES (", tbl_name?tbl_name:xdb_table_name(pRes, 0));
	int 	slen;
	const char 	*str;
	const uint8_t	*bin;
	int		col_count = xdb_column_count (pRes);

	for (int i = 0; i < col_count; ++i) {
		if (xdb_column_null (pRes, pRow, i)) {
			len += sprintf (buf+len, "NULL,");
			continue;
		}
		switch (xdb_column_type (pRes, i)) {
		case XDB_TYPE_INT:
		case XDB_TYPE_SMALLINT:
		case XDB_TYPE_TINYINT:
			len += sprintf (buf+len, "%d,", xdb_column_int (pRes, pRow, i));
			break;
		case XDB_TYPE_UINT:
		case XDB_TYPE_USMALLINT:
		case XDB_TYPE_UTINYINT:
			len += sprintf (buf+len, "%u,", xdb_column_int (pRes, pRow, i));
			break;
		case XDB_TYPE_BOOL:
			len += sprintf (buf+len, "%s,", xdb_column_bool(pRes, pRow, i) ? "true" : "false");
			break;
		case XDB_TYPE_BIGINT:
			len += sprintf (buf+len, "%"PRIi64",", xdb_column_int64 (pRes, pRow, i));
			break;
		case XDB_TYPE_UBIGINT:
			len += sprintf (buf+len, "%"PRIu64",", xdb_column_int64 (pRes, pRow, i));
			break;
		case XDB_TYPE_TIMESTAMP:
			*(buf + len++) = '\'';
			len +=	xdb_timestamp_sprintf (xdb_column_int64 (pRes, pRow, i), buf+len, size - len);
			*(buf + len++) = '\'';
			*(buf + len++) = ',';
			break;
		case XDB_TYPE_FLOAT:
		case XDB_TYPE_DOUBLE:
			len += sprintf (buf+len, "%f,", xdb_column_double (pRes, pRow, i));
			break;
		case XDB_TYPE_CHAR:
		case XDB_TYPE_VCHAR:
		case XDB_TYPE_JSON:
			str = xdb_column_str2 (pRes, pRow, i, &slen);
			*(buf + len++) = '\'';
			len += xdb_str_escape (buf+len, str, len);
			*(buf + len++) = '\'';
			*(buf + len++) = ',';
			break;
		case XDB_TYPE_BINARY:
		case XDB_TYPE_VBINARY:
			bin = xdb_column_blob (pRes, pRow, i, &slen);
			*(buf + len++) = 'X';
			*(buf + len++) = '\'';
			for (int h = 0; h < slen; ++h) {
				*(buf + len++) = s_xdb_hex_2_str[bin[h]][0];
				*(buf + len++) = s_xdb_hex_2_str[bin[h]][1];
			}
			*(buf + len++) = '\'';
			*(buf + len++) = ',';
			break;
		case XDB_TYPE_INET:
			*(buf + len++) = '\'';
			len += xdb_inet_sprintf (xdb_column_inet(pRes, pRow, i), buf+len, size - len);
			*(buf + len++) = '\'';
			*(buf + len++) = ',';
			break;
		case XDB_TYPE_MAC:
			*(buf + len++) = '\'';
			len += xdb_mac_sprintf (xdb_column_mac(pRes, pRow, i), buf+len, size - len);
			*(buf + len++) = '\'';
			*(buf + len++) = ',';
			break;
		default:
			break;
		}
	}
	buf[len-1] = ')';
	buf[len++] = ';';
	buf[len++] = '\0';
	return len;
}

XDB_STATIC int 
xdb_res2sql (FILE *pFile, xdb_res_t *pRes, const char *tbl_name, char *buf, int size)
{
	xdb_row_t	*pRow;
	while (NULL != (pRow = xdb_fetch_row (pRes))) {
		int len = xdb_row2sql (pRes, pRow, tbl_name, buf, size);
		fwrite (buf, 1, len, pFile);
	}
	return XDB_OK;
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
		const char *tbl_name = xdb_column_str (pRes, pRow, 0);
		const char *tbl_schema = xdb_column_str (pRes, pRow, 1);

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

	char path[XDB_PATH_LEN], cwd[XDB_PATH_LEN];
	xdb_strcpy (path, file);
	char *dir = strrchr (path, '/');
	if (NULL == dir) {
		dir = strrchr (path, '\\');
	}
	if (NULL != dir) {
		*dir = '\0';
		xdb_chdir (path, cwd);
	}

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
	if (NULL != dir) {
		xdb_chdir (cwd, NULL);
	}
	xdb_fclose (pFile);
	xdb_free (sql_buf);
	return rc;
}
