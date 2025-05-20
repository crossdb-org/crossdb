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

#define CLN_EXPORT	XDB_STATIC
#include "../3rd/crossline.c"

static char *s_xdb_banner = 
"   _____                   _____  ____      _        \n"  
"  / ____|                 |  __ \\|  _ \\   _| |_    \n"
" | |     _ __ ___  ___ ___| |  | | |_) | |_   _|     \n"
" | |    | '__/ _ \\/ __/ __| |  | |  _ <    |_|      \n"
" | |____| | | (_) \\__ \\__ \\ |__| | |_) |  %s      \n"
"  \\_____|_|  \\___/|___/___/_____/|____/ crossdb.org\n\n";

XDB_STATIC void 
xdb_print_char (FILE *pFile, char ch, int n)
{
	int i;
	for (i = 0; i < n; ++i) {
		xdb_fputc (ch, pFile);
	}
}

XDB_STATIC bool 
xdb_is_sql_complete (char *sql, bool split)
{
	for (char *ch = sql; *ch; ++ch) {
		if (';' == *ch) {
			if (split) {
				*ch = '\0';
			}
			ch++;
			while (*ch && isspace((int)*ch)) {
				ch++;
			}
			if ('\0' == *ch) {
				return true;
			}
		}
		switch (*ch) {
		case '\'':
		case '"':
		case '`':
			do {
				ch = strchr (ch+1, *ch);
				if (NULL == ch) {
					return false;
				}
			} while ('\\' == *(ch-1));
		}
	}
	return false;
}

XDB_STATIC void 
xdb_print_table_line (FILE *pFile, int *colLen, int n)
{
	xdb_fputc ('+', pFile);
	for (int i = 0; i < n; ++i) {
		xdb_print_char (pFile, '-', colLen[i]+2);
		xdb_fputc ('+', pFile);
	}
	xdb_fputc ('\n', pFile);
}

XDB_STATIC void 
xdb_get_row_len (xdb_res_t *pRes, xdb_row_t *pRow, int *pColLen)
{
	char		buf[1024], *ch;
	const		char *line;
	int			len, slen;
	int			col_count = xdb_column_count (pRes);

	for (int i = 0; i < col_count; ++i) {
		if (xdb_column_null (pRes, pRow, i)) {
			if (pColLen[i] < 4) {
				pColLen[i] = 4;
			}
			continue;
		}
		switch (xdb_column_type (pRes, i)) {
		case XDB_TYPE_INT:
		case XDB_TYPE_SMALLINT:
		case XDB_TYPE_TINYINT:
			len = snprintf (buf, sizeof(buf), "%d", xdb_column_int (pRes, pRow, i));
			break;
		case XDB_TYPE_BOOL:
			len = snprintf (buf, sizeof(buf), "%s", xdb_column_bool(pRes, pRow, i) ? "true" : "false");
			break;
		case XDB_TYPE_BIGINT:
			len = snprintf (buf, sizeof(buf), "%"PRIi64, xdb_column_int64 (pRes, pRow, i));
			break;
		case XDB_TYPE_FLOAT:
		case XDB_TYPE_DOUBLE:
			len = snprintf (buf, sizeof(buf), "%f", xdb_column_double (pRes, pRow, i));
			break;
		case XDB_TYPE_CHAR:
		case XDB_TYPE_VCHAR:
		case XDB_TYPE_JSON:
			line = xdb_column_str (pRes, pRow, i);
			do {
				ch = strchr (line, '\n');
				len = (NULL != ch) ? ch - line : strlen (line);
				if (pColLen[i] < len) {
					pColLen[i] = len;
				}
				line = ch + 1;
			} while (NULL != ch);
			continue;
		case XDB_TYPE_BINARY:
		case XDB_TYPE_VBINARY:
			xdb_column_blob (pRes, pRow, i, &slen);
			len = 2 + slen * 2;
			break;
		case XDB_TYPE_TIMESTAMP:
			len =	xdb_timestamp_sprintf (xdb_column_int64 (pRes, pRow, i), buf, sizeof(buf));
			break;
		case XDB_TYPE_INET:
			len = xdb_inet_sprintf (xdb_column_inet(pRes, pRow, i), buf, sizeof(buf));
			break;
		case XDB_TYPE_MAC:
			len = xdb_mac_sprintf (xdb_column_mac(pRes, pRow, i), buf, sizeof(buf));
			break;
		default:
			continue;
		}
		if (pColLen[i] < len) {
			pColLen[i] = len;
		}
	}
}

XDB_STATIC void 
xdb_fprint_row_table (FILE *pFile, xdb_res_t *pRes, xdb_row_t *pRow, int *pColLen)
{
	xdb_meta_t	*pMeta = (xdb_meta_t*)pRes->col_meta;
	xdb_col_t	**pCol = (xdb_col_t**)pMeta->col_list;
	int			line = 1, n;
	const char		*str;

	for (int i = 0; i < pMeta->col_count; ++i) {
		if (xdb_column_null (pRes, pRow, i)) {
			continue;
		}
		switch (pCol[i]->col_type) {
		case XDB_TYPE_CHAR:
		case XDB_TYPE_VCHAR:
		case XDB_TYPE_JSON:
			for (n=1,str=xdb_column_str(pRes, pRow, i); (str=strchr(str, '\n')) != NULL; str++) {
				n++;
			}
			if (n > line) {
				line = n;
			}
			break;
		}
	}

	for (int n = 0; n < line; ++n) {
		xdb_fputc ('|', pFile);
		for (int i = 0; i < pMeta->col_count; ++i) {
			char	buf[1024];
			int 	plen = 0;
			const char 	*str = "";
			char	*ch = NULL;
			xdb_fputc (' ', pFile);
			if (xdb_column_null (pRes, pRow, i)) {
				if (0 == n) {
					plen = xdb_fprintf (pFile, "NULL");
				}
			} else {
				switch (pCol[i]->col_type) {
				case XDB_TYPE_INT:
				case XDB_TYPE_SMALLINT:
				case XDB_TYPE_TINYINT:
					if (0 == n) {
						plen = xdb_fprintf (pFile, "%d", xdb_column_int (pRes, pRow, i));
					}
					break;
				case XDB_TYPE_BOOL:
					if (0 == n) {
						plen = xdb_fprintf (pFile, "%s", xdb_column_bool(pRes, pRow, i) ? "true" : "false");
					}
					break;
				case XDB_TYPE_BIGINT:
					if (0 == n) {
						plen = xdb_fprintf (pFile, "%"PRIi64, xdb_column_int64 (pRes, pRow, i));
					}
					break;
				case XDB_TYPE_FLOAT:
				case XDB_TYPE_DOUBLE:
					if (0 == n) {
						plen = xdb_fprintf (pFile, "%f", xdb_column_double (pRes, pRow, i));
					}
					break;
				case XDB_TYPE_CHAR:
				case XDB_TYPE_VCHAR:
				case XDB_TYPE_JSON:
					str = xdb_column_str(pRes, pRow, i);
					for (int k = 0; k < n; ++k) {
						ch = strchr (str, '\n');
						//len = (NULL != ch) ? ch - str : strlen (line);
						if (NULL == ch) {
							str = NULL;
							break;
						}
						str = ch + 1;
					}
					if (NULL == str) {
						str = "";
					} else {
						ch = strchr (str, '\n');
						if (ch != NULL) {
							*ch = '\0';
						}
					}
					plen = xdb_fprintf (pFile, "%s", str);
					break;
				case XDB_TYPE_BINARY:
				case XDB_TYPE_VBINARY:
					if (0 == n) {
						int slen;
						const uint8_t *bin = xdb_column_blob (pRes, pRow, i, &slen);
						plen += xdb_fprintf (pFile, "0x");
						for (int h = 0; h < slen; ++h) {
							plen += xdb_fprintf (pFile, "%c%c", s_xdb_hex_2_str[bin[h]][0], s_xdb_hex_2_str[bin[h]][1]);
						}
					}
					break;
				case XDB_TYPE_TIMESTAMP:
					xdb_timestamp_sprintf (xdb_column_int64(pRes, pRow, i), buf, sizeof(buf));
					plen += xdb_fprintf (pFile, "%s", buf);
					break;
				case XDB_TYPE_INET:
					xdb_inet_sprintf (xdb_column_inet(pRes, pRow, i), buf, sizeof(buf));
					plen += xdb_fprintf (pFile, "%s", buf);
					break;
				case XDB_TYPE_MAC:
					xdb_mac_sprintf (xdb_column_mac(pRes, pRow, i), buf, sizeof(buf));
					plen += xdb_fprintf (pFile, "%s", buf);
					break;
				}
			}

			xdb_print_char (pFile, ' ', pColLen[i] + 1 - plen);

			xdb_fputc ('|', pFile);
			if (ch != NULL) {
				*ch = '\n';
			}
		}
		xdb_fputc ('\n', pFile);
	}
}

XDB_STATIC void 
xdb_output_table (xdb_conn_t *pConn, xdb_res_t *pRes)
{
	xdb_row_t	*pRow;
	xdb_meta_t *pMeta = (xdb_meta_t*)pRes->col_meta;
	int			row_count = pRes->row_count > 1000 ? 1000 : pRes->row_count;
	xdb_col_t	**pCol = (xdb_col_t**)pMeta->col_list;
	int 		count = pMeta->col_count;
	int 		colLen[XDB_MAX_COLUMN];

	for (int i = 0; i < pMeta->col_count; ++i) {
		colLen[i] = pCol[i]->col_nmlen;
	}

	for (int i = 0;i < row_count; ++i) {
		if (NULL != (pRow = xdb_fetch_row (pRes))) {
			xdb_get_row_len (pRes, pRow, colLen);
		}
	}
	xdb_rewind_result (pRes);
	
	xdb_print_table_line (pConn->conn_stdout, colLen, count);
	xdb_fputc ('|', pConn->conn_stdout);
	for (int i = 0; i < count; ++i) {
		xdb_fputc (' ', pConn->conn_stdout);
		xdb_fprintf (pConn->conn_stdout, "%s", pCol[i]->col_name);
		xdb_print_char (pConn->conn_stdout, ' ', colLen[i] + 1 - pCol[i]->col_nmlen);
		xdb_fputc ('|', pConn->conn_stdout);
	}
	xdb_fputc ('\n', pConn->conn_stdout);
	
	xdb_print_table_line (pConn->conn_stdout, colLen, count);
	
	while (NULL != (pRow = xdb_fetch_row (pRes))) {
		xdb_fprint_row_table (pConn->conn_stdout, pRes, pRow, colLen);
	}

	xdb_print_table_line (pConn->conn_stdout, colLen, count);
}

XDB_STATIC int 
sql_add_completion (crossline_completions_t *pCompletion, const char *prefix, const char **match, const char **help)
{
	int 	i, len = (int)strlen(prefix), num = 0;
	crossline_color_e wcolor, hcolor;

	for (i = 0; match[i] != NULL; ++i) {
		if (!strcasecmp(prefix, match[i])) {
			return -1;
		}
	}

	for (i = 0; match[i] != NULL; ++i) {
		if (!strncasecmp(prefix, match[i], len)) {
			num++;
			if (NULL != help) {
				wcolor = CROSSLINE_FGCOLOR_BRIGHT | (i%2 ? CROSSLINE_FGCOLOR_YELLOW : CROSSLINE_FGCOLOR_CYAN);
				hcolor = i%2 ? CROSSLINE_FGCOLOR_WHITE : CROSSLINE_FGCOLOR_CYAN;
				crossline_completion_add_color (pCompletion, match[i], wcolor, help[i], hcolor);
			} else {
				crossline_completion_add_color (pCompletion, match[i], CROSSLINE_FGCOLOR_BRIGHT | CROSSLINE_FGCOLOR_MAGENTA, NULL, 0);
			}
		}
	}

	return num;
}

XDB_STATIC bool 
xdb_shell_add_db (xdb_conn_t* pConn, const char *prefix, crossline_completions_t *pCompletion)
{
	bool		bFound = true;
	xdb_row_t 	*pRow;

	xdb_res_t *pRes = xdb_pexec (pConn, "SELECT database FROM system.databases WHERE database='%s'", prefix);
	if (0 == pRes->row_count) {
		bFound = false;
		xdb_free_result (pRes);
		pRes = xdb_exec (pConn, "SELECT database FROM system.databases");
		while (NULL != (pRow = xdb_fetch_row (pRes))) {
			const char *name = xdb_column_str(pRes, pRow, 0);
			if (! strncasecmp(prefix, name, strlen(prefix))) {
				crossline_completion_add (pCompletion, name, NULL);
			}
		}
	}
	xdb_free_result (pRes);
	return bFound;
}

XDB_STATIC bool 
xdb_shell_add_tbl (xdb_conn_t* pConn, const char *prefix, crossline_completions_t *pCompletion)
{
	bool		bFound = true;
	xdb_row_t 	*pRow;

	xdb_res_t *pRes = xdb_pexec (pConn, "SELECT table FROM system.tables WHERE database='%s' AND table='%s'", xdb_curdb(pConn), prefix);
	if (0 == pRes->row_count) {
		bFound = false;
		xdb_free_result (pRes);
		pRes = xdb_pexec (pConn, "SELECT table FROM system.tables WHERE database='%s'", xdb_curdb(pConn));
		while (NULL != (pRow = xdb_fetch_row (pRes))) {
			const char *name = xdb_column_str(pRes, pRow, 0);
			if (! strncasecmp(prefix, name, strlen(prefix))) {
				crossline_completion_add (pCompletion, name, NULL);
			}
		}
	}
	xdb_free_result (pRes);
	return bFound;
}

XDB_STATIC bool 
xdb_shell_add_svr (xdb_conn_t* pConn, const char *prefix, crossline_completions_t *pCompletion)
{
	bool		bFound = true;
	xdb_row_t 	*pRow;

	xdb_res_t *pRes = xdb_pexec (pConn, "SELECT server FROM system.servers WHERE server='%s'", xdb_curdb(pConn), prefix);
	if (0 == pRes->row_count) {
		bFound = false;
		xdb_free_result (pRes);
		pRes = xdb_pexec (pConn, "SELECT server FROM system.servers", xdb_curdb(pConn));
		while (NULL != (pRow = xdb_fetch_row (pRes))) {
			const char *name = xdb_column_str(pRes, pRow, 0);
			if (! strncasecmp(prefix, name, strlen(prefix))) {
				crossline_completion_add (pCompletion, name, NULL);
			}
		}
	}
	xdb_free_result (pRes);
	return bFound;
}

XDB_STATIC bool 
xdb_shell_add_idx (xdb_conn_t* pConn, const char *prefix, crossline_completions_t *pCompletion)
{
	bool		bFound = true;
	xdb_row_t 	*pRow;

	xdb_res_t *pRes = xdb_pexec (pConn, "SELECT idx_key FROM system.indexes WHERE database='%s' AND idx_key='%s'", xdb_curdb(pConn), prefix);
	if (0 == pRes->row_count) {
		bFound = false;
		xdb_free_result (pRes);
		pRes = xdb_pexec (pConn, "SELECT idx_key FROM system.indexes WHERE database='%s'", xdb_curdb(pConn));
		while (NULL != (pRow = xdb_fetch_row (pRes))) {
			const char *name = xdb_column_str(pRes, pRow, 0);
			if (! strncasecmp(prefix, name, strlen(prefix))) {
				if (!crossline_completion_find (pCompletion, name)) {
					crossline_completion_add (pCompletion, name, NULL);
				}
			}
		}
	}
	xdb_free_result (pRes);
	return bFound;
}

XDB_STATIC int 
xdb_shell_add_col (xdb_conn_t* pConn, const char *tbl, const char *prefix, crossline_completions_t *pCompletion)
{
	bool		bFound = true;
	int			num = 0;
	xdb_row_t 	*pRow;

	xdb_res_t *pRes = xdb_pexec (pConn, "SELECT column FROM system.columns WHERE database='%s' AND table='%s' AND colunm='%s'", xdb_curdb(pConn), tbl, prefix);
	if (0 == pRes->row_count) {
		bFound = false;
		xdb_free_result (pRes);
		pRes = xdb_pexec (pConn, "SELECT column FROM system.columns WHERE database='%s' AND table='%s'", xdb_curdb(pConn), tbl);
		while (NULL != (pRow = xdb_fetch_row (pRes))) {
			const char *name = xdb_column_str(pRes, pRow, 0);
			if (! strncasecmp(prefix, name, strlen(prefix))) {
				crossline_completion_add (pCompletion, name, NULL);
				num++;
			}
		}
	}
	xdb_free_result (pRes);
	return bFound ? -1 : num;
}

XDB_STATIC bool 
xdb_shell_completion_filter (xdb_conn_t* pConn, crossline_completions_t *pCompletion, xdb_token_t *pTkn, const char *tbl, const char **commands)
{
	xdb_token_type 	type = xdb_next_token (pTkn);
	const char 		*pre_tkn = pTkn->token;
	xdb_token_type 	pre_type = type;

	while (XDB_TOK_EOF != type) {
		pre_type = type;
		pre_tkn = pTkn->token;
		type = xdb_next_token (pTkn);
	}
	if (XDB_TOK_ID == pre_type) {
		int ret1 = sql_add_completion (pCompletion, pre_tkn, commands, NULL);
		int ret2 = xdb_shell_add_col (pConn, tbl, pre_tkn, pCompletion);
		if (!(((ret1<0) || (ret2<0)) || (ret1+ret2==0))) {
			return false;
		}
	}
	sql_add_completion (pCompletion, "", commands, NULL); 
	xdb_shell_add_col (pConn, tbl, "", pCompletion);
	return true;
}

XDB_STATIC void 
xdb_shell_completion_hook (char const *buf, crossline_completions_t *pCompletion, void *pArg)
{
	char		*sql = xdb_strdup (buf, 0);
	xdb_conn_t* pConn = pArg;

	if (NULL == sql) {
		return;
	}

	static const char *commands[] = {
		"CREATE",
		"DROP",
		"ALTER",
		"SELECT",
		"INSERT",
		"UPDATE",
		"DELETE",
		"BEGIN",
		"COMMIT",
		"ROLLBACK",
		"USE",
		"SHOW",
		"DESCRIBE",
		"EXPLAIN",
		"SET",
		"OPEN",
		"CLOSE",
		"DUMP",
		"SOURCE",
		"SHELL",
		"HELP",
		NULL
	};

	static const char *cmd_helps[] = {
		"Create database, table, index, etc",
		"Drop database, table, index, etc",
		"Alter database, table, index, etc",
		"Select rows from table",
		"Insert rows into table",
		"Update rows in table",
		"Delete rows from table",
		"Begin transaction",
		"Commit transaction",
		"Rollback transaction",
		"Switch Database",
		"Show database, table, index, etc",
		"Show Table Schema",
		"Show SELECT statement index selection",
		"Config parameters",
		"Open database",
		"Close database",
		"Dump database",
		"Load SQL file",
		"Enter interactive shell",
		"Help",
		NULL
	};

	static const char *fitlercommands[] = {"WHERE", "ORDER BY", "LIMIT", "OFFSET", "AND", NULL};

	crossline_color_e hint_color = CROSSLINE_FGCOLOR_BLACK | CROSSLINE_BGCOLOR_CYAN;
	//crossline_color_e hint_color = CROSSLINE_FGCOLOR_WHITE | CROSSLINE_BGCOLOR_MAGENTA;


	xdb_token_t token = XDB_TOK_INIT(sql);
	xdb_token_type type = xdb_next_token (&token);
	if (! strcasecmp (token.token, "SELECT")) {
select:
		type = xdb_next_token (&token); // skip select
		if (XDB_TOK_EOF == type) {
			crossline_completion_add_color (pCompletion, "*", CROSSLINE_FGCOLOR_BRIGHT | CROSSLINE_FGCOLOR_MAGENTA, NULL, 0);
			goto exit;
		}
		type = xdb_next_token (&token); // skip first col		
		while (XDB_TOK_COMMA == type ) {
			type = xdb_next_token (&token); // skip ,
			if (XDB_TOK_EOF == type) {
				break;
			}
			type = xdb_next_token (&token); // skip col
		}

		if (XDB_TOK_EOF == type) {
			crossline_completion_add_color (pCompletion, "FROM", CROSSLINE_FGCOLOR_BRIGHT | CROSSLINE_FGCOLOR_MAGENTA, NULL, 0);
			goto exit;
		}
		static const char *commands[] = {"FROM", NULL};
		if (sql_add_completion (pCompletion, token.token, commands, NULL) >= 0) {
			goto exit;
		}

		if (! strcasecmp (token.token, "FROM")) {
			xdb_next_token (&token);
			if (!xdb_shell_add_tbl (pConn, token.token, pCompletion)) {
				goto exit;
			}
			xdb_shell_completion_filter (pConn, pCompletion, &token, token.token, fitlercommands);
		}
	} else if (! strcasecmp (token.token, "SHOW")) {
		static const char *commands[] = {"DATABASES", "TABLES", "INDEXES", "COLUMNS", "SERVERS", "CREATE", NULL};
		xdb_next_token (&token);
		if (sql_add_completion (pCompletion, token.token, commands, NULL) >= 0) {
			goto exit;
		}
		if (! strcasecmp (token.token, "CREATE")) {
			static const char *commands[] = {"TABLE", NULL};
			xdb_next_token (&token);
			if (sql_add_completion (pCompletion, token.token, commands, NULL) >= 0) {
				goto exit;
			}
			if (! strcasecmp (token.token, "TABLE")) {			
				xdb_next_token (&token);
				if (!xdb_shell_add_tbl (pConn, token.token, pCompletion)) {
					goto exit;
				}
			}
		}
	} else if (! strcasecmp (token.token, "SET")) {
		//pStmt = xdb_parse_set (pConn, &token);
	} else if (! strcasecmp (token.token, "DELETE")) {
		type = xdb_next_token (&token); // skip select
		static const char *commands[] = {"FROM", NULL};
		if (sql_add_completion (pCompletion, token.token, commands, NULL) >= 0) {
			goto exit;
		}
		if (! strcasecmp (token.token, "FROM")) {
			xdb_next_token (&token);
			if (!xdb_shell_add_tbl (pConn, token.token, pCompletion)) {
				goto exit;
			}
			xdb_shell_completion_filter (pConn, pCompletion, &token, token.token, fitlercommands);
		}	
	} else if (! strcasecmp (token.token, "DROP")) {
		static const char *commands[] = {"DATABASE", "TABLE", "INDEX", "SERVER", NULL};
		xdb_next_token (&token);
		if (sql_add_completion (pCompletion, token.token, commands, NULL) >= 0) {
			goto exit;
		}
		if (! strcasecmp (token.token, "DATABASE")) {
			xdb_next_token (&token);
			if (!xdb_shell_add_db (pConn, token.token, pCompletion)) {
				goto exit;
			}
		} else if (! strcasecmp (token.token, "TABLE")) {
			xdb_next_token (&token);
			if (!xdb_shell_add_tbl (pConn, token.token, pCompletion)) {
				goto exit;
			}
		} else if (! strcasecmp (token.token, "INDEX")) {
			xdb_next_token (&token);
			if (!xdb_shell_add_idx (pConn, token.token, pCompletion)) {
				goto exit;
			}
			xdb_next_token (&token);
			static const char *commands[] = {"ON", NULL};
			if (sql_add_completion (pCompletion, token.token, commands, NULL) >= 0) {
				goto exit;
			}
			if (! strcasecmp (token.token, "ON")) {
				xdb_next_token (&token);
				if (!xdb_shell_add_tbl (pConn, token.token, pCompletion)) {
					goto exit;
				}
			}
		} else if (! strcasecmp (token.token, "SERVER")) {
			xdb_next_token (&token);
			if (!xdb_shell_add_svr (pConn, token.token, pCompletion)) {
				goto exit;
			}
		}
	} else if (! strcasecmp (token.token, "DESC") || ! strcasecmp (token.token, "DESCRIBE")) {
		xdb_next_token (&token);
		if (!xdb_shell_add_tbl (pConn, token.token, pCompletion)) {
			goto exit;
		}
	} else if (! strcasecmp (token.token, "INSERT")) {
		type = xdb_next_token (&token);
		static const char *commands[] = {"INTO", NULL};
		if (sql_add_completion (pCompletion, token.token, commands, NULL) >= 0) {
			goto exit;
		}
		if (! strcasecmp (token.token, "INTO")) {
			xdb_next_token (&token);
			if (!xdb_shell_add_tbl (pConn, token.token, pCompletion)) {
				goto exit;
			}
			static const char *commands[] = {"VALUES", NULL};
			xdb_shell_completion_filter (pConn, pCompletion, &token, token.token, commands);
		}
 	} else if (! strcasecmp (token.token, "CREATE")) {
		static const char *commands[] = {"DATABASE", "TABLE", "INDEX", "SERVER", NULL};
		xdb_next_token (&token);
		if (sql_add_completion (pCompletion, token.token, commands, NULL) >= 0) {
			goto exit;
		}
		if (! strcasecmp (token.token, "DATABASE")) {
			type = xdb_next_token (&token);
			if (XDB_TOK_ID != type) {
				crossline_hints_set_color (pCompletion, "db_name", hint_color);
				goto exit;
			}
		} else if (! strcasecmp (token.token, "TABLE")) {
			type = xdb_next_token (&token);
			if (XDB_TOK_ID != type) {
				crossline_hints_set_color (pCompletion, "tbl_name", hint_color);
				goto exit;
			}
		} else if (! strcasecmp (token.token, "SERVER")) {
			type = xdb_next_token (&token);
			if (XDB_TOK_ID != type) {
				crossline_hints_set_color (pCompletion, "svr_name", hint_color);
				goto exit;
			}
		} else if (! strcasecmp (token.token, "INDEX")) {
			type = xdb_next_token (&token);
			if (XDB_TOK_ID != type) {
				crossline_hints_set_color (pCompletion, "idx_name", hint_color);
				goto exit;
			}
			type = xdb_next_token (&token);
			static const char *commands[] = {"ON", NULL};
			if (sql_add_completion (pCompletion, token.token, commands, NULL) >= 0) {
				goto exit;
			}
			if (! strcasecmp (token.token, "ON")) {
				xdb_next_token (&token);
				if (!xdb_shell_add_tbl (pConn, token.token, pCompletion)) {
					goto exit;
				}
			}
		}
	} else if (! strcasecmp (token.token, "UPDATE")) {
		type = xdb_next_token (&token);
		if (!xdb_shell_add_tbl (pConn, token.token, pCompletion)) {
			goto exit;
		}
		char *tbl = token.token;
		static const char *commands[] = {"SET", NULL};
		type = xdb_next_token (&token);
		if (sql_add_completion (pCompletion, token.token, commands, NULL) >= 0) {
			goto exit;
		}
		if (! strcasecmp (token.token, "SET")) {
			xdb_shell_completion_filter (pConn, pCompletion, &token, tbl, fitlercommands);
		}
	} else if (! strcasecmp (token.token, "USE")) {
		xdb_next_token (&token);
		xdb_shell_add_db (pConn, token.token, pCompletion);
	} else if (! strcasecmp (token.token, "EXPLAIN")) {
		static const char *commands[] = {"SELECT", NULL};
		xdb_next_token (&token);
		if (sql_add_completion (pCompletion, token.token, commands, NULL) >= 0) {
			goto exit;
		}
		goto select;
	} else if (! strcasecmp (token.token, "OPEN")) {
		static const char *commands[] = {"DATABASE", NULL};
		xdb_next_token (&token);
		if (sql_add_completion (pCompletion, token.token, commands, NULL) >= 0) {
			goto exit;
		}	
	} else if (! strcasecmp (token.token, "CLOSE")) {
		static const char *commands[] = {"DATABASE", NULL};
		xdb_next_token (&token);
		if (sql_add_completion (pCompletion, token.token, commands, NULL) >= 0) {
			goto exit;
		}
		if (! strcasecmp (token.token, "DATABASE")) {
			xdb_next_token (&token);
			if (!xdb_shell_add_db (pConn, token.token, pCompletion)) {
				goto exit;
			}
		}
	} else if (! strcasecmp (token.token, "DUMP")) {
		static const char *commands[] = {"DATABASE", NULL};
		xdb_next_token (&token);
		if (sql_add_completion (pCompletion, token.token, commands, NULL) >= 0) {
			goto exit;
		}
		if (! strcasecmp (token.token, "DATABASE")) {
			xdb_next_token (&token);
			if (!xdb_shell_add_db (pConn, token.token, pCompletion)) {
				goto exit;
			}
			type = xdb_next_token (&token);
			while (1) {
				static const char *commands[] = {"NODROP", "NOCREATE", "NODATA", "INTO", NULL};
				if (sql_add_completion (pCompletion, token.token, commands, NULL) > 0) {
					break;
				}
				type = xdb_next_token (&token);
			}
		}
	} else {
		sql_add_completion (pCompletion, token.token, commands, cmd_helps);
	}

exit:
	xdb_free (sql);
}

XDB_STATIC int 
xdb_shell_process (xdb_conn_t *pConn, const char *sql)
{
	while (XDB_TOK_SP == s_tok_type[(uint8_t)*sql]) {
		sql++;
	}

	xdb_exec_out (pConn, sql, 0);

	return XDB_OK;
}

XDB_STATIC int 
xdb_shell_loop (xdb_conn_t* pConn, const char *prompt, const char *db, bool bQuite)
{
	bool	bNewConn = false;
	
	if (!bQuite && isatty(STDIN_FILENO)) {
		xdb_print (s_xdb_banner, xdb_version());

		crossline_color_set (CROSSLINE_FGCOLOR_YELLOW);
		xdb_print ("============ Welcome to CrossDB Shell ============\n");
		crossline_color_set (CROSSLINE_FGCOLOR_BRIGHT | CROSSLINE_FGCOLOR_CYAN);
		xdb_print ("<help>");
		crossline_color_set (0);
		xdb_print (": Help Info\t");
		crossline_color_set (CROSSLINE_FGCOLOR_BRIGHT | CROSSLINE_FGCOLOR_MAGENTA);
		xdb_print ("<F1>");
		crossline_color_set (0);
		xdb_print (": Shortcuts\n");
		crossline_color_set (CROSSLINE_FGCOLOR_BRIGHT | CROSSLINE_FGCOLOR_RED);
		xdb_print ("<exit>");
		crossline_color_set (0);
		xdb_print (": Exit shell\t");
		crossline_color_set (CROSSLINE_FGCOLOR_BRIGHT | CROSSLINE_FGCOLOR_BLUE);
		xdb_print ("<TAB>");
		crossline_color_set (0);
		xdb_print (": Auto completion	\n\n");
	}

	if (NULL == pConn) {
		pConn = xdb_open (db);
		bNewConn = true;
	} else if (NULL != db) {
		xdb_pexec (pConn, "USE %s", db);
	}
	if (NULL == pConn) {
		return -1;
	}
	char *buf = xdb_malloc (XDB_MAX_SQL_BUF);
	if (NULL == buf) {
		xdb_errlog ("Run out of memory\n");
		return -1;
	}

	if (NULL == prompt) {
		prompt = "XDB> ";
	}

	crossline_paging_set (0);
	crossline_completion_register2 (xdb_shell_completion_hook, pConn);

	crossline_prompt_color_set (CROSSLINE_FGCOLOR_BRIGHT | CROSSLINE_FGCOLOR_GREEN);
	while (1) {
		char *sql = buf;
		int len = XDB_MAX_SQL_BUF;

		while (NULL != (sql = crossline_readline (sql==buf?prompt:"  -> ", sql, len - 1))) {
			if ((sql==buf) && (!strncasecmp (buf, "exit", 4) || !strncasecmp (buf, "quit", 4))) {
				goto exit;
			}
			if (0 == strlen(buf)) {
				break;
			}
			if (!strncasecmp (buf, "HELP", 4) || xdb_is_sql_complete (buf, false)) {
				break;
			}
			int slen = strlen (sql);
			sql += slen;
			*sql++ = '\n';
			len -= (slen+1);
		}
		if (NULL == sql) {
			break;
		}

		buf[len - 1] = '\0';
		if (0 == strlen(buf)) {
			continue;
		}		

		//xdb_dbgprint ("run '%s'\n", buf);
		xdb_shell_process (pConn, buf);
	}

exit:
	if (bNewConn) {
		xdb_close (pConn);
	}
	xdb_free (buf);
	return XDB_OK;
}
