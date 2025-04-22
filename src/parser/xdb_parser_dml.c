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

XDB_STATIC xdb_tblm_t * xdb_stmt_find_table (xdb_stmt_select_t *pStmt, const char *name, int *pRefTblId)
{
	for (int i = 0; i < pStmt->reftbl_count; ++i) {
		xdb_tblm_t *pTblm = pStmt->ref_tbl[i].pRefTblm;
		if (!strcasecmp (XDB_OBJ_NAME(pTblm), name)) {
			*pRefTblId = i;
			return pTblm;
		}
	}
	return NULL;
}

#if 0
XDB_STATIC xdb_field_t * xdb_stmt_find_field (xdb_conn_t *pConn, xdb_stmt_select_t *pStmt, const char *name, int len, int *pRefTblId)
{
	xdb_field_t *pField = NULL;
	for (int i = 0; i < pStmt->reftbl_count; ++i) {
		xdb_tblm_t *pTblm = pStmt->ref_tbl[i].pRefTblm;
		xdb_field_t *pFld = xdb_find_field (pTblm, name, len);
		if (pFld != NULL) {
			XDB_EXPECT (NULL == pField, XDB_E_STMT, "Column '%s' in field list is ambiguous", name);
			*pRefTblId = i;
		}
	}
	XDB_EXPECT (NULL != pField, XDB_E_NOTFOUND, "Field '%s' doesn't exist", name);
	return pField;

error:
	return NULL;
}
#endif

XDB_STATIC int 
xdb_inet_sprintf (const xdb_inet_t *pInet, char *buf, int size)
{
	int len = 0;
	if (4 == pInet->family) {
		if (32 == pInet->mask) {
			len = sprintf (buf, "%d.%d.%d.%d", pInet->addr[0], pInet->addr[1], pInet->addr[2], pInet->addr[3]);
		} else {
			len = sprintf (buf, "%d.%d.%d.%d/%d", pInet->addr[0], pInet->addr[1], pInet->addr[2], pInet->addr[3], pInet->mask);
		}
	} else if (6 == pInet->family) {
		inet_ntop (AF_INET6, pInet->addr, buf, INET6_ADDRSTRLEN);
		len = strlen (buf);
		if (128 != pInet->mask) {
			len += sprintf (buf + len, "/%d", pInet->mask);
		}
	}
	return len;
}

XDB_STATIC bool 
xdb_inet_scanf (xdb_inet_t *pInet, const char *addr)
{
	int rc;
	pInet->family = 4;
	pInet->mask = 32;
	char *mask = strchr (addr, '/');
	if (mask) {
		pInet->mask = atoi (mask + 1);
		*mask = '\0';
	}
	char *ipv6 = strchr (addr, ':');
	if (NULL == ipv6) {
		rc = inet_pton (AF_INET, addr, pInet->addr);
	} else {
		pInet->family = 6;
		rc = inet_pton (AF_INET6, addr, pInet->addr);
		if (NULL == mask) {
			pInet->mask = 128;
		}
	}
	return rc == 1;
}

XDB_STATIC int 
xdb_mac_sprintf (const xdb_mac_t *pMac, char *buf, int size)
{
	return sprintf (buf, "%02x:%02x:%02x:%02x:%02x:%02x", pMac->addr[0],pMac->addr[1],pMac->addr[2],pMac->addr[3],pMac->addr[4],pMac->addr[5]);	
}

XDB_STATIC bool 
xdb_mac_scanf (xdb_mac_t *pMac, const char *addr)
{
	int hex1, hex2;
	for (int i = 0; i < 6; ++i) {
		hex1 = s_xdb_str_2_hex[(uint8_t)*addr++];
		if (xdb_unlikely (hex1 < 0)) {
			return false;
		}
		hex2 = s_xdb_str_2_hex[(uint8_t)*addr++];
		if (xdb_unlikely (hex2 < 0)) {
			return false;
		}
		pMac->addr[i] = (hex1<<4) | hex2;
		if (*addr == ':' || *addr == '-' || *addr == '.') {
			addr++;
		}
	}

	return *addr == '\0';
}

XDB_STATIC int 
xdb_timestamp_sprintf (uint64_t timestamp, char *buf, int size)
{
	struct tm tm_val;
	int 	millsec = timestamp%1000000;
	time_t time_val = (time_t)timestamp/1000000;
	localtime_r(&time_val, &tm_val);
	int len = strftime (buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S", &tm_val);
	if (millsec) {
		if (millsec%1000) {
			len += sprintf (buf+len, ".%06d", millsec);
		} else {
			len += sprintf (buf+len, ".%03d", millsec/1000);
		}
	}
	return len;
}

XDB_STATIC int64_t 
xdb_timestamp_scanf (const char *time_str)
{
	struct tm tm;
	uint32_t n, mm = 0, dd = 0, yy = 0, HH = 0, MM = 0, SS = 0, SubSec = 0;
	uint64_t time_val;
	char	pad[6];

	n = sscanf(time_str, "%u%c%u%c%u%c%u%c%u%c%u%c%u", &yy, &pad[0], &mm, &pad[1], &dd, &pad[2], &HH, &pad[3], &MM, &pad[4], &SS, &pad[4], &SubSec);

	if (1 == n) {
		return yy;
	}

	tm.tm_mon = mm - 1;
	tm.tm_mday = dd;
	tm.tm_year = yy - 1900;

	tm.tm_hour = HH;
	tm.tm_min = MM;
	tm.tm_sec = SS;
	tm.tm_isdst = -1;  // Daylight saving time auto set by system

	time_val = mktime(&tm);

	if (SubSec > 0) {
		char *dot = strrchr (time_str, '.');
		if (dot) {
			switch (strlen(dot)-1) {
			case 1: SubSec *= 100000; break;
			case 2: SubSec *= 10000; break;
			case 3: SubSec *= 1000; break;
			case 4: SubSec *= 100; break;
			case 5: SubSec *= 10; break;			
			}
		}
	}

	return time_val*1000000 + SubSec;
}

XDB_STATIC xdb_stmt_t* 
xdb_parse_insert (xdb_conn_t* pConn, xdb_token_t *pTkn, bool bPStmt, bool bReplace)
{
	xdb_stmt_insert_t *pStmt;

	if (xdb_unlikely (bPStmt)) {
		pStmt = xdb_malloc (sizeof (*pStmt));
		XDB_EXPECT(NULL != pStmt, XDB_E_MEMORY, "Run out of memory");
	} else {
		pStmt = &pConn->stmt_union.insert_stmt;
	}

	pStmt->stmt_type = !bReplace ? XDB_STMT_INSERT : XDB_STMT_REPLACE;
	pStmt->pSql = NULL;
	pStmt->pRowsBuf = NULL;

	XDB_EXPECT (pConn->pCurDbm != NULL, XDB_E_NODB, XDB_SQL_NO_DB_ERR);

	xdb_token_type	type = xdb_next_token (pTkn);

	XDB_EXPECT ((XDB_TOK_ID == type) && !strcasecmp (pTkn->token, "INTO"), XDB_E_STMT, "Expect INTO");

	type = xdb_next_token (pTkn);

	XDB_EXPECT (XDB_TOK_ID == type, XDB_E_STMT, "Miss table");

	XDB_PARSE_DBTBLNAME();

	xdb_tblm_t		*pTblm = pStmt->pTblm;

	if (xdb_unlikely (pTblm->pDbm->bSysDb)) {
		XDB_EXPECT (pConn == s_xdb_sysdb_pConn, XDB_E_CONSTRAINT, "Can't insert into table '%s' in system database", XDB_OBJ_NAME(pTblm));
	}

	pStmt->fld_count = 0;
	pStmt->row_count = 0;
	pStmt->bind_count = 0;

	bool bColList = false;

	uint8_t		null_bmp[(XDB_MAX_COLUMN+7)>>3];

	if (XDB_TOK_LP == type) {
		memset (null_bmp, 0, XDB_ALIGN8(pTblm->null_bytes));
		bColList = true;
		// col list
		do {
			type = xdb_next_token (pTkn);
		 	if (XDB_TOK_ID == type) {
				xdb_field_t *pField = xdb_find_field (pTblm, pTkn->token, pTkn->tk_len);
				XDB_EXPECT (pField != NULL, XDB_E_STMT, "field '%s' doesn't exist", pTkn->token);
				pStmt->pFldList[pStmt->fld_count++] = pField;
				XDB_SET_NOTNULL (null_bmp, pField->fld_id);
		 	} else {
		 		break;
		 	}
			type = xdb_next_token (pTkn);
		} while (XDB_TOK_COMMA == type);

		XDB_EXPECT (XDB_TOK_RP == type, XDB_E_STMT, "Miss )");
		type = xdb_next_token (pTkn);
	} else {
		memset (null_bmp, 0xFF, XDB_ALIGN8(pTblm->null_bytes));
		pStmt->fld_count = pTblm->fld_count;
		if (bReplace) {
			// if update will use it
			memcpy (pStmt->pFldList, pTblm->ppFields, pStmt->fld_count * sizeof (void*));
		}
	}

	XDB_EXPECT ((XDB_TOK_ID == type) && !strcasecmp (pTkn->token, "VALUES"), XDB_E_STMT, "Expect VALUES");

	if (pTblm->row_size < sizeof (pStmt->row_buf)) {
		pStmt->pRowsBuf = pStmt->row_buf;
		pStmt->buf_len	= sizeof (pStmt->row_buf);
	} else {
		pStmt->buf_len = 4  * pTblm->row_size;
		pStmt->pRowsBuf = xdb_malloc (pStmt->buf_len);
	}
	XDB_EXPECT (NULL != pStmt->pRowsBuf, XDB_E_MEMORY, "No memory");

	uint32_t	offset = 0;
	int 		row_vlen = 0;
	if (pTblm->vfld_count > 0) {
		row_vlen = pTblm->vfld_count * sizeof(xdb_str_t);
	}
	int row_len = XDB_ALIGN4 (pTblm->row_size + row_vlen);

	// Multiple rows
	do {
		if (xdb_unlikely (offset + row_len > pStmt->buf_len)) {
			uint32_t buf_len;
			do {
				buf_len = pStmt->buf_len <<= 1;
			} while (offset + row_len > buf_len);
			void *pRowsBuf;
			if (pStmt->pRowsBuf == pStmt->row_buf) {
				pRowsBuf = xdb_malloc (buf_len);
				if (NULL != pRowsBuf) {
					memcpy (pRowsBuf, pStmt->row_buf, sizeof (pStmt->row_buf));
				}
			} else {
				pRowsBuf = xdb_realloc (pStmt->pRowsBuf, buf_len);
			}
			XDB_EXPECT (NULL != pRowsBuf, XDB_E_MEMORY, "No memory");
			pStmt->pRowsBuf = pRowsBuf;
			pStmt->buf_len = buf_len;
		}

		void *pRow = pStmt->pRowsBuf + offset;

		// TBD copy from default?
		memset (pRow, 0, pTblm->row_size);
		uint8_t	*pNull = pRow + pTblm->null_off;
		memcpy (pNull, null_bmp, pTblm->null_bytes);
		if (pTblm->vfld_count > 0) {
			memset (pRow + pTblm->row_size, 0, row_vlen);
		}

		pStmt->row_offset[pStmt->row_count++] = offset;
		type = xdb_next_token (pTkn);

		XDB_EXPECT (XDB_TOK_LP == type, XDB_E_STMT, "Miss (");

		// Single row values
		int fld_seq = 0;
		xdb_str_t *pVStr = pRow + pTblm->row_size, *pStr;		
		*((uint8_t*)pRow + pTblm->vtype_off) = XDB_VTYPE_PTR;

		do {
			type = xdb_next_token (pTkn);
			xdb_field_t *pField = bColList ? pStmt->pFldList[fld_seq] : &pTblm->pFields[fld_seq];
			XDB_EXPECT (++fld_seq <= pStmt->fld_count, XDB_E_STMT, "Too many values");

			if (type <= XDB_TOK_NUM) {
				if ((xdb_unlikely (XDB_TOK_ID == type) && !strcasecmp(pTkn->token, "NULL"))) {
					XDB_SET_NULL (pNull, pField->fld_id);
				} else {
					switch (pField->fld_type) {
					case XDB_TYPE_INT:
						XDB_EXPECT ((XDB_TOK_NUM == type), XDB_E_STMT, "Expect number");
						*(int32_t*)(pRow+pField->fld_off) = atoi (pTkn->token);
						//xdb_dbgprint ("%s %d\n", pField->fld_name, vi32);
						break;
					case XDB_TYPE_TIMESTAMP:
						if (XDB_TOK_STR == type) {
							*(int64_t*)(pRow+pField->fld_off) = xdb_timestamp_scanf (pTkn->token);
							break;
						}
						// fall through
					case XDB_TYPE_BIGINT:
						XDB_EXPECT ((XDB_TOK_NUM == type), XDB_E_STMT, "Expect number");
						*(int64_t*)(pRow+pField->fld_off) = atoll (pTkn->token);
						//xdb_dbgprint ("%s %d\n", pField->fld_name, vi32);
						break;
					case XDB_TYPE_TINYINT:
						XDB_EXPECT ((XDB_TOK_NUM == type), XDB_E_STMT, "Expect number");
						*(int8_t*)(pRow+pField->fld_off) = atoi (pTkn->token);
						//xdb_dbgprint ("%s %d\n", pField->fld_name, vi32);
						break;
					case XDB_TYPE_BOOL:
						XDB_EXPECT ((XDB_TOK_ID == type), XDB_E_STMT, "Expect TRUE/FALSE");
						if (!strcasecmp(pTkn->token, "true")) {
							*(int8_t*)(pRow+pField->fld_off) = 1;
						} else {
							XDB_EXPECT (!strcasecmp(pTkn->token, "false"), XDB_E_STMT, "Expect TRUE/FALSE");
							*(int8_t*)(pRow+pField->fld_off) = 0;
						}
						//xdb_dbgprint ("%s %d\n", pField->fld_name, vi32);
						break;
					case XDB_TYPE_SMALLINT:
						XDB_EXPECT ((XDB_TOK_NUM == type), XDB_E_STMT, "Expect number");
						*(int16_t*)(pRow+pField->fld_off) = atoi (pTkn->token);
						//xdb_dbgprint ("%s %d\n", pField->fld_name, vi32);
						break;
					case XDB_TYPE_FLOAT:
						XDB_EXPECT ((XDB_TOK_NUM == type), XDB_E_STMT, "Expect number");
						*(float*)(pRow+pField->fld_off) = atof (pTkn->token);
						//xdb_dbgprint ("%s %d\n", pField->fld_name, vi32);
						break;
					case XDB_TYPE_DOUBLE:
						XDB_EXPECT ((XDB_TOK_NUM == type), XDB_E_STMT, "Expect number");
						*(double*)(pRow+pField->fld_off) = atof (pTkn->token);
						//xdb_dbgprint ("%s %d\n", pField->fld_name, vi32);
						break;
					case XDB_TYPE_CHAR:
						XDB_EXPECT ((XDB_TOK_STR == type) && (pTkn->tk_len <= pField->fld_len), XDB_E_STMT, "Expect string <= %d", pField->fld_len);
						*(uint16_t*)(pRow+pField->fld_off-2) = pTkn->tk_len;
						memcpy (pRow+pField->fld_off, pTkn->token, pTkn->tk_len+1);
						//xdb_dbgprint ("%s %s\n", pField->fld_name, pTkn->token);
						break;
					case XDB_TYPE_VCHAR:
						XDB_EXPECT ((XDB_TOK_STR == type) && (pTkn->tk_len <= pField->fld_len), XDB_E_STMT, "Expect string <= %d", pField->fld_len);
						pStr = &pVStr[pField->fld_vid];
						pStr->len = pTkn->tk_len;
						pStr->str = pTkn->token;
						break;
					case XDB_TYPE_BINARY:
						XDB_EXPECT (((XDB_TOK_STR == type) || (XDB_TOK_HEX == type)) && (pTkn->tk_len <= pField->fld_len), XDB_E_STMT, "Expect binary <= %d", pField->fld_len);
						*(uint16_t*)(pRow+pField->fld_off-2) = pTkn->tk_len;
						memcpy (pRow+pField->fld_off, pTkn->token, pTkn->tk_len+1);
						//xdb_dbgprint ("%s %s\n", pField->fld_name, pTkn->token);
						break;
					case XDB_TYPE_VBINARY:
						XDB_EXPECT (((XDB_TOK_STR == type) || (XDB_TOK_HEX == type)) && (pTkn->tk_len <= pField->fld_len), XDB_E_STMT, "Expect binary <= %d", pField->fld_len);
						pStr = &pVStr[pField->fld_vid];
						pStr->len = pTkn->tk_len;
						pStr->str = pTkn->token;
						break;
					case XDB_TYPE_INET:
						XDB_EXPECT (XDB_TOK_STR == type, XDB_E_STMT, "Expect IP Addr");
						XDB_EXPECT (xdb_inet_scanf (pRow + pField->fld_off, pTkn->token),  XDB_E_STMT, "Invalid IP Addr");
						break;
					case XDB_TYPE_MAC:
						XDB_EXPECT (XDB_TOK_STR == type, XDB_E_STMT, "Expect MAC Addr");
						XDB_EXPECT (xdb_mac_scanf (pRow + pField->fld_off, pTkn->token),  XDB_E_STMT, "Invalid MAC Addr");
						break;
					}
				}
			} else if (XDB_TOK_QM == type) {
				pStmt->pBindRow[pStmt->bind_count] = pRow;
				pStmt->pBind[pStmt->bind_count++] = pField;
			} else {
				break;
			}
			type = xdb_next_token (pTkn);
		} while (XDB_TOK_COMMA == type);

		int null_u64 = XDB_ALIGN8(pTblm->null_bytes) >> 3;
		for (int i = 0; i < null_u64; ++i) {
			uint64_t nullL = ((uint64_t*)pNull)[i], nullR = ((uint64_t*)pTblm->pNullBytes)[i];
			XDB_EXPECT ((nullL&nullR) == nullR, XDB_E_STMT, "Expect NOT NULL fields with valid value");
		}

		XDB_EXPECT (XDB_TOK_RP == type, XDB_E_STMT, "Miss )");

		XDB_EXPECT (fld_seq >= pStmt->fld_count, XDB_E_STMT, "Too few values");
		type = xdb_next_token (pTkn);
		offset += row_len;
	} while (XDB_TOK_COMMA == type);

	return (xdb_stmt_t*)pStmt;

error:
	xdb_stmt_free ((xdb_stmt_t*)pStmt);
	return NULL;
}

static inline void 
xdb_init_where_stmt (xdb_stmt_select_t *pStmt)
{
#if 1
	// fast reset
	memset (&pStmt->col_count, 0, 8 * sizeof (pStmt->col_count));
#else
	pStmt->col_count 	= 0;
	pStmt->exp_count	= 0;
	pStmt->agg_count 	= 0;
	pStmt->order_count 	= 0;
	pStmt->bind_count 	= 0;
	pStmt->set_bind_count = 0;
	pStmt->set_count 	= 0;
	pStmt->reftbl_count 	= 0;
#endif

	pStmt->ref_tbl[0].bUseIdx = false;
	pStmt->ref_tbl[0].filter_count = 0;
	pStmt->ref_tbl[0].or_count = 0;
#if 0
	pStmt->ref_tbl[0].or_list[0].pIdxFilter	= NULL;
	pStmt->ref_tbl[0].or_list[0].filter_count  = 0;
#endif

	pStmt->limit	= XDB_MAX_ROWS;
	pStmt->offset	= 0;
}

XDB_STATIC int 
xdb_parse_orderby (xdb_conn_t* pConn, xdb_stmt_select_t *pStmt, xdb_token_t *pTkn)
{
	xdb_token_type type = xdb_next_token (pTkn);
	
	XDB_EXPECT ((XDB_TOK_ID == type) && !strcasecmp (pTkn->token, "BY"), XDB_E_STMT, "Expect ORDER BY");

	pStmt->order_count = 0;
	do {
		type = xdb_next_token (pTkn);
		if (XDB_TOK_ID == type) {
			xdb_field_t *pField = xdb_find_field (pStmt->pTblm, pTkn->token, pTkn->tk_len);
			XDB_EXPECT (pField != NULL, XDB_E_STMT, "Can't find field '%s'", pTkn->token);
			pStmt->pOrderFlds[pStmt->order_count] = pField;
			pStmt->bOrderDesc[pStmt->order_count] = false;
		} else {
			break;
		}
		type = xdb_next_token (pTkn);
		pStmt->order_count++;
		if (XDB_TOK_ID == type) {
			if (!strcasecmp (pTkn->token, "DESC")) {
				pStmt->bOrderDesc[pStmt->order_count-1] = true;
			} else if (strcasecmp (pTkn->token, "ASC")) {
				break;
			}
		}
		type = xdb_next_token (pTkn);
	} while (XDB_TOK_COMMA == type);

	return type;

error:
	return -XDB_E_STMT;
}

XDB_STATIC int 
xdb_parse_limit (xdb_conn_t* pConn, xdb_stmt_select_t *pStmt, xdb_token_t *pTkn)
{
	xdb_token_type type = xdb_next_token (pTkn);
	XDB_EXPECT (XDB_TOK_NUM == type, XDB_E_STMT, "Expect number for limit");
	pStmt->limit = atoi (pTkn->token);
	
	type = xdb_next_token (pTkn);
	if (((XDB_TOK_ID == type) && !strcasecmp (pTkn->token, "OFFSET")) || (XDB_TOK_COMMA == type)) {
		int offtype = type;
		type = xdb_next_token (pTkn);
		XDB_EXPECT (XDB_TOK_NUM == type, XDB_E_STMT, "Expect number for limit");
		pStmt->offset = atoi (pTkn->token);
		if (XDB_TOK_COMMA == offtype) {
			int offset = pStmt->limit;
			pStmt->limit = pStmt->offset;
			pStmt->offset = offset;
		}
		type = xdb_next_token (pTkn);
	}
	return type;

error:
	return -XDB_E_STMT;
}

XDB_STATIC xdb_token_type 
xdb_parse_val (xdb_stmt_select_t *pStmt, xdb_field_t *pField, xdb_value_t *pVal,  xdb_token_t *pTkn)
{
	xdb_token_type type;
	xdb_conn_t *pConn = pStmt->pConn;

	pVal->val_str.str = pTkn->token;
	pVal->val_str.len = pTkn->tk_len;
	pVal->pField = NULL;
	
	switch (pTkn->tk_type) {
	case XDB_TOK_NUM:
		if (xdb_likely (!pTkn->bFloat)) {
			pVal->ival = atoll (pTkn->token);
			pVal->val_type = XDB_TYPE_BIGINT;
			pVal->sup_type = XDB_TYPE_BIGINT;
		} else {
			pVal->fval = atof (pTkn->token);
			pVal->val_type = XDB_TYPE_DOUBLE;
			pVal->sup_type = XDB_TYPE_DOUBLE;
		}
		type = xdb_next_token (pTkn);
		break;
	case XDB_TOK_STR:
	case XDB_TOK_HEX:
		if (NULL != pField) {
			XDB_EXPECT(pTkn->tk_len <= pField->fld_len, XDB_E_STMT, "Too long string values %d > %d", pTkn->tk_len, pField->fld_len);
		}
		pVal->str.len = pTkn->tk_len;
		pVal->str.str = pTkn->token;
		pVal->val_type = XDB_TYPE_CHAR;
		type = xdb_next_token (pTkn);
		break;
	case XDB_TOK_ID:
		if (!strcasecmp(pTkn->token, "NULL")) {
			pVal->val_type = XDB_TYPE_NULL;
			type = xdb_next_token (pTkn);
			break;
		} else if (!strcasecmp(pTkn->token, "true")) {
			pVal->val_type = XDB_TYPE_BIGINT;
			pVal->sup_type = XDB_TYPE_BIGINT;
			pVal->ival = 1;
			type = xdb_next_token (pTkn);
			break;
		} else if (!strcasecmp(pTkn->token, "false")) {
			pVal->val_type = XDB_TYPE_BIGINT;
			pVal->sup_type = XDB_TYPE_BIGINT;
			pVal->ival = 0;
			type = xdb_next_token (pTkn);
			break;
		}
		if (pStmt->pTblm != NULL) {
			pVal->pField = xdb_find_field (pStmt->pTblm, pTkn->token, pTkn->tk_len);
			XDB_EXPECT(pVal->pField != NULL, XDB_E_STMT, "Can't find field '%s'", pTkn->token);
		}
		pVal->val_type = XDB_TYPE_FIELD;
		type = xdb_next_token (pTkn);
		if (XDB_TOK_DOT != type) {
			pVal->val_str2.str = NULL;
		} else {
			type = xdb_next_token (pTkn);
			XDB_EXPECT(XDB_TOK_ID == type, XDB_E_STMT, "Expect ID");
			pVal->val_str2 = pVal->val_str;
			pVal->val_str.str = pTkn->token;
			pVal->val_str.len = pTkn->tk_len;
			type = xdb_next_token (pTkn);			
		}
		break;
	default:
		XDB_SETERR (XDB_E_STMT, "Unkown token type");
		goto error;
	}

	return type;

error:
	return -1;
}

static xdb_token_type s_XDB_TOK_opposite[] = {
	[XDB_TOK_EQ] = XDB_TOK_EQ,
	[XDB_TOK_NE] = XDB_TOK_NE,
	[XDB_TOK_LT] = XDB_TOK_GT,
	[XDB_TOK_GT] = XDB_TOK_LT,
	[XDB_TOK_LE] = XDB_TOK_GE,
	[XDB_TOK_GE] = XDB_TOK_LE,
	[XDB_TOK_LIKE] = XDB_TOK_LIKE,
};

XDB_STATIC bool
xdb_find_idx (xdb_tblm_t	*pTblm, xdb_singfilter_t *pSigFlt, uint8_t 	bmp[])
{
	int fid;
	for (int i = 0; i < XDB_OBJM_COUNT(pTblm->idx_objm); ++i) {
		xdb_idxm_t *pIdxm = XDB_OBJM_GET(pTblm->idx_objm, pTblm->idx_order[i]);
		if (pSigFlt->filter_count < pIdxm->fld_count) {
			continue;
		}
		for (fid = 0 ; fid < pIdxm->fld_count; ++fid) {
			uint16_t fld_id = pIdxm->pFields[fid]->fld_id;
			if (!(bmp[fld_id>>3] & (1<<(fld_id&7)))) {
				break;
			}
		}
		if (fid == pIdxm->fld_count) {
			xdb_dbglog ("use index %s\n", XDB_OBJ_NAME(pIdxm));
			xdb_idxfilter_t *pIdxFilter = &pSigFlt->idx_filter;
			pIdxFilter->idx_flt_cnt = 0;
			if (xdb_unlikely (XDB_IDX_RBTREE == pIdxm->idx_type)) {
				pIdxFilter->match_opt = XDB_TOK_EQ;
				pIdxFilter->match_opt2 = -1;
				pIdxFilter->match_cnt = pIdxm->fld_count;
			}
			pIdxFilter->pIdxm = pIdxm;
			int 		idx_id = XDB_OBJ_ID(pIdxm);

			for (fid = 0; fid < pSigFlt->filter_count; ++fid) {
				xdb_filter_t *pFltr = pSigFlt->pFilters[fid];
				int idx_fid = pFltr->pField->idx_fid[idx_id];
				if (idx_fid >= 0) {
					// matched in index
					pIdxFilter->pIdxVals[idx_fid] = &pFltr->val;
				} else {
					// extra filters
					pIdxFilter->pIdxFlts[pIdxFilter->idx_flt_cnt++] = pFltr;
				}
			}
			pSigFlt->pIdxFilter = pIdxFilter;
			return true;
		}
	}

	return false;
}

XDB_STATIC int 
xdb_parse_where (xdb_conn_t* pConn, xdb_stmt_select_t *pStmt, xdb_token_t *pTkn)
{
	uint8_t 	bmp[XDB_MAX_COLUMN], i = 0;
	xdb_tblm_t	*pTblm = pStmt->pTblm;
	memset (bmp, 0, (pTblm->fld_count+7)>>3);
	xdb_token_type	type;
	xdb_token_type	vtype;
	int				vlen, flen;
	xdb_token_type		op;
	char 			*pVal, *pFldName, *pTblName = NULL;
	xdb_reftbl_t	*pRefTbl = &pStmt->ref_tbl[0];

	pRefTbl->or_count = 1;
	xdb_singfilter_t	*pSigFlt = &pRefTbl->or_list[0];
	pSigFlt->filter_count = 0;

	pRefTbl->bUseIdx = true;

	do {
next_filter:
		type = xdb_next_token (pTkn);
		if (xdb_likely (XDB_TOK_ID == type)) {
			pFldName = pTkn->token;
			flen = pTkn->tk_len;
			op = xdb_next_token (pTkn);
			if (xdb_unlikely (XDB_TOK_ID == op)) {
				if (!strcasecmp (pTkn->token, "LIKE")) {
					op = XDB_TOK_LIKE;
				} else if (!strcasecmp (pTkn->token, "BETWEEN")) {
					op = XDB_TOK_BTWN;
				}
			}
			pTblName = NULL;
			if (xdb_unlikely (XDB_TOK_DOT == op)) {
				pTblName = pFldName;
				op = xdb_next_token (pTkn);
				XDB_EXPECT (op == XDB_TOK_ID, XDB_E_STMT, "Expect ID");
				pFldName = pTkn->token;
				flen = pTkn->tk_len;
				op = xdb_next_token (pTkn);
			}
			XDB_EXPECT (op >= XDB_TOK_EQ && op <= XDB_TOK_NE, XDB_E_STMT, "Unsupported operator");
			vtype = xdb_next_token (pTkn);
			pVal = pTkn->token;
			vlen  = pTkn->tk_len;
		} else {
			vtype = type;
			pVal = pTkn->token;
			vlen  = pTkn->tk_len;
			op = xdb_next_token (pTkn);
			XDB_EXPECT (op >= XDB_TOK_EQ && op <= XDB_TOK_NE, XDB_E_STMT, "Unsupported operator");
			op = s_XDB_TOK_opposite[op];
			type = xdb_next_token (pTkn);
			XDB_EXPECT (XDB_TOK_ID == type, XDB_E_STMT, "One val must be field");
			pFldName = pTkn->token;
			flen = pTkn->tk_len;
		}
		xdb_field_t *pField = NULL;
		if (pTblName == NULL) {
			pField = xdb_find_field (pStmt->pTblm, pFldName, flen);
			for (int i = 1; i < pStmt->reftbl_count; ++i) {
				xdb_field_t *pField2 = xdb_find_field (pRefTbl[i].pRefTblm, pFldName, flen);
				XDB_EXPECT (NULL == pField2, XDB_E_STMT, "Ambiguous field '%s'", pFldName);
			}
		} else {
			for (i = 0; i < pStmt->reftbl_count; ++i) {
				if (pRefTbl[i].as_name) {
					if (!strcasecmp(pRefTbl[i].as_name, pTblName)) {
						break;
					}
				} else if (!strcasecmp(XDB_OBJ_NAME(pRefTbl[i].pRefTblm), pTblName)) {
					break;
				}
			}
			XDB_EXPECT (i < pStmt->reftbl_count, XDB_E_STMT, "No join table '%s'", pTblName);
			pField = xdb_find_field (pRefTbl[i].pRefTblm, pFldName, flen);
		}
		XDB_EXPECT (pField != NULL, XDB_E_STMT, "Can't find field '%s'", pFldName);
		if (xdb_unlikely (i > 0)) {
			pRefTbl = &pRefTbl[i];
			pTblm = pRefTbl->pRefTblm;
		}
		xdb_filter_t *pFilter = &pRefTbl->filters[pRefTbl->filter_count++];
		pSigFlt->pFilters[pSigFlt->filter_count++] = pFilter;
		//pFilter->fld_off	= pField->fld_off;
		//pFilter->fld_type	= pField->fld_type;
		pFilter->pField = pField;

		if (xdb_likely (XDB_TOK_EQ == op)) {
			bmp[pField->fld_id>>3] |= (1<<(pField->fld_id&7));
		}
		pFilter->cmp_op = op;

		if (xdb_unlikely (XDB_TOK_QM == vtype)) {
			pFilter->val.fld_type = pField->fld_type;
			pFilter->val.val_type = pField->sup_type;
			pStmt->pBind[pStmt->bind_count++] = &pFilter->val;
		} else {
			switch (pField->fld_type) {
			case XDB_TYPE_TIMESTAMP:
				if (XDB_TOK_STR == vtype) {
					pFilter->val.ival = xdb_timestamp_scanf (pVal);
					pFilter->val.val_type = XDB_TYPE_BIGINT;
					break;
				}
				// fall through
			case XDB_TYPE_INT:
			case XDB_TYPE_BIGINT:
			case XDB_TYPE_TINYINT:
			case XDB_TYPE_SMALLINT:
				XDB_EXPECT (XDB_TOK_NUM == vtype, XDB_E_STMT, "Expect Value");
				pFilter->val.ival = atoll (pVal);
				pFilter->val.val_type = XDB_TYPE_BIGINT;
				//xdb_dbgprint ("%s = %d\n", pField->fld_name.str, pFilter->val.ival);
				break;
			case XDB_TYPE_BOOL:
				XDB_EXPECT ((XDB_TOK_ID == vtype), XDB_E_STMT, "Expect TRUE/FALSE");
				if (!strcasecmp(pVal, "true")) {
					pFilter->val.ival = 1;
				} else {
					XDB_EXPECT (!strcasecmp(pVal, "false"), XDB_E_STMT, "Expect TRUE/FALSE");
					pFilter->val.ival = 0;
				}
				pFilter->val.val_type = XDB_TYPE_BIGINT;
				break;
			case XDB_TYPE_CHAR:
			case XDB_TYPE_VCHAR:
				XDB_EXPECT (XDB_TOK_STR == vtype, XDB_E_STMT, "Expect Value");
				pFilter->val.str.len = vlen;
				pFilter->val.str.str = pVal;
				pFilter->val.val_type = XDB_TYPE_CHAR;
				//xdb_dbgprint ("%s = %s\n", pField->fld_name.str, pFilter->val.str.str);
				break;
			case XDB_TYPE_BINARY:
			case XDB_TYPE_VBINARY:
				XDB_EXPECT (((XDB_TOK_STR == vtype) || (XDB_TOK_HEX == vtype)), XDB_E_STMT, "Expect Value");
				pFilter->val.str.len = vlen;
				pFilter->val.str.str = pVal;
				pFilter->val.val_type = XDB_TYPE_BINARY;
				//xdb_dbgprint ("%s = %s\n", pField->fld_name.str, pFilter->val.str.str);
				break;
			case XDB_TYPE_FLOAT:
			case XDB_TYPE_DOUBLE:
				XDB_EXPECT (XDB_TOK_NUM == vtype, XDB_E_STMT, "Expect Value");
				pFilter->val.fval = atof (pVal);
				pFilter->val.val_type = XDB_TYPE_DOUBLE;
				//xdb_dbgprint ("%s = %d\n", pField->fld_name.str, pFilter->val.ival);
				break;
			case XDB_TYPE_INET:
				XDB_EXPECT (xdb_inet_scanf (&pFilter->val.inet, pVal),  XDB_E_STMT, "Invalid IP Addr");
				pFilter->val.val_type = XDB_TYPE_INET;
				break;
			case XDB_TYPE_MAC:
				XDB_EXPECT (xdb_mac_scanf (&pFilter->val.mac, pVal),  XDB_E_STMT, "Invalid MAC Addr");
				pFilter->val.val_type = XDB_TYPE_MAC;
				break;
			}
		}
		type = xdb_next_token (pTkn);
		if (xdb_unlikely (XDB_TOK_ID != type)) {
			break;
		} else if (!strcasecmp (pTkn->token, "OR")) {
			if (pRefTbl->bUseIdx) {
				pRefTbl->bUseIdx = xdb_find_idx (pTblm, pSigFlt, bmp);
			}
			XDB_EXPECT (pRefTbl->or_count <= XDB_MAX_MATCH_OR, XDB_E_STMT, "Too many OR filters, MAX %d", XDB_MAX_MATCH_OR);
			pSigFlt = &pRefTbl->or_list[pRefTbl->or_count++];
			pSigFlt->filter_count = 0;
			memset (bmp, 0, sizeof(bmp));
			goto next_filter;
		}
	} while (!strcasecmp (pTkn->token, "AND"));

	if (pRefTbl->bUseIdx) {
		pRefTbl->bUseIdx = xdb_find_idx (pTblm, pSigFlt, bmp);
	}

	return type;

error:
	return -1;
}

XDB_STATIC xdb_ret 
xdb_parse_select_cols (xdb_conn_t *pConn, xdb_stmt_select_t *pStmt, int meta_size)
{
	xdb_tblm_t		*pTblm = pStmt->pTblm;

	//int	cols_off	= XDB_ALIGN4(meta_size + pTblm->pMeta->tbl_nmlen + 1);

	meta_size += sizeof (xdb_col_t) * pStmt->col_count + pTblm->pMeta->tbl_nmlen + 1 + 4;
	meta_size = XDB_ALIGN8 (meta_size);
	if (meta_size + pStmt->col_count * 8 <= sizeof (pStmt->set_flds)) {
		pStmt->pMeta = (void*)pStmt->set_flds;
	} else {
		pStmt->pMeta	= xdb_malloc (meta_size + pStmt->col_count * 8);
		XDB_EXPECT (pStmt->pMeta, XDB_E_MEMORY, "Can't alloc memory");
		pStmt->meta_size = meta_size; // alloc
	}
	pStmt->pMeta->col_count = pStmt->col_count;
	pStmt->pMeta->row_size = pTblm->pMeta->row_size;
	pStmt->pMeta->null_off = pTblm->pMeta->null_off;
	pStmt->pMeta->cols_off	= pTblm->pMeta->cols_off;
	//pStmt->pMeta->cols_off	= cols_off;
	memcpy (&pStmt->pMeta->tbl_nmlen, &pTblm->pMeta->tbl_nmlen, pTblm->pMeta->tbl_nmlen+2);

	xdb_col_t *pCol = (void*)pStmt->pMeta + pStmt->pMeta->cols_off;
	uint64_t *pColList = (void*)pStmt->pMeta + meta_size;
	pStmt->pMeta->col_list = (uintptr_t)pColList;
	if (xdb_likely (0 == pStmt->agg_count + pStmt->exp_count)) {
		for (int i = 0; i < pStmt->col_count; ++i) {
			xdb_selcol_t *pSelCol = &pStmt->sel_cols[i];
			xdb_str_t *pName = &pSelCol->as_name;
			xdb_value_t *pVal = &pSelCol->exp.op_val[0];
			xdb_str_t *pFld = &pVal->val_str;
			xdb_tblm_t *pTblmRef;
			if (NULL == pVal->val_str2.str) {
				pTblmRef = pTblm;
			} else {
				int refTblId;
				pTblmRef = xdb_stmt_find_table (pStmt, pVal->val_str2.str, &refTblId);
				XDB_EXPECT (pTblmRef != NULL, XDB_E_STMT, "table '%s' doesn't exist", pVal->val_str2.str);
			}
			xdb_field_t *pField = xdb_find_field (pTblmRef, pFld->str, pFld->len);
			XDB_EXPECT (pField != NULL, XDB_E_STMT, "field '%s' doesn't exist", pFld->str);
			xdb_col_t		*pColTbl = ((xdb_col_t**)pTblmRef->pMeta->col_list)[pField->fld_id];

			*pCol = *pColTbl;
			if (NULL == pName->str) {
				pName = pFld;
			} else {
				pCol->col_nmlen = pName->len;
				pCol->col_len = XDB_ALIGN4 (sizeof (*pCol) + pCol->col_nmlen + 1);
			}
			memcpy (pCol->col_name, pName->str, pName->len + 1);
			pColList[i] = (uintptr_t)pCol;
			//printf ("%p[%d]=%p\n", pColList, i, pCol);
			pCol = (void*)pCol + pCol->col_len;
		}
	} else {
		uint32_t	offset = 0;
		for (int i = 0; i < pStmt->col_count; ++i) {
			xdb_selcol_t *pSelCol = &pStmt->sel_cols[i];
			xdb_str_t *pName = &pSelCol->as_name;
			//xdb_str_t *pFld = &pSelCol->exp.op_val[0].val_str;
			xdb_value_t *pVal = &pSelCol->exp.op_val[0];
			xdb_value_t *pVal1;

			pCol->col_off	= offset;

			if (NULL != pName->str) {
				pCol->col_nmlen = pName->len;
				memcpy (pCol->col_name, pName->str, pName->len + 1);
			}

			if (XDB_TOK_NONE == pSelCol->exp.exp_op) {
				// single field
				if (NULL == pName->str) {
					pCol->col_nmlen = pVal->val_str.len;
					memcpy (pCol->col_name, pVal->val_str.str, pVal->val_str.len + 1);
				}
				if (XDB_TYPE_FIELD == pVal->val_type) {
					pVal->pField = xdb_find_field (pTblm, pVal->val_str.str, pVal->val_str.len);
					XDB_EXPECT (pVal->pField != NULL, XDB_E_STMT, "field '%s' doesn't exist", pVal->val_str.str);
					pCol->col_type	= pVal->pField->fld_type;
					offset			= XDB_ALIGN4 (offset + pVal->pField->fld_len);
					if (xdb_unlikely ((XDB_TYPE_CHAR == pCol->col_type) || (XDB_TYPE_BINARY == pCol->col_type))) {
						pCol->col_off	+= 2;
						offset			= XDB_ALIGN4 (offset + pVal->str.len + 3);
					}
				} else {
					pVal->pField	= NULL;
					pCol->col_type	= pVal->val_type;
					offset			= offset + 8;
				}
			} else if (pSelCol->exp.exp_op < XDB_TOK_COUNT) {
				// exp
				pVal1 = &pSelCol->exp.op_val[1];
				if (NULL == pName->str) {
					pCol->col_nmlen = sprintf (pCol->col_name, "%s%s%s", pVal->val_str.str, xdb_tok2str(pSelCol->exp.exp_op), pVal1->val_str.str);
				}

				xdb_type_t vtype, vtype1;
				if (XDB_TYPE_FIELD == pVal->val_type) {
					pVal->pField = xdb_find_field (pTblm, pVal->val_str.str, pVal->val_str.len);
					XDB_EXPECT (pVal->pField != NULL, XDB_E_STMT, "field '%s' doesn't exist", pVal->val_str.str);
					vtype = pVal->pField->sup_type;
				} else {
					pVal->pField	= NULL;
					vtype = pVal->val_type;
				}
				if (XDB_TYPE_FIELD == pVal1->val_type) {
					pVal1->pField = xdb_find_field (pStmt->pTblm, pVal1->val_str.str, pVal1->val_str.len);
					XDB_EXPECT (pVal1->pField != NULL, XDB_E_STMT, "field '%s' doesn't exist", pVal1->val_str.str);
					vtype1 = pVal1->pField->sup_type;
				} else {
					pVal1->pField	= NULL;
					vtype1 = pVal1->val_type;
				}

				if (XDB_TOK_DIV != pSelCol->exp.exp_op) {
					pCol->col_type	= vtype >= vtype1 ? vtype : vtype1;
					offset			= XDB_ALIGN4 (offset + 8);
				} else {
					pCol->col_type	= XDB_TYPE_DOUBLE;
					offset			= XDB_ALIGN4 (offset + 8);
				}
			} else {
				// agg
				pVal1 = &pSelCol->exp.op_val[1];
				if (xdb_likely (pVal1->val_str.str != NULL)) {
					pVal1->pField = xdb_find_field (pTblm, pVal1->val_str.str, pVal1->val_str.len);
					XDB_EXPECT (pVal1->pField != NULL, XDB_E_STMT, "field '%s' doesn't exist", pVal1->val_str.str);
					if (NULL == pName->str) {
						pCol->col_nmlen = sprintf (pCol->col_name, "%s(%s)", pVal->val_str.str, pVal1->val_str.str);
					}
				} else {
					pVal1->pField = NULL;
					if (NULL == pName->str) {
						pCol->col_nmlen = sprintf (pCol->col_name, "%s(*)", pVal->val_str.str);
					}
				}
				switch (pSelCol->exp.exp_op) {
				case XDB_TOK_COUNT:
					pCol->col_type	= XDB_TYPE_BIGINT;
					offset			= XDB_ALIGN4 (offset + 8);
					break;
				case XDB_TOK_SUM:
					pCol->col_type = pVal1->pField->sup_type;
					offset			= XDB_ALIGN4 (offset + 8);
					break;
				case XDB_TOK_AVG:
					pCol->col_type	= XDB_TYPE_DOUBLE;
					offset			= XDB_ALIGN4 (offset + 8);
					break;
				case XDB_TOK_MIN:
				case XDB_TOK_MAX:
					pCol->col_type	= pVal1->pField->fld_type;
					offset			= XDB_ALIGN4 (offset + pVal1->pField->fld_len);
					break;
				default:
					break;
				}
			} // end agg

			pCol->col_len = XDB_ALIGN4 (sizeof (*pCol) + pCol->col_nmlen + 1);
			pColList[i] = (uintptr_t)pCol;
			pCol = (void*)pCol + pCol->col_len;
		}

		pStmt->pMeta->null_off = offset;
		offset = XDB_ALIGN4(offset + ((pStmt->col_count+7)>>3) + 2);
		pStmt->pMeta->row_size = offset;
	}

	//*(int*)pCol = 0;
	//xdb_hexdump (pStmt->pMeta, meta_size);

	pStmt->pMeta->len_type = meta_size | (XDB_RET_META<<28);

	return XDB_OK;

error:
	return -1;
}

#if 0
XDB_STATIC xdb_field_t* 
xdb_parse_tblfldname (xdb_conn_t *pConn, xdb_stmt_select_t *pStmt, xdb_token_t *pTkn, int *pRefTblId)
{
	char 	*fld_name = pTkn->token;
	int		len = pTkn->tk_len;
	int 	type = xdb_next_token (pTkn);
	
	if (XDB_TOK_DOT == type) {
		xdb_tblm_t *pTblm = xdb_stmt_find_table (pStmt, fld_name, pRefTblId);
		XDB_EXPECT (NULL != pTblm, XDB_E_NOTFOUND, "Table '%s' doesn't exist", fld_name);
		type = xdb_next_token (pTkn);
		XDB_EXPECT (type <= XDB_TOK_STR, XDB_E_STMT, "Miss field name");
		fld_name = pTkn->token;
		len = pTkn->tk_len;
		type = xdb_next_token (pTkn);
		xdb_field_t *pField = xdb_find_field (pTblm, fld_name, len);
		XDB_EXPECT (NULL != pField, XDB_E_NOTFOUND, "Field '%s' doesn't exist", fld_name);
		return pField;
	} else {
		return xdb_stmt_find_field (pConn, pStmt, fld_name, len, pRefTblId);
	}

error:
	return NULL;
}
#endif

XDB_STATIC xdb_stmt_t* 
xdb_parse_select (xdb_conn_t* pConn, xdb_token_t *pTkn, bool bPStmt)
{
	int 	rc;
	xdb_stmt_select_t 	*pStmt;
	xdb_value_t 		*pVal;
	xdb_value_t 		*pVal1;

	if (xdb_unlikely (bPStmt)) {
		pStmt = xdb_malloc (sizeof (*pStmt));
		XDB_EXPECT(NULL != pStmt, XDB_E_MEMORY, "Run out of memory");
	} else {
		pStmt = &pConn->stmt_union.select_stmt;
	}

	pStmt->stmt_type = XDB_STMT_SELECT;
	pStmt->pSql = NULL;
	pStmt->meta_size = 0; // no alloc
	pStmt->pTblm = NULL;

	xdb_init_where_stmt (pStmt);

	xdb_token_type		type;
	int					meta_size = sizeof (xdb_meta_t);
	int 				nmlen;

	// fields
	do {
		type = xdb_next_token (pTkn);

		if (XDB_TOK_MUL == type) {
			type = xdb_next_token (pTkn);
			break;
		}

		XDB_EXPECT (pStmt->col_count < XDB_ARY_LEN(pStmt->sel_cols), XDB_E_STMT, "Too many fields");
		xdb_selcol_t *pSelCol = &pStmt->sel_cols[pStmt->col_count++];
		pSelCol->as_name.str = NULL;

		pVal = &pSelCol->exp.op_val[0];
		type = xdb_parse_val (pStmt, NULL, pVal, pTkn);
		XDB_EXPECT2 (type >= 0);

		pSelCol->exp.exp_op = XDB_TOK_NONE;
		nmlen = XDB_ALIGN4 (pTkn->tk_len + 1);

		if (xdb_likely (XDB_TOK_COMMA == type)) {
			// fast hit
			if (pVal->val_type != XDB_TYPE_FIELD) {
				pStmt->exp_count++;
			}
		} else if (type >= XDB_TOK_ADD && type <= XDB_TOK_DIV) {
			// exp
			pSelCol->exp.exp_op = type;
			xdb_next_token (pTkn);
			pVal1 = &pSelCol->exp.op_val[1];
			type = xdb_parse_val (pStmt, NULL, pVal1, pTkn);
			XDB_EXPECT2 (type >= 0);
			nmlen = XDB_ALIGN4 (pVal->val_str.len + pVal1->val_str.len + 1 + 1);
			pStmt->exp_count++;
		} else if (XDB_TOK_LP == type) {
			// agg func
			xdb_str_t *pFunc = &pVal->val_str;
			if (!strcasecmp(pFunc->str, "COUNT")) {
				pSelCol->exp.exp_op = XDB_TOK_COUNT;
			} else if (!strcasecmp(pFunc->str, "MAX")) {
				pSelCol->exp.exp_op = XDB_TOK_MAX;
			} else if (!strcasecmp(pFunc->str, "MIN")) {
				pSelCol->exp.exp_op = XDB_TOK_MIN;
			} else if (!strcasecmp(pFunc->str, "SUM")) {
				pSelCol->exp.exp_op = XDB_TOK_SUM;
			} else if (!strcasecmp(pFunc->str, "AVG")) {
				pSelCol->exp.exp_op = XDB_TOK_AVG;
			} else {
				XDB_EXPECT (0, XDB_E_STMT, "Unkonwn ID '%s'", pFunc->str);
			}
			type = xdb_next_token (pTkn);
			pVal1 = &pSelCol->exp.op_val[1];
			if (XDB_TOK_MUL == type) {
				XDB_EXPECT (XDB_TOK_COUNT == pSelCol->exp.exp_op, XDB_E_STMT, "%s can't use '*'", pFunc->str);
				pVal1->val_str.str = NULL;
				pVal1->val_str.len = 1;
			} else {
				pVal1->val_str.str = pTkn->token;
				pVal1->val_str.len = pTkn->tk_len;
			}
			type = xdb_next_token (pTkn);
			XDB_EXPECT (XDB_TOK_RP == type, XDB_E_STMT, "Expect )");
			pStmt->agg_count++;
			nmlen = XDB_ALIGN4 (pFunc->len + 2 + pVal1->val_str.len + 1); // COUNT(fld)+`\0`
			type = xdb_next_token (pTkn);
		}

		if (xdb_unlikely (XDB_TOK_ID == type)) {
			if (!strcasecmp(pTkn->token, "AS")) {
				type = xdb_next_token (pTkn);
				XDB_EXPECT (XDB_TOK_ID == type, XDB_E_STMT, "Expect ID");
				pSelCol->as_name.str = pTkn->token;
				pSelCol->as_name.len = pTkn->tk_len;
				nmlen = XDB_ALIGN4 (pTkn->tk_len + 1);
				type = xdb_next_token (pTkn);
			} else {
				if (pVal->val_type != XDB_TYPE_FIELD) {
					pStmt->exp_count++;
				}
				meta_size += nmlen;
				break;
			}
		}

		meta_size += nmlen;
	} while (XDB_TOK_COMMA == type);

	XDB_EXPECT ((XDB_TOK_ID == type) && !strcasecmp (pTkn->token, "FROM"), XDB_E_STMT, "Except FROM");

	do {
		type = xdb_next_token (pTkn);
		XDB_EXPECT (type <= XDB_TOK_STR, XDB_E_STMT, "Miss table name");
		// [db_name.]tbl_name
		XDB_PARSE_DBTBLNAME();
		pStmt->ref_tbl[pStmt->reftbl_count++].pRefTblm = pStmt->pTblm;
	} while (XDB_TOK_COMMA == type);

	while ((XDB_TOK_ID == type) && !strcasecmp (pTkn->token, "JOIN")) {
		type = xdb_next_token (pTkn);
		xdb_reftbl_t *pRefTbl = &pStmt->ref_tbl[pStmt->reftbl_count];
		pRefTbl->join_type = XDB_JOIN_INNER;
		pRefTbl->pRefTblm = xdb_parse_dbtblname (pConn, pTkn);
		XDB_EXPECT2 (pRefTbl->pRefTblm != NULL);
		XDB_EXPECT ((XDB_TOK_ID == pTkn->tk_type) && !strcasecmp (pTkn->token, "ON"), XDB_E_STMT, "Except JOIN ON");
		pRefTbl->field_count = 0;

		pStmt->reftbl_count++;

		type = xdb_parse_where (pConn, pStmt, pTkn);
		XDB_EXPECT2 (type >= 0);

#if 0
		do {
			type = xdb_next_token (pTkn);
			XDB_EXPECT (XDB_TOK_ID == type, XDB_E_STMT, "Except field name");
			xdb_field_t *pField = xdb_parse_tblfldname (pConn, pStmt, pTkn);
			XDB_EXPECT2 (pField != NULL);
			pRefTbl->pField[pRefTbl->field_count] = pField;
			XDB_EXPECT (XDB_TOK_EQ == pTkn->tk_type, XDB_E_STMT, "Except =");
			type = xdb_next_token (pTkn);
			pField = xdb_parse_tblfldname (pConn, pRefTbl->pRefTblm, pTkn);
			XDB_EXPECT2 (pField != NULL);
			type = pTkn->tk_type;
			pRefTbl->pJoinField[pRefTbl->field_count] = pField;
			pRefTbl->field_count++;
		} while ((XDB_TOK_ID == type) && !strcasecmp (pTkn->token, "AND"));
#endif

	}

	if (0 == pStmt->col_count) {
		if (xdb_likely (1 == pStmt->reftbl_count)) {
			pStmt->pMeta = pStmt->pTblm->pMeta;
			pStmt->col_count = pStmt->pTblm->fld_count;
			pStmt->meta_size = 0;
		} else {
			pStmt->col_count = 0;
			int row_size = 0;
			meta_size = 0;
			for (int jt = 0; jt < pStmt->reftbl_count; ++jt) {
				xdb_reftbl_t *pJoin = &pStmt->ref_tbl[jt];
				pStmt->col_count += pJoin->pRefTblm->fld_count;
				row_size += pJoin->pRefTblm->row_size;
				meta_size += pJoin->pRefTblm->meta_size - sizeof(xdb_meta_t) - 4;
//xdb_dbgprint ("%s col %d row size %d meta len %d\n", XDB_OBJ_NAME(pJoin->pRefTblm), pJoin->pRefTblm->fld_count, pJoin->pRefTblm->row_size, pJoin->pRefTblm->meta_size);
			}
			meta_size += 4;
			meta_size = XDB_ALIGN8 (meta_size);
			if (meta_size + pStmt->col_count * 8 <= sizeof (pStmt->set_flds)) {
				pStmt->pMeta = (void*)pStmt->set_flds;
			} else {
				pStmt->pMeta	= xdb_malloc (meta_size + pStmt->col_count * 8);
				XDB_EXPECT (pStmt->pMeta, XDB_E_MEMORY, "Can't alloc memory");
				pStmt->meta_size = meta_size; // alloc
			}
			xdb_col_t *pCol = (xdb_col_t*)(pStmt->pMeta + 1);
			uint64_t *pColList = (void*)pStmt->pMeta + meta_size;

			int fld_count = 0, row_offset = 0;
			void *pJoinMeta = (xdb_col_t*)(pStmt->pMeta + 1);

			for (int jt = 0; jt < pStmt->reftbl_count; ++jt) {
				xdb_reftbl_t *pJoin = &pStmt->ref_tbl[jt];
				int meta_len = pJoin->pRefTblm->meta_size - sizeof(xdb_meta_t) - 4; // last 4B is 0 eof
				memcpy (pJoinMeta, pJoin->pRefTblm->pMeta + 1, meta_len);
				pJoinMeta += meta_len;

				if (row_offset) {
					// adjust offset
					for (int i = 0; i < pJoin->pRefTblm->fld_count; ++i) {
						pColList[fld_count++] = (uintptr_t)pCol;
						pCol->col_off += row_offset;
						pCol = (void*)pCol + pCol->col_len;
					}
				}
				row_offset += pJoin->pRefTblm->row_size;
			}
			*(int*)pCol = 0; // eof

//xdb_dbgprint ("col %d row size %d meta len %d colptr %p %p %d\n", pStmt->col_count, row_size, meta_size, pColList, pCol, (int)((void*)pCol-(void*)pStmt->pMeta));
			pStmt->pMeta->len_type = meta_size | (XDB_RET_META<<28);
			pStmt->pMeta->col_count = pStmt->col_count;
			pStmt->pMeta->null_off = row_size;
			row_size += ((pStmt->pMeta->col_count+7)>>3) + 2;
			row_size = XDB_ALIGN4(row_size);
			pStmt->pMeta->row_size = row_size;
			pStmt->pMeta->col_list = (uintptr_t)pColList;
		}
	} else {
		rc = xdb_parse_select_cols (pConn, pStmt, meta_size);
		XDB_EXPECT2(XDB_OK == rc);
	}

	if ((XDB_TOK_ID == type) && !strcasecmp (pTkn->token, "WHERE")) {
		type = xdb_parse_where (pConn, pStmt, pTkn);
		if (xdb_likely (type >= XDB_TOK_END)) {
			return (xdb_stmt_t*)pStmt;
		}
		XDB_EXPECT2 (type >= 0);
	}

	if ((XDB_TOK_ID == type) && !strcasecmp (pTkn->token, "ORDER")) {
		type = xdb_parse_orderby (pConn, pStmt, pTkn);
		XDB_EXPECT2 (type >= 0);
	}

	if ((XDB_TOK_ID == type) && !strcasecmp (pTkn->token, "LIMIT")) {
		type = xdb_parse_limit (pConn, pStmt, pTkn);
		XDB_EXPECT2 (type >= 0);
	}

	XDB_EXPECT (type >= XDB_TOK_END, XDB_E_STMT, "Unkown token");

	return (xdb_stmt_t*)pStmt;

error:
	xdb_stmt_free ((xdb_stmt_t*)pStmt);
	return NULL;
}

XDB_STATIC xdb_token_type 
xdb_parse_setcol (xdb_stmt_select_t *pStmt, xdb_token_t *pTkn)
{
	xdb_tblm_t *pTblm = pStmt->pTblm;
	xdb_conn_t	*pConn = pStmt->pConn;
	xdb_token_type	type;
	
	do {
		type = xdb_next_token (pTkn);
		XDB_EXPECT((type==XDB_TOK_ID), XDB_E_STMT, "Miss field");

		xdb_field_t *pField = xdb_find_field (pTblm, pTkn->token, pTkn->tk_len);
		XDB_EXPECT(pField != NULL, XDB_E_STMT, "Can't find field '%s'", pTkn->token);
		xdb_setfld_t *pSet = &pStmt->set_flds[pStmt->set_count++];
		pSet->pField = pField;
		
		type = xdb_next_token (pTkn);
		XDB_EXPECT(type==XDB_TOK_EQ, XDB_E_STMT, "Miss =");

		type = xdb_next_token (pTkn);
		if (xdb_unlikely (XDB_TOK_QM == type)) {
			pSet->exp.op_val[0].fld_type = pField->fld_type;
			pSet->exp.op_val[0].val_type = pField->sup_type;
			pSet->exp.op_val[0].sup_type = pField->sup_type;
			pStmt->pBind[pStmt->bind_count++] = &pSet->exp.op_val[0];			
			type = xdb_next_token (pTkn);
		} else {
			XDB_EXPECT (type<=XDB_TOK_NUM, XDB_E_STMT, "Miss ID/STR/NUM");
			type = xdb_parse_val (pStmt, pField, &pSet->exp.op_val[0], pTkn);
		}
		pSet->exp.exp_op = XDB_TOK_NONE;
		if (xdb_unlikely ((type >= XDB_TOK_ADD) && (type <= XDB_TOK_DIV))) {
			pSet->exp.exp_op = type ;
			type = xdb_next_token (pTkn);
			if (xdb_unlikely (XDB_TOK_QM == type)) {
				pSet->exp.op_val[1].fld_type = pField->fld_type;
				pSet->exp.op_val[1].val_type = pField->sup_type;
				pSet->exp.op_val[1].sup_type = pField->sup_type;
				pStmt->pBind[pStmt->bind_count++] = &pSet->exp.op_val[1];
				type = xdb_next_token (pTkn);
			} else {
				XDB_EXPECT(type<=XDB_TOK_NUM, XDB_E_STMT, "Miss ID/STR/NUM");
				type = xdb_parse_val (pStmt, NULL, &pSet->exp.op_val[1], pTkn);
			}
		}
	} while (XDB_TOK_COMMA == type);

	return type;

error:
	return -1;
}

XDB_STATIC xdb_stmt_t* 
xdb_parse_update (xdb_conn_t* pConn, xdb_token_t *pTkn, bool bPStmt)
{
	xdb_stmt_select_t 	*pStmt;
	if (xdb_unlikely (bPStmt)) {
		pStmt = xdb_malloc (sizeof (*pStmt));
		XDB_EXPECT(NULL != pStmt, XDB_E_MEMORY, "Run out of memory");
	} else {
		pStmt = &pConn->stmt_union.select_stmt;
	}
	pStmt->stmt_type = XDB_STMT_UPDATE;
	pStmt->pSql = NULL;

	xdb_init_where_stmt (pStmt);

	xdb_token_type	type = xdb_next_token (pTkn);

	XDB_EXPECT(type<=XDB_TOK_STR, XDB_E_STMT, "Miss table name");

	XDB_PARSE_DBTBLNAME();

	pStmt->ref_tbl[0].pRefTblm = pStmt->pTblm;

	if (xdb_unlikely (pStmt->pTblm->pDbm->bSysDb)) {
		XDB_EXPECT (pConn == s_xdb_sysdb_pConn, XDB_E_CONSTRAINT, "Can't update table '%s' in system database", XDB_OBJ_NAME(pStmt->pTblm));
	}

	XDB_EXPECT((XDB_TOK_ID==type) && !strcasecmp(pTkn->token, "SET"), XDB_E_STMT, "Expect SET");

	type = xdb_parse_setcol (pStmt, pTkn);
	XDB_EXPECT2 (type >= 0);

	if ((XDB_TOK_ID == type) && !strcasecmp(pTkn->token, "WHERE")) {
		type = xdb_parse_where (pConn, pStmt, pTkn);
		XDB_EXPECT2 (type >= 0);
	}

	if ((XDB_TOK_ID == type) && !strcasecmp (pTkn->token, "LIMIT")) {
		type = xdb_parse_limit (pConn, pStmt, pTkn);
		XDB_EXPECT2 (type >= 0);
	}

	XDB_EXPECT (type >= XDB_TOK_END, XDB_E_STMT, "Unkown token");

	return (xdb_stmt_t*)pStmt;

error:
	xdb_stmt_free ((xdb_stmt_t*)pStmt);
	return NULL;
}

XDB_STATIC xdb_stmt_t* 
xdb_parse_delete (xdb_conn_t* pConn, xdb_token_t *pTkn, bool bPStmt)
{
	xdb_token_type	type = xdb_next_token (pTkn);
	xdb_stmt_select_t *pStmt;

	if (xdb_unlikely (bPStmt)) {
		pStmt = xdb_malloc (sizeof (*pStmt));
		XDB_EXPECT(NULL != pStmt, XDB_E_MEMORY, "Run out of memory");
	} else {
		pStmt = &pConn->stmt_union.select_stmt;
	}
	pStmt->stmt_type = XDB_STMT_DELETE;
	pStmt->pSql = NULL;

	xdb_init_where_stmt (pStmt);

	XDB_EXPECT((XDB_TOK_ID==type) && !strcasecmp(pTkn->token, "FROM"), XDB_E_STMT, "Expect FROM");

	type = xdb_next_token (pTkn);

	XDB_EXPECT(type<=XDB_TOK_STR, XDB_E_STMT, "Miss table name");

	XDB_PARSE_DBTBLNAME();

	pStmt->ref_tbl[0].pRefTblm = pStmt->pTblm;

	if (xdb_unlikely (pStmt->pTblm->pDbm->bSysDb)) {
		XDB_EXPECT (pConn == s_xdb_sysdb_pConn, XDB_E_CONSTRAINT, "Can't delete form table '%s' in system database", XDB_OBJ_NAME(pStmt->pTblm));
	}

	if ((XDB_TOK_ID == type) && !strcasecmp(pTkn->token, "WHERE")) {
		type = xdb_parse_where (pConn, pStmt, pTkn);
		XDB_EXPECT2 (type >= 0);
	}

	if ((XDB_TOK_ID == type) && !strcasecmp (pTkn->token, "LIMIT")) {
		type = xdb_parse_limit (pConn, pStmt, pTkn);
		XDB_EXPECT2 (type >= 0);
	}

	XDB_EXPECT (type >= XDB_TOK_END, XDB_E_STMT, "Unkown token");

	return (xdb_stmt_t*)pStmt;

error:
	xdb_stmt_free ((xdb_stmt_t*)pStmt);
	return NULL;
}

XDB_STATIC xdb_stmt_t* 
xdb_parse_audit (xdb_conn_t* pConn, xdb_token_t *pTkn)
{
	xdb_stmt_select_t 	*pStmt = &pConn->stmt_union.select_stmt;

	pStmt->stmt_type = 0;
	pStmt->pSql = NULL;

	xdb_token_type	type = xdb_next_token (pTkn);
	XDB_EXPECT (type == XDB_TOK_ID, XDB_E_STMT, "Miss action");

	if (! strcasecmp (pTkn->token, "MARK")) {
		pStmt->stmt_type = XDB_STMT_AUDIT_MARK;
	} else if (! strcasecmp (pTkn->token, "SWEEP")) {
		pStmt->stmt_type = XDB_STMT_AUDIT_SWEEP;
	}

	type = xdb_next_token (pTkn);

	XDB_EXPECT(type<=XDB_TOK_STR, XDB_E_STMT, "Miss table name");

	XDB_PARSE_DBTBLNAME();

	return (xdb_stmt_t*)pStmt;

error:
	xdb_stmt_free ((xdb_stmt_t*)pStmt);
	return NULL;
}
