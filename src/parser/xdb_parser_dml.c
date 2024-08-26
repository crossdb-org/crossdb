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
xdb_parse_insert (xdb_conn_t* pConn, xdb_token_t *pTkn, bool bPStmt)
{
	xdb_stmt_insert_t *pStmt;

	if (xdb_unlikely (bPStmt)) {
		pStmt = xdb_malloc (sizeof (*pStmt));
		XDB_EXPECT(NULL != pStmt, XDB_E_MEMORY, "Run out of memory");
	} else {
		pStmt = &pConn->stmt_union.insert_stmt;
	}

	pStmt->stmt_type = XDB_STMT_INSERT;
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

	if (XDB_TOK_LP == type) {
		bColList = true;
		// col list
		do {
			type = xdb_next_token (pTkn);
		 	if (XDB_TOK_ID == type) {
				int fld_id = xdb_find_field (pTblm, pTkn->token, pTkn->tk_len);
				XDB_EXPECT (fld_id >= 0, XDB_E_STMT, "field '%s' doesn't exist", pTkn->token);
				pStmt->fld_list[pStmt->fld_count++] = fld_id;
		 	} else {
		 		break;
		 	}
			type = xdb_next_token (pTkn);
		} while (XDB_TOK_COMMA == type);

		XDB_EXPECT (XDB_TOK_RP == type, XDB_E_STMT, "Miss )");
		type = xdb_next_token (pTkn);
	} else {
		pStmt->fld_count = pTblm->fld_count;
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

	// Multiple rows
	do {
		// TBD: increase buf if less
		void *pRow = pStmt->pRowsBuf + offset;

		if (xdb_unlikely (offset + pTblm->row_size > pStmt->buf_len)) {
			uint32_t buf_len = pStmt->buf_len <<= 1;
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

		// TBD copy from default?
		memset (pRow, 0, pTblm->row_size);

		pStmt->row_offset[pStmt->row_count++] = offset;
		type = xdb_next_token (pTkn);

		XDB_EXPECT (XDB_TOK_LP == type, XDB_E_STMT, "Miss (");

		// Single row values
		int fld_seq = 0;			
		do {
			type = xdb_next_token (pTkn);
			int fld_id = bColList ? pStmt->fld_list[fld_seq] : fld_seq;
			XDB_EXPECT (++fld_seq <= pStmt->fld_count, XDB_E_STMT, "Too many values");
			xdb_field_t *pFld = &pTblm->pFields[fld_id];

			if (type <= XDB_TOK_NUM) {
				switch (pFld->fld_type) {
				case XDB_TYPE_INT:
					XDB_EXPECT ((XDB_TOK_NUM == type), XDB_E_STMT, "Expect number");
					*(int32_t*)(pRow+pFld->fld_off) = atoi (pTkn->token);
					//xdb_print ("%s %d\n", pFld->fld_name, vi32);
					break;
				case XDB_TYPE_BIGINT:
					XDB_EXPECT ((XDB_TOK_NUM == type), XDB_E_STMT, "Expect number");
					*(int64_t*)(pRow+pFld->fld_off) = atoll (pTkn->token);
					//xdb_print ("%s %d\n", pFld->fld_name, vi32);
					break;
				case XDB_TYPE_TINYINT:
					XDB_EXPECT ((XDB_TOK_NUM == type), XDB_E_STMT, "Expect number");
					*(int8_t*)(pRow+pFld->fld_off) = atoi (pTkn->token);
					//xdb_print ("%s %d\n", pFld->fld_name, vi32);
					break;
				case XDB_TYPE_SMALLINT:
					XDB_EXPECT ((XDB_TOK_NUM == type), XDB_E_STMT, "Expect number");
					*(int16_t*)(pRow+pFld->fld_off) = atoi (pTkn->token);
					//xdb_print ("%s %d\n", pFld->fld_name, vi32);
					break;
				case XDB_TYPE_FLOAT:
					XDB_EXPECT ((XDB_TOK_NUM == type), XDB_E_STMT, "Expect number");
					*(float*)(pRow+pFld->fld_off) = atof (pTkn->token);
					//xdb_print ("%s %d\n", pFld->fld_name, vi32);
					break;
				case XDB_TYPE_DOUBLE:
					XDB_EXPECT ((XDB_TOK_NUM == type), XDB_E_STMT, "Expect number");
					*(double*)(pRow+pFld->fld_off) = atof (pTkn->token);
					//xdb_print ("%s %d\n", pFld->fld_name, vi32);
					break;
				case XDB_TYPE_CHAR:
					XDB_EXPECT ((XDB_TOK_STR == type) && (pTkn->tk_len <= pFld->fld_len), XDB_E_STMT, "Expect string <= %d", pFld->fld_len);
					*(uint16_t*)(pRow+pFld->fld_off-2) = pTkn->tk_len;
					memcpy (pRow+pFld->fld_off, pTkn->token, pTkn->tk_len+1);
					//xdb_print ("%s %s\n", pFld->fld_name, pTkn->token);
					break;
				}
			} else if (XDB_TOK_QM == type) {
				pStmt->pBindRow[pStmt->bind_count] = pRow;
				pStmt->pBind[pStmt->bind_count++] = pFld;
			} else {
				break;
			}
			type = xdb_next_token (pTkn);
		} while (XDB_TOK_COMMA == type);

		XDB_EXPECT (XDB_TOK_RP == type, XDB_E_STMT, "Miss )");

		XDB_EXPECT (fld_seq >= pStmt->fld_count, XDB_E_STMT, "Too few values");
		type = xdb_next_token (pTkn);
		offset += pTblm->row_size;

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
	pStmt->filter_count = 0;
	pStmt->bind_count = 0;
	pStmt->idx_flt_cnt = 0;
	pStmt->order_count = 0;
	pStmt->agg_count = 0;
	pStmt->col_count = 0;
	pStmt->set_bind_count = 0;
	pStmt->set_count = 0;
#endif

	pStmt->pIdxm	= NULL;
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
			int fld_id = xdb_find_field (pStmt->pTblm, pTkn->token, pTkn->tk_len);
			XDB_EXPECT (fld_id>=0, XDB_E_STMT, "Can't find field '%s'", pTkn->token);
			pStmt->pOrderFlds[pStmt->order_count] = &pStmt->pTblm->pFields[fld_id];
			pStmt->bOrderDesc[pStmt->order_count] = false;
		} else {
			break;
		}
		type = xdb_next_token (pTkn);
		if (XDB_TOK_ID == type) {
			if (!strcasecmp (pTkn->token, "DESC")) {
				pStmt->bOrderDesc[pStmt->order_count] = true;
			} else {
				XDB_EXPECT (!strcasecmp (pTkn->token, "ASC"), XDB_E_STMT, "Expect DESC | ASC");
			}
		}
		pStmt->order_count++;
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
	}
	return type;

error:
	return -XDB_E_STMT;
}

XDB_STATIC int 
xdb_parse_where (xdb_conn_t* pConn, xdb_stmt_select_t *pStmt, xdb_token_t *pTkn)
{
	uint8_t bmp[8];
	xdb_tblm_t	*pTblm = pStmt->pTblm;
	memset (bmp, 0 , sizeof(bmp));
	xdb_token_type type;
	
	do {
		type = xdb_next_token (pTkn);
		if (XDB_TOK_ID != type) {
			break;
		}
		int fld_id = xdb_find_field (pStmt->pTblm, pTkn->token, pTkn->tk_len);
		XDB_EXPECT (fld_id>=0, XDB_E_STMT, "Can't find field '%s'", pTkn->token);
		xdb_filter_t *pFilter = &pStmt->filters[pStmt->filter_count];
		pStmt->pFilters[pStmt->filter_count++] = pFilter;
		//pFilter->fld_off	= pField->fld_off;
		//pFilter->fld_type	= pField->fld_type;
		xdb_field_t *pField = &pStmt->pTblm->pFields[fld_id];
		bmp[fld_id>>3] |= (1<<(fld_id&7));
		pFilter->pField = pField;

		type = xdb_next_token (pTkn);
		XDB_EXPECT (XDB_TOK_EQ == type, XDB_E_STMT, "Miss =");
		pFilter->cmp_op = XDB_OP_EQ;
		type = xdb_next_token (pTkn);

		if (xdb_unlikely (XDB_TOK_QM == type)) {
			pFilter->val.fld_type = pField->fld_type;
			pFilter->val.val_type = pField->sup_type;
			pStmt->pBind[pStmt->bind_count++] = &pFilter->val;
		} else {
			switch (pField->fld_type) {
			case XDB_TYPE_INT:
			case XDB_TYPE_BIGINT:
			case XDB_TYPE_TINYINT:
			case XDB_TYPE_SMALLINT:
				pFilter->val.ival = atoll (pTkn->token);
				pFilter->val.val_type = XDB_TYPE_BIGINT;
				//xdb_print ("%s = %d\n", pField->fld_name.str, pFilter->val.ival);
				break;
			case XDB_TYPE_CHAR:
				pFilter->val.str.len = pTkn->tk_len;
				pFilter->val.str.ptr = pTkn->token;
				pFilter->val.val_type = XDB_TYPE_CHAR;
				//xdb_print ("%s = %s\n", pField->fld_name.str, pFilter->val.str.ptr);
				break;
			case XDB_TYPE_FLOAT:
			case XDB_TYPE_DOUBLE:
				pFilter->val.fval = atof (pTkn->token);
				pFilter->val.val_type = XDB_TYPE_DOUBLE;
				//xdb_print ("%s = %d\n", pField->fld_name.str, pFilter->val.ival);
				break;
			}
		}
		type = xdb_next_token (pTkn);
		if (XDB_TOK_ID != type) {
			break;
		}
		XDB_EXPECT (!strcasecmp (pTkn->token, "AND"), XDB_E_STMT, "Expect AND %s", pTkn->token);
	} while (1);

	int fid;
	for (int i = 0; i < XDB_OBJM_COUNT(pTblm->idx_objm); ++i) {
		xdb_idxm_t *pIdxm = XDB_OBJM_GET(pTblm->idx_objm, pTblm->idx_order[i]);
		if (pStmt->filter_count < pIdxm->fld_count) {
			continue;
		}
		for (fid = 0 ; fid < pIdxm->fld_count; ++fid) {
			uint16_t fld_id = pIdxm->pFields[fid]->fld_id;
			if (!(bmp[fld_id>>3] & (1<<(fld_id&7)))) {
				break;
			}
		}
		if (fid == pIdxm->fld_count) {
			//xdb_dbglog ("use index %s\n", XDB_OBJ_NAME(pIdxm));
			pStmt->pIdxm = pIdxm;
			int 		idx_id = XDB_OBJ_ID(pIdxm);
			for (fid = 0; fid < pStmt->filter_count; ++fid) {
				xdb_filter_t *pFltr = &pStmt->filters[fid];
				int idx_fid = pFltr->pField->idx_fid[idx_id];
				if (idx_fid >= 0) {
					// matched in index
					pStmt->pIdxVals[idx_fid] = &pFltr->val;
				} else {
					// extra filters
					pStmt->pIdxFlts[pStmt->idx_flt_cnt++] = pFltr;
				}
			}
			break;
		}
	}

	return type;

error:
	return -1;
}

XDB_STATIC xdb_stmt_t* 
xdb_parse_select (xdb_conn_t* pConn, xdb_token_t *pTkn, bool bPStmt)
{
	xdb_stmt_select_t 	*pStmt;
	if (xdb_unlikely (bPStmt)) {
		pStmt = xdb_malloc (sizeof (*pStmt));
		XDB_EXPECT(NULL != pStmt, XDB_E_MEMORY, "Run out of memory");
	} else {
		pStmt = &pConn->stmt_union.select_stmt;
	}

	pStmt->stmt_type = XDB_STMT_SELECT;
	pStmt->pSql = NULL;
	pStmt->meta_size = 0; // no alloc

	xdb_init_where_stmt (pStmt);

	xdb_str_t 			pFldName[XDB_MAX_COLUMN];
	//xdb_str_t			pColName[XDB_MAX_COLUMN];
	xdb_str_t			pAggName[XDB_MAX_COLUMN];
	xdb_token_type		type;
	uint16_t			col_count = 0;
	int					meta_size = sizeof (xdb_meta_t);

	// fields
	do {
		xdb_str_t *pStr;
		type = xdb_next_token (pTkn);
	 	if (XDB_TOK_ID == type) {
			XDB_EXPECT (col_count < XDB_ARY_LEN(pFldName), XDB_E_STMT, "Too many fields");
			pStr = &pFldName[col_count];
			pStr->len = pTkn->tk_len;
			pStr->str = pTkn->token;
	 	} else {
	 		XDB_EXPECT (XDB_TOK_MUL == type, XDB_E_STMT, "Expect *");
			type = xdb_next_token (pTkn);
	 		break;
	 	}
		type = xdb_next_token (pTkn);
		if (XDB_TOK_LP == type) {
			if (!strcasecmp(pStr->str, "COUNT")) {
				pStmt->agg_func[col_count] = XDB_AGGFUNC_COUNT;
			} else if (!strcasecmp(pStr->str, "MAX")) {
				pStmt->agg_func[col_count] = XDB_AGGFUNC_MAX;
			} else if (!strcasecmp(pStr->str, "MIN")) {
				pStmt->agg_func[col_count] = XDB_AGGFUNC_MIN;
			} else if (!strcasecmp(pStr->str, "SUM")) {
				pStmt->agg_func[col_count] = XDB_AGGFUNC_SUM;
			} else if (!strcasecmp(pStr->str, "AVG")) {
				pStmt->agg_func[col_count] = XDB_AGGFUNC_AVG;
			} else {
				XDB_EXPECT (0, XDB_E_STMT, "Unkonwn ID '%s'", pStr->str);
			}
			pAggName[col_count].len = pTkn->tk_len;
			pAggName[col_count].str = pTkn->token;
			type = xdb_next_token (pTkn);
			if (XDB_TOK_MUL == type) {
				XDB_EXPECT (XDB_AGGFUNC_COUNT == pStmt->agg_func[col_count], XDB_E_STMT, "%s can't use '*'", pStr->str);
			}
			pStr->len = pTkn->tk_len;
			pStr->str = pTkn->token;
			type = xdb_next_token (pTkn);
			XDB_EXPECT (XDB_TOK_RP == type, XDB_E_STMT, "Expect )");
			type = xdb_next_token (pTkn);
			pStmt->agg_count++;
			meta_size += (pAggName[col_count].len + 2 + pTkn->tk_len + 1 + 3) & (~3);
		} else {
			pStmt->agg_func[col_count] = XDB_AGGFUNC_NONE;
			meta_size += (pTkn->tk_len + 1 + 3) & (~3);
		}
#if 0
		if (xdb_unlikely (XDB_TOK_ID == type)) {
			XDB_EXPECT (strcasecmp(pTkn->token, "AS"), XDB_E_STMT, "Expect AS", pStr->str);
			type = xdb_next_token (pTkn);
			XDB_EXPECT (XDB_TOK_ID == type, XDB_E_STMT, "Expect ID");
			pColName[col_count].str = pTkn->token;
			pColName[col_count].len = pTkn->tk_len;
			as_count++;
		} else {
			pColName[col_count] = pFldName[col_count];
		}
#endif

		col_count++;
	} while (XDB_TOK_COMMA == type);

	XDB_EXPECT ((XDB_TOK_ID == type) && !strcasecmp (pTkn->token, "FROM"), XDB_E_STMT, "Except FROM");

	type = xdb_next_token (pTkn);
	XDB_EXPECT (type <= XDB_TOK_STR, XDB_E_STMT, "Miss table name");

	// [db_name.]tbl_name
	XDB_PARSE_DBTBLNAME();

	xdb_tblm_t		*pTblm = pStmt->pTblm;

	if (0 == col_count) {
		pStmt->pMeta = pTblm->pMeta;
		pStmt->col_count = pTblm->pMeta->col_count;
		pStmt->meta_size = 0;
	} else {
		meta_size += sizeof (xdb_col_t) * col_count;
		pStmt->meta_size = meta_size;
		meta_size = (meta_size + 7) & (~7); // 8B align
		if (meta_size + col_count * 8 <= sizeof (pStmt->set_flds)) {
			pStmt->pMeta = (void*)pStmt->set_flds;
		} else {
			pStmt->pMeta	= xdb_malloc (meta_size + col_count * 8);
			XDB_EXPECT (pStmt->pMeta, XDB_E_MEMORY, "Can't alloc memory");
		}
		pStmt->col_count		= col_count;
		pStmt->pMeta->col_count = col_count;
		pStmt->pMeta->row_size = pTblm->pMeta->row_size;
		pStmt->pMeta->null_off = pTblm->pMeta->null_off;
		xdb_col_t *pCol = pStmt->pMeta->cols;
		uint64_t *pColList = (void*)pStmt->pMeta + meta_size;
		pStmt->pMeta->col_list = (uintptr_t)pColList;
		if (xdb_likely (0 == pStmt->agg_count)) {
			for (int i = 0; i < col_count; ++i) {
				xdb_str_t *pStr =  &pFldName[i];
				int fld_id = xdb_find_field (pStmt->pTblm, pStr->str, pStr->len);
				XDB_EXPECT (fld_id >= 0, XDB_E_STMT, "field '%s' doesn't exist", pStr->str);
				xdb_col_t		*pColTbl = ((xdb_col_t**)pTblm->pMeta->col_list)[fld_id];
				pColList[i] = (uintptr_t)pCol;
				memcpy (pCol, pColTbl, sizeof(*pColTbl));
				memcpy (pCol->col_name, pStr->str, pStr->len + 1);
				pCol = (void*)pCol + pCol->col_len;
			}
		} else {
			uint32_t	offset = 0;
			for (int i = 0; i < col_count; ++i) {
				xdb_str_t *pStr =  &pFldName[i];
				int fld_id = -1;
				if (xdb_likely (*pStr->str != '*')) {
					fld_id = xdb_find_field (pStmt->pTblm, pStr->str, pStr->len);
					XDB_EXPECT (fld_id >= 0, XDB_E_STMT, "field '%s' doesn't exist", pStr->str);
				}
				pStmt->agg_fid[i] = fld_id;

				xdb_col_t	*pTblCol = ((xdb_col_t**)pTblm->pMeta->col_list)[fld_id];

				pColList[i] = (uintptr_t)pCol;
				pCol->col_off 	= offset;

				if (XDB_AGGFUNC_COUNT == pStmt->agg_func[i]) {
					pCol->col_type	= XDB_TYPE_BIGINT;
				} else if (XDB_AGGFUNC_SUM == pStmt->agg_func[i]) {
					switch (pTblCol->col_type) {
					case XDB_TYPE_INT:
					case XDB_TYPE_BIGINT:
					case XDB_TYPE_TINYINT:
					case XDB_TYPE_SMALLINT:
						pCol->col_type	= XDB_TYPE_BIGINT;
						break;
					case XDB_TYPE_FLOAT:
					case XDB_TYPE_DOUBLE:
						pCol->col_type	= XDB_TYPE_DOUBLE;
						break;
					}
				} else if (XDB_AGGFUNC_AVG == pStmt->agg_func[i]) {
					pCol->col_type	= XDB_TYPE_DOUBLE;
				} else {
					pCol->col_type = pTblCol->col_type;
				}
				offset += 8;
				if (xdb_likely (pStmt->agg_func[i] != XDB_AGGFUNC_NONE)) {
					if ('*' != *pFldName[i].str) {
						pCol->col_nmlen = pAggName[i].len + pFldName[i].len + 2; // ()
						sprintf (pCol->col_name, "%s(%s)", pAggName[i].str, pFldName[i].str);
					} else {
						pCol->col_nmlen = pAggName[i].len + 1 + 2; // ()
						sprintf (pCol->col_name, "%s(*)", pAggName[i].str);
					}
				} else {
					pCol->col_nmlen = pFldName[i].len;
					memcpy (pCol->col_name, pFldName[i].str, pFldName[i].len + 1);
				}
				pCol->col_len = (sizeof (*pCol) + pCol->col_nmlen + 1 + 3) & (~3); // 4B align
				pCol = (void*)pCol + pCol->col_len;
			}
			pStmt->pMeta->row_size = offset;
		}
		pStmt->pMeta->len_type = pStmt->meta_size | (XDB_RET_META<<28);
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

	return (xdb_stmt_t*)pStmt;

error:
	xdb_stmt_free ((xdb_stmt_t*)pStmt);
	return NULL;
}

XDB_STATIC xdb_ret 
xdb_parse_val (xdb_stmt_select_t *pStmt, xdb_value_t *pVal,  xdb_token_t *pTkn)
{
	int fld_id;
	xdb_conn_t *pConn = pStmt->pConn;
	
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
		break;
	case XDB_TOK_STR:
		//XDB_EXPECT(pTkn->tk_len <= pFld->fld_len, XDB_E_STMT, "Too long string values %d > %d", pTkn->tk_len, pFld->fld_len);
		pVal->str.len = pTkn->tk_len;
		pVal->str.ptr = pTkn->token;
		pVal->val_type = XDB_TYPE_CHAR;
		break;
	case XDB_TOK_ID:
		fld_id = xdb_find_field (pStmt->pTblm, pTkn->token, pTkn->tk_len);
		XDB_EXPECT(fld_id>=0, XDB_E_STMT, "Can't find field '%s'", pTkn->token);
		pVal->pField = &pStmt->pTblm->pFields[fld_id];
		pVal->val_type = XDB_TYPE_FIELD;
		break;
	default:
		break;
	}
	return XDB_OK;

error:
	return -1;
}

XDB_STATIC xdb_token_type 
xdb_parse_setcol (xdb_stmt_select_t *pStmt, xdb_token_t *pTkn)
{
	xdb_tblm_t *pTblm = pStmt->pTblm;
	xdb_conn_t	*pConn = pStmt->pConn;
	xdb_token_type	type;
	int	rc;
	
	do {
		type = xdb_next_token (pTkn);
		XDB_EXPECT((type==XDB_TOK_ID), XDB_E_STMT, "Miss field");

		int fld_id = xdb_find_field (pTblm, pTkn->token, pTkn->tk_len);
		XDB_EXPECT(fld_id>=0, XDB_E_STMT, "Can't find field '%s'", pTkn->token);
		xdb_setfld_t *pSet = &pStmt->set_flds[pStmt->set_count++];
		xdb_field_t *pFld = &pTblm->pFields[fld_id];
		pSet->pField = pFld;
		
		type = xdb_next_token (pTkn);
		XDB_EXPECT(type==XDB_TOK_EQ, XDB_E_STMT, "Miss =");

		type = xdb_next_token (pTkn);
		if (xdb_unlikely (XDB_TOK_QM == type)) {
			pSet->set_val[0].fld_type = pFld->fld_type;
			pSet->set_val[0].val_type = pFld->sup_type;
			pSet->set_val[0].sup_type = pFld->sup_type;
			pStmt->pBind[pStmt->bind_count++] = &pSet->set_val[0];			
		} else {
			XDB_EXPECT(type<=XDB_TOK_NUM, XDB_E_STMT, "Miss ID/STR/NUM");
			rc = xdb_parse_val (pStmt, &pSet->set_val[0], pTkn);
			XDB_EXPECT2 (XDB_OK == rc);
		}
		pSet->set_op = XDB_OP_EQ;
		type = xdb_next_token (pTkn);
		if (xdb_unlikely (type <= XDB_TOK_DIV && type >= XDB_TOK_ADD)) {
			pSet->set_op = XDB_OP_ADD - XDB_TOK_ADD + type ;
			type = xdb_next_token (pTkn);
			if (xdb_unlikely (XDB_TOK_QM == type)) {
				pSet->set_val[1].fld_type = pFld->fld_type;
				pSet->set_val[1].val_type = pFld->sup_type;
				pSet->set_val[1].sup_type = pFld->sup_type;
				pStmt->pBind[pStmt->bind_count++] = &pSet->set_val[1];
			} else {
				XDB_EXPECT(type<=XDB_TOK_NUM, XDB_E_STMT, "Miss ID/STR/NUM");
				rc = xdb_parse_val (pStmt, &pSet->set_val[1], pTkn);
				XDB_EXPECT2 (XDB_OK == rc);
			}
			type = xdb_next_token (pTkn);
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

	return (xdb_stmt_t*)pStmt;

error:
	xdb_stmt_free ((xdb_stmt_t*)pStmt);
	return NULL;
}
