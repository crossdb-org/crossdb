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

static inline xdb_rowid
xdb_row_vdata_info (uint32_t row_size, void *pRow, uint8_t *pType)
{
	*pType = *(uint8_t*)(pRow + row_size - 1);
	return *(xdb_rowid*)(pRow + row_size);
}

static inline void*
xdb_row_vdata_get (xdb_tblm_t *pTblm, void *pRow)
{
	uint8_t type;
	xdb_rowid vid = xdb_row_vdata_info (pTblm->row_size, pRow, &type);
	return XDB_VTYPE_OK(type) ? xdb_vdata_get (pTblm->pVdatm, type, vid) : NULL;
}

static inline void*
xdb_fld_vdata_get (xdb_field_t *pField, void *pRow)
{
	int voff = *(int*)(pRow + pField->fld_off);
	if (xdb_unlikely (0 == voff)) {
		return NULL;
	}
	uint8_t type;
	xdb_rowid vid = xdb_row_vdata_info (pField->pTblm->row_size, pRow, &type);
	if (xdb_unlikely (!XDB_VTYPE_OK(type))) {
		return NULL;
	}
	void *pVdat = xdb_vdata_get (pField->pTblm->pVdatm, type, vid);
	return pVdat + 4 + voff;
}

static inline void*
xdb_row_vdata_get2 (xdb_tblm_t *pTblm, void *pRow)
{
	uint8_t type;
	xdb_rowid vid = xdb_row_vdata_info (pTblm->row_size, pRow, &type);
	if (XDB_VTYPE_OK(type)) {
		xdb_vdata_expand (pTblm->pVdatm, type);
		return xdb_vdata_get (pTblm->pVdatm, type, vid);
	}
	return NULL;
}

static inline void
xdb_row_vdata_free (xdb_tblm_t *pTblm, void *pRow)
{
	uint8_t type;
	xdb_rowid vid = xdb_row_vdata_info (pTblm->row_size, pRow, &type);
	if (XDB_VTYPE_OK(type)) {
		xdb_vdata_free (pTblm->pVdatm, type, vid);
	}
}

static inline void xdb_get_rowptr (xdb_meta_t *pMeta, void *pRow, xdb_row_t *pRowPtr)
{
	xdb_col_t		**pColBase = (xdb_col_t**)pMeta->col_list;
	if (xdb_likely (0 == pMeta->null_off)) {
		for (int i = 0; i < pMeta->col_count; ++i) {
			pRowPtr[i] =	(uintptr_t)(pRow + pColBase[i]->col_off);
		}
	} else {
		uint8_t *pNull = (uint8_t*)(pRow + pMeta->null_off);
		for (int i = 0; i < pMeta->col_count; ++i) {
			if (XDB_IS_NOTNULL(pNull,i)) {
				pRowPtr[i] = (uintptr_t)(pRow + pColBase[i]->col_off);
			} else {
				pRowPtr[i] = 0;
			}
		}
	}
}

int
xdb_fetch_rows (xdb_res_t* pRes)
{
	xdb_queryRes_t	*pResult = (void*)pRes - offsetof(xdb_queryRes_t, res);

	xdb_meta_t 		*pMeta = (xdb_meta_t*)pRes->col_meta;
	xdb_rowlist_t	*pRowList = &pResult->rowlist;
	xdb_col_t		**pColBase = (xdb_col_t**)pMeta->col_list;
	uint64_t 		*pRowFld = pRowList->rl_pRows;
	uint32_t 		id = 0;

	pRowList->rl_curid = 0;

	if (pRes->row_count > 0) {
		xdb_rowdat_t	*pRowDat = pResult->pCurRow;
		for (id = 0; (id < pRowList->rl_count) && (pRowDat->len_type > 0); ++id) {
			void		*pRow = pRowDat->rowdat;
			if (xdb_likely (0 == pMeta->null_off)) {
				for (int i = 0; i < pMeta->col_count; ++i, ++pRowFld) {
					if (xdb_unlikely (s_xdb_vdat[pColBase[i]->col_type])) {
						int voff = *(int*)(pRow + pColBase[i]->col_off);
						if (voff) {
							*pRowFld =  (uintptr_t) (pRow + pMeta->row_size + voff);
						} else {
							*pRowFld = 0;
						}
					} else {
						*pRowFld =  (uintptr_t)(pRow + pColBase[i]->col_off);
					}
				}
			} else {
				uint8_t *pNull = (uint8_t*)(pRow + pMeta->null_off);
				for (int i = 0; i < pMeta->col_count; ++i, ++pRowFld) {
					if (XDB_IS_NOTNULL(pNull,i)) {
						if (xdb_unlikely (s_xdb_vdat[pColBase[i]->col_type])) {
							int voff = *(int*)(pRow + pColBase[i]->col_off);
							if (voff) {
								*pRowFld =	(uintptr_t) (pRow + pMeta->row_size + voff);
							} else {
								*pRowFld = 0;
							}
						} else {
							*pRowFld =	(uintptr_t)(pRow + pColBase[i]->col_off);
						}
					} else {
						*pRowFld = 0;
					}
				}
			}
			pRowDat = (void*)pRowDat + pRowDat->len_type;
		}
		pResult->pCurRow = pRowDat;
		pRowList->rl_count = id;
	}

	return id;
}

xdb_row_t*
xdb_fetch_row (xdb_res_t *pRes)
{
	if (pRes->row_count) {
		xdb_rowlist_t	*pRowList = (xdb_rowlist_t*)pRes->row_data;
		if (pRowList->rl_curid < pRowList->rl_count) {
			if (1 == pRowList->rl_count) {
				pRowList->rl_curid = 1;
				return pRowList->rl_pRows;
			}
			xdb_row_t *pRow = (xdb_row_t*)((void*)pRowList->rl_pRows + pRowList->rl_curid * pRes->col_count * 8);
			if (++pRowList->rl_curid == pRowList->rl_count) {
				xdb_fetch_rows (pRes);
			}
			return pRow;
		}
	}

	return NULL;
}

int
xdb_rewind_result (xdb_res_t *pRes)
{
	xdb_queryRes_t *pQueryRes = (void*)pRes - offsetof(xdb_queryRes_t, res);
	xdb_init_rowlist (pQueryRes);
	xdb_fetch_rows (pRes);
	return XDB_OK;
}

xdb_meta_t*
xdb_fetch_meta (xdb_res_t *pRes)
{
	return (xdb_meta_t*)pRes->col_meta;
}

void
xdb_free_result (xdb_res_t* pRes)
{
	if (xdb_unlikely(NULL == pRes)) {
		return;
	}

	xdb_conn_t	*pConn = *(xdb_conn_t**)((void*)pRes - (XDB_OFFSET(xdb_conn_t, conn_res) - XDB_OFFSET(xdb_conn_t, pConn)));
	xdb_queryRes_t *pQueryRes = pConn->pQueryRes;

	if (pRes->col_meta > 0) {
		pConn->ref_cnt--;
	}

	if (&pQueryRes->res == pRes) {
		pQueryRes->buf_free = -1LL;
		if (pQueryRes->buf_len > 2*XDB_ROW_BUF_SIZE) {
			if (NULL != pQueryRes->pStmt) {
				xdb_stmt_free((xdb_stmt_t*)pQueryRes->pStmt);
				pQueryRes->pStmt = NULL;
			}
			pQueryRes = xdb_realloc (pConn->pQueryRes, XDB_ROW_BUF_SIZE);
			if (NULL != pQueryRes) {
				pQueryRes->buf_len = XDB_ROW_BUF_SIZE;
				pConn->pQueryRes = pQueryRes;
			}
		}
		if (NULL != pQueryRes->pStmt) {
			xdb_stmt_free((xdb_stmt_t*)pQueryRes->pStmt);
		}
	} else if (&pConn->conn_res != pRes) {
		pQueryRes = (void*)pRes - XDB_OFFSET(xdb_queryRes_t, res);
		if (NULL != pQueryRes->pStmt) {
			xdb_stmt_free((xdb_stmt_t*)pQueryRes->pStmt);
		}
		xdb_free (pQueryRes);
	}

	if (xdb_unlikely (0 == pConn->ref_cnt)) {
		xdb_close (pConn);
	}
}

static inline void* xdb_get_vdat ()
{
	return NULL;
}

XDB_STATIC bool 
xdb_row_isequal (xdb_tblm_t *pTblm, void *pRow, xdb_field_t **ppFields, xdb_value_t *ppValues[], int count)
{
	for (int i = 0; i < count; ++i) {
		int len, voff;
		xdb_field_t *pField = ppFields[i];
		xdb_value_t	*pValue = ppValues[i];
		void *pFldVal = pRow + pField->fld_off;
		switch (pField->fld_type) {
		case XDB_TYPE_INT:
			if (pValue->ival != *(int32_t*)pFldVal) {
				return false;
			}
			break;
		case XDB_TYPE_BOOL:
		case XDB_TYPE_TINYINT:
			if (pValue->ival != *(int8_t*)pFldVal) {
				return false;
			}
			break;
		case XDB_TYPE_SMALLINT:
			if (pValue->ival != *(int16_t*)pFldVal) {
				return false;
			}
			break;
		case XDB_TYPE_BIGINT:
			if (pValue->ival != *(int64_t*)pFldVal) {
				return false;
			}
			break;
		case XDB_TYPE_FLOAT:
			if (pValue->fval != *(float*)pFldVal) {
				return false;
			}
			break;
		case XDB_TYPE_DOUBLE:
			if (pValue->fval != *(double*)pFldVal) {
				return false;
			}
			break;
		case XDB_TYPE_VCHAR:
			voff = *(int32_t*)pFldVal;
			if (0 == voff) { return false; }
			pFldVal = xdb_row_vdata_get (pTblm, pRow) + 4 + voff;
			// fall through
		case XDB_TYPE_CHAR:
			len = *(uint16_t*)(pFldVal-2);
			if (len != pValue->str.len) {
				return false;
			}
			//if (memcmp (pFldVal, pValue->str.str, len)) {
			if (strcasecmp (pFldVal, pValue->str.str)) {
				return false;
			}
			break;
		case XDB_TYPE_VBINARY:
			voff = *(int32_t*)pFldVal;
			if (0 == voff) { return false; }
			pFldVal = xdb_row_vdata_get (pTblm, pRow) + 4 + voff;
			// fall through
		case XDB_TYPE_BINARY:
			len = *(uint16_t*)(pFldVal-2);
			if (len != pValue->str.len) {
				return false;
			}
			if (memcmp (pFldVal, pValue->str.str, len)) {
				return false;
			}
			break;
		}
	}

	return true;
}

XDB_STATIC bool 
xdb_row_isequal2 (xdb_tblm_t *pTblm, void *pRowL, void *pRowR, xdb_field_t **ppFields, int count)
{
	xdb_field_t **ppField = ppFields;
	for (int i = 0; i < count; ++i, ++ppField) {
		int lenL, lenR, voffL, voffR;
		xdb_field_t *pField = *ppField;
		void *pFldValL = pRowL + pField->fld_off;
		void *pFldValR = pRowR + pField->fld_off;
		switch (pField->fld_type) {
		case XDB_TYPE_INT:
			if (*(int32_t*)pFldValL != *(int32_t*)pFldValR) {
				return false;
			}
			break;
		case XDB_TYPE_BIGINT:
			if (*(int64_t*)pFldValL != *(int64_t*)pFldValR) {
				return false;
			}
			break;
		case XDB_TYPE_BOOL:
		case XDB_TYPE_TINYINT:
			if (*(int8_t*)pFldValL != *(int8_t*)pFldValR) {
				return false;
			}
			break;
		case XDB_TYPE_SMALLINT:
			if (*(int16_t*)pFldValL != *(int16_t*)pFldValR) {
				return false;
			}
			break;
		case XDB_TYPE_FLOAT:
			if (*(float*)pFldValL != *(float*)pFldValR) {
				return false;
			}
			break;
		case XDB_TYPE_DOUBLE:
			if (*(double*)pFldValL != *(double*)pFldValR) {
				return false;
			}
			break;

		case XDB_TYPE_VCHAR:
			voffL = *(int32_t*)pFldValL;
			if (0 == voffL) { return false; }
			voffR = *(int32_t*)pFldValR;
			if (0 == voffR) { return false; }
			pFldValL = xdb_row_vdata_get (pTblm, pRowL) + 4 + voffL;
			pFldValR = xdb_row_vdata_get (pTblm, pRowR) + 4 + voffR;
			// fall through
		case XDB_TYPE_CHAR:
			lenL = *(uint16_t*)(pFldValL-2);
			lenR = *(uint16_t*)(pFldValR-2);
			if (lenL != lenR) {
				return false;
			}
			//if (memcmp (pFldValL, pFldValR, lenL)) {
			if (strcasecmp (pFldValL, pFldValR)) {
				return false;
			}
			break;

		case XDB_TYPE_VBINARY:
			voffL = *(int32_t*)pFldValL;
			if (0 == voffL) { return false; }
			voffR = *(int32_t*)pFldValR;
			if (0 == voffR) { return false; }
			pFldValL = xdb_row_vdata_get (pTblm, pRowL) + 4 + voffL;
			pFldValR = xdb_row_vdata_get (pTblm, pRowR) + 4 + voffR;
			// fall through
		case XDB_TYPE_BINARY:
			lenL = *(uint16_t*)(pFldValL-2);
			lenR = *(uint16_t*)(pFldValR-2);
			if (lenL != lenR) {
				return false;
			}
			if (memcmp (pFldValL, pFldValR, lenL)) {
				return false;
			}
			break;
		}
	}

	return true;
}

XDB_STATIC int 
xdb_row_cmp (const void *pRowL, const void *pRowR, const xdb_field_t **ppFields, int *pCount)
{
	const xdb_field_t **ppField = ppFields;
	int count = *pCount;
	for (int i = 0; i < count; ++i, ++ppField) {
		int 	cmp;
		double 	cmpf;
		const xdb_field_t *pField = *ppField;
		const void *pRowValL = pRowL + pField->fld_off;
		const void *pRowValR = pRowR + pField->fld_off;
		int	lenL, lenR;
		switch (pField->fld_type) {
		case XDB_TYPE_INT:
			cmp = *(int32_t*)pRowValL - *(int32_t*)pRowValR;
			if (cmp) {
				*pCount = i;
				return cmp;
			}
			break;
		case XDB_TYPE_BIGINT:
			cmp = *(int64_t*)pRowValL - *(int64_t*)pRowValR;
			if (cmp) {
				*pCount = i;
				return cmp;
			}
			break;
		case XDB_TYPE_BOOL:
		case XDB_TYPE_TINYINT:
			cmp = *(int8_t*)pRowValL - *(int8_t*)pRowValR;
			if (cmp) {
				*pCount = i;
				return cmp;
			}
			break;
		case XDB_TYPE_SMALLINT:
			cmp = *(int16_t*)pRowValL - *(int16_t*)pRowValR;
			if (cmp) {
				*pCount = i;
				return cmp;
			}
			break;
		case XDB_TYPE_FLOAT:
			cmpf = *(float*)pRowValL - *(float*)pRowValR;
			if (cmpf) {
				*pCount = i;
				return cmpf > 0 ? 1 : -1;
			}
			break;
		case XDB_TYPE_DOUBLE:
			cmpf = *(double*)pRowValL - *(double*)pRowValR;
			if (cmpf) {
				*pCount = i;
				return cmpf > 0 ? 1 : -1;
			}
			break;
		case XDB_TYPE_CHAR:
			cmp = strcasecmp (pRowValL, pRowValR);
			if (cmp) {
				*pCount = i;
				return cmp;
			}
			break;
		case XDB_TYPE_BINARY:
			lenL = *(uint16_t*)(pRowValL-2);
			lenR = *(uint16_t*)(pRowValR-2);
			cmp = memcmp (pRowValL, pRowValR, lenL>lenR?lenR:lenL);
			if (cmp) {
				*pCount = i;
				return cmp;
			} else if (lenL != lenR) {
				*pCount = i;
				return lenL - lenR;
			}
			break;
		}
	}

	return 0;
}

XDB_STATIC bool 
xdb_row_and_match (xdb_tblm_t *pTblm, void *pRow, xdb_filter_t **pFilters, int count)
{
	for (int i = 0; i < count; ++i) {
		int voff;
		xdb_filter_t *pFilter = pFilters[i];
		xdb_field_t *pField = pFilter->pField;
		void *pVal = pRow + pField->fld_off;
		xdb_value_t	value, *pValue = &pFilter->val;

		switch (pField->fld_type) {
		case XDB_TYPE_INT:
			value.ival = *(int32_t*)pVal;
			value.val_type = XDB_TYPE_BIGINT;
			break;
		case XDB_TYPE_BIGINT:
			value.ival = *(int64_t*)pVal;
			value.val_type = XDB_TYPE_BIGINT;
			break;
		case XDB_TYPE_BOOL:
		case XDB_TYPE_TINYINT:
			value.ival = *(int8_t*)pVal;
			value.val_type = XDB_TYPE_BIGINT;
			break;
		case XDB_TYPE_SMALLINT:
			value.ival = *(int16_t*)pVal;
			value.val_type = XDB_TYPE_BIGINT;
			break;
		case XDB_TYPE_FLOAT:
			value.fval = *(float*)pVal;
			value.val_type = XDB_TYPE_DOUBLE;
			pValue->fval = (float)pValue->fval;
			break;
		case XDB_TYPE_DOUBLE:
			value.fval = *(double*)pVal;
			value.val_type = XDB_TYPE_DOUBLE;
			break;
		case XDB_TYPE_VCHAR:
			voff = *(int32_t*)pVal;
			if (0 == voff) { return 0; }
			pVal = xdb_row_vdata_get (pTblm, pRow) + 4 + voff;
			// fall through
		case XDB_TYPE_CHAR:
			value.str.len = *(uint16_t*)(pVal-2);
			value.str.str = pVal;
			value.val_type = XDB_TYPE_CHAR;
			break;
		case XDB_TYPE_VBINARY:
			voff = *(int32_t*)pVal;
			if (0 == voff) { return 0; }
			pVal = xdb_row_vdata_get (pTblm, pRow) + 4 + voff;
			// fall through
		case XDB_TYPE_BINARY:
			value.str.len = *(uint16_t*)(pVal-2);
			value.str.str = pVal;
			value.val_type = XDB_TYPE_BINARY;
			break;
		default:
			value.val_type = XDB_TYPE_NULL;
			break;
		}

		switch (pValue->val_type) {
		case XDB_TYPE_BIGINT:
			if (xdb_unlikely (value.val_type != XDB_TYPE_BIGINT)) {
				return false;
			}
			switch (pFilter->cmp_op) {
			case XDB_TOK_EQ: 
				if (value.ival != pValue->ival) { return 0;	}
				break;
			case XDB_TOK_GT: 
				if (value.ival <= pValue->ival) { return 0; }
				break;
			case XDB_TOK_GE: 
				if (value.ival < pValue->ival) { return 0; }
				break;
			case XDB_TOK_LT: 
				if (value.ival >= pValue->ival) { return 0; }
				break;
			case XDB_TOK_LE: 
				if (value.ival > pValue->ival) { return 0; }
				break;
			case XDB_TOK_NE: 
				if (value.ival == pValue->ival) { return 0; }
				break;
			}
			break;

		case XDB_TYPE_DOUBLE:
			if (xdb_unlikely (value.val_type != XDB_TYPE_DOUBLE)) {
				return false;
			}
			switch (pFilter->cmp_op) {
			case XDB_TOK_EQ: 
				if (value.fval != pValue->fval) {	return 0; }
				break;
			case XDB_TOK_GT: 
				if (value.fval <= pValue->fval) { return 0; }
				break;
			case XDB_TOK_GE: 
				if (value.fval < pValue->fval) { return 0; }
				break;
			case XDB_TOK_LT: 
				if (value.fval >= pValue->fval) { return 0; }
				break;
			case XDB_TOK_LE: 
				if (value.fval > pValue->fval) { return 0; }
				break;
			case XDB_TOK_NE: 
				if (value.fval == pValue->fval) { return 0; }
				break;
			}
			break;

		case XDB_TYPE_CHAR:
		case XDB_TYPE_VCHAR:
			if (xdb_unlikely (value.val_type != XDB_TYPE_CHAR)) {
				return 0;
			}
			switch (pFilter->cmp_op) {
			case XDB_TOK_EQ: 
				if ((value.str.len != pValue->str.len) || strcasecmp (value.str.str, pValue->str.str)) {
				//if (memcmp (value.str.str, pValue->str.str, value.str.len)) {
					return false;
				}
				break;
			case XDB_TOK_NE: 
				if ((value.str.len == pValue->str.len) && !strcasecmp (value.str.str, pValue->str.str)) {
					//if (memcmp (value.str.str, pValue->str.str, value.str.len)) {
					return false;
				}
				break;
			case XDB_TOK_LIKE: 
				if (!xdb_str_like2 (value.str.str, value.str.len, pValue->str.str, pValue->str.len, true)) {
					return false;
				}
				break;
			default:
				break;
			}
			break;

		case XDB_TYPE_BINARY:
		case XDB_TYPE_VBINARY:
			if (xdb_unlikely (value.val_type != XDB_TYPE_BINARY)) {
				return 0;
			}
			switch (pFilter->cmp_op) {
			case XDB_TOK_EQ: 
				if ((value.str.len != pValue->str.len) || memcmp (value.str.str, pValue->str.str, value.str.len)) {
					return false;
				}
				break;
			case XDB_TOK_NE: 
				if ((value.str.len == pValue->str.len) && !memcmp (value.str.str, pValue->str.str, value.str.len)) {
					return false;
				}
				break;
			default:
				break;
			}
			break;

		default:
			return false;
		}
	}

	return true;
}

XDB_STATIC void 
xdb_row_getval (void *pRow, xdb_value_t *pVal)
{
	if (!(pVal->pField->fld_flags & XDB_FLD_NOTNULL) && !XDB_IS_NOTNULL(pRow+pVal->pField->pTblm->null_off, pVal->pField->fld_id)) {
		pVal->val_type = pVal->sup_type = XDB_TYPE_NULL;
		return;
	}
	void *pFldPtr = pRow + pVal->pField->fld_off;
	void *pVdat;
	switch (pVal->pField->fld_type) {
	case XDB_TYPE_INT:
		pVal->ival = *(int32_t*)pFldPtr;
		pVal->sup_type = XDB_TYPE_BIGINT;
		break;
	case XDB_TYPE_BOOL:
	case XDB_TYPE_TINYINT:
		pVal->ival = *(int8_t*)pFldPtr;
		pVal->sup_type = XDB_TYPE_BIGINT;
		break;
	case XDB_TYPE_SMALLINT:
		pVal->ival = *(int16_t*)pFldPtr;
		pVal->sup_type = XDB_TYPE_BIGINT;
		break;
	case XDB_TYPE_BIGINT:
		pVal->ival = *(int64_t*)pFldPtr;
		pVal->sup_type = XDB_TYPE_BIGINT;
		break;
	case XDB_TYPE_FLOAT:
		pVal->fval = *(float*)pFldPtr;
		pVal->sup_type = XDB_TYPE_DOUBLE;
		break;
	case XDB_TYPE_DOUBLE:
		pVal->fval = *(double*)pFldPtr;
		pVal->sup_type = XDB_TYPE_DOUBLE;
		break;
	case XDB_TYPE_CHAR:
		pVal->str.str = (char*)pFldPtr;
		pVal->str.len = *(uint16_t*)(pFldPtr - 2);
		pVal->sup_type = XDB_TYPE_CHAR;
		break;
	case XDB_TYPE_VCHAR:
		pVdat = xdb_fld_vdata_get (pVal->pField, pRow);
		if (xdb_likely (pVdat != NULL)) {
			pVal->str.str = pVdat;
			pVal->str.len = *(uint16_t*)(pVdat - 2);
		} else {
			pVal->str.str = NULL;
			pVal->str.len = 0;
		}
		pVal->sup_type = XDB_TYPE_VCHAR;
		break;
	case XDB_TYPE_BINARY:
		pVal->str.str = (char*)pFldPtr;
		pVal->str.len = *(uint16_t*)(pFldPtr - 2);
		pVal->sup_type = XDB_TYPE_BINARY;
		break;
	case XDB_TYPE_VBINARY:
		pVdat = xdb_fld_vdata_get (pVal->pField, pRow);
		if (xdb_likely (pVdat != NULL)) {
			pVal->str.str = pVdat;
			pVal->str.len = *(uint16_t*)(pVdat - 2);
		} else {
			pVal->str.str = NULL;
			pVal->str.len = 0;
		}
		pVal->sup_type = XDB_TYPE_VBINARY;
		break;
	}
}

XDB_STATIC void 
xdb_convert_val (xdb_value_t *pVal, xdb_type_t type)
{
	switch (type) {
	case XDB_TYPE_BIGINT:
		pVal->ival = (uint64_t)pVal->fval;
		break;
	case XDB_TYPE_DOUBLE:
		pVal->fval = (double)pVal->ival;
		break;
	default:
		break;
	}
}

XDB_STATIC xdb_value_t * 
xdb_exp_eval (xdb_value_t *pResVal, xdb_exp_t *pExp, void *pRow)
{
	xdb_value_t 	*pVal	= &pExp->op_val[0];

	if (xdb_likely (XDB_TYPE_FIELD == pVal->val_type)) {
		xdb_row_getval (pRow, pVal);
	}
	if (xdb_likely (XDB_TOK_NONE == pExp->exp_op)) {
		return pVal;
	}

	xdb_value_t 	*pVal2	= &pExp->op_val[1];
	if (xdb_unlikely (XDB_TYPE_FIELD == pVal2->val_type)) {
		xdb_row_getval (pRow, pVal2);
	}

	if (xdb_unlikely (pVal->sup_type != pVal2->sup_type)) {
		xdb_convert_val (pVal2, pVal->sup_type);
	}

	pResVal->sup_type = pVal->sup_type;

	switch (pExp->exp_op) {
	case XDB_TOK_ADD:
		if (XDB_TYPE_BIGINT == pVal->sup_type) {
			pResVal->ival = pVal->ival + pVal2->ival;
		} else {
			pResVal->fval = pVal->fval + pVal2->fval;
		}
		break;
	case XDB_TOK_SUB:
		if (XDB_TYPE_BIGINT == pVal->sup_type) {
			pResVal->ival = pVal->ival - pVal2->ival;
		} else {
			pResVal->fval = pVal->fval - pVal2->fval;
		}
		break;
	case XDB_TOK_MUL:
		if (XDB_TYPE_BIGINT == pVal->sup_type) {
			pResVal->ival = pVal->ival * pVal2->ival;
		} else {
			pResVal->fval = pVal->fval * pVal2->fval;
		}
		break;
	case XDB_TOK_DIV:
		if (XDB_TYPE_BIGINT == pVal->sup_type) {
			if (xdb_likely (pVal2->ival != 0)) {
				pResVal->ival = pVal->ival / pVal2->ival;
			} else {
				pResVal->ival = 0;						
			}
		} else {
			if (xdb_likely (pVal2->fval != 0)) {
				pResVal->fval = pVal->fval / pVal2->fval;
			} else {
				pResVal->fval = 0;						
			}
		}
		break;
	default:
		break;
	}

	return pResVal;
}

XDB_STATIC void 
xdb_col_set2 (void *pColPtr, xdb_type_t col_type, xdb_value_t *pVal)
{
	switch (col_type) {
	case XDB_TYPE_INT:
		*(int32_t*)(pColPtr) = pVal->ival;
		break;
	case XDB_TYPE_BIGINT:
		*(int64_t*)(pColPtr) = pVal->ival;
		break;
	case XDB_TYPE_BOOL:
	case XDB_TYPE_TINYINT:
		*(int8_t*)(pColPtr) = pVal->ival;
		break;
	case XDB_TYPE_SMALLINT:
		*(int16_t*)(pColPtr) = pVal->ival;
		break;
	case XDB_TYPE_FLOAT:
		*(float*)(pColPtr) = pVal->fval;
		break;
	case XDB_TYPE_DOUBLE:
		*(double*)(pColPtr) = pVal->fval;
		break;
	case XDB_TYPE_CHAR:
		memcpy (pColPtr, pVal->str.str, pVal->str.len+1);
		*(uint16_t*)(pColPtr-2) = pVal->str.len;
		break;
	default:
		break;
	}
}

XDB_STATIC void 
xdb_col_set (xdb_tblm_t *pTblm, void *pRow, xdb_field_t *pField, xdb_value_t *pVal)
{
	xdb_str_t *pVStr;
	void *pColPtr = pRow + pField->fld_off;
	switch (pField->fld_type) {
	case XDB_TYPE_INT:
		*(int32_t*)(pColPtr) = pVal->ival;
		break;
	case XDB_TYPE_BIGINT:
		*(int64_t*)(pColPtr) = pVal->ival;
		break;
	case XDB_TYPE_BOOL:
	case XDB_TYPE_TINYINT:
		*(int8_t*)(pColPtr) = pVal->ival;
		break;
	case XDB_TYPE_SMALLINT:
		*(int16_t*)(pColPtr) = pVal->ival;
		break;
	case XDB_TYPE_FLOAT:
		*(float*)(pColPtr) = pVal->fval;
		break;
	case XDB_TYPE_DOUBLE:
		*(double*)(pColPtr) = pVal->fval;
		break;
	case XDB_TYPE_CHAR:
		memcpy (pColPtr, pVal->str.str, pVal->str.len+1);
		*(uint16_t*)(pColPtr-2) = pVal->str.len;
		break;
	case XDB_TYPE_VCHAR:
		pVStr = pRow + pTblm->row_size;
		pVStr[pField->fld_vid] = pVal->str;
		break;		
	case XDB_TYPE_BINARY:
		memcpy (pColPtr, pVal->str.str, pVal->str.len+1);
		*(uint16_t*)(pColPtr-2) = pVal->str.len;
		break;
	case XDB_TYPE_VBINARY:
		pVStr = pRow + pTblm->row_size;
		pVStr[pField->fld_vid] = pVal->str;
		break;		
	default:
		break;
	}
}

XDB_STATIC void 
xdb_row_set (xdb_tblm_t *pTblm, void *pRow, xdb_setfld_t set_flds[], int count)
{
	for (int i = 0; i < count; ++i) {
		xdb_setfld_t 	*pSetFld 	= &set_flds[i];
		xdb_field_t		*pField = pSetFld->pField;
		xdb_value_t		*pVal;
		xdb_value_t 	res_val; 

		pVal = xdb_exp_eval (&res_val, &pSetFld->exp, pRow);

		if (xdb_unlikely (pField->sup_type != pVal->sup_type)) {
			xdb_convert_val (pVal, pField->sup_type);
		}
		xdb_col_set (pTblm, pRow, pField, pVal);
	}
}

XDB_STATIC uint64_t 
xdb_row_hash (xdb_tblm_t *pTblm, void *pRow, xdb_field_t *pFields[], int count)
{
	uint64_t hashsum = 0, hash;
	xdb_field_t **ppField = pFields;
	for (int i = 0; i < count; ++i, ++ppField) {
		int voff;
		xdb_field_t *pField = *ppField;
		void *ptr = pRow+pField->fld_off;
		switch (pField->fld_type) {
		case XDB_TYPE_INT:
			hash = *(int32_t*)ptr;
			break;
		case XDB_TYPE_BIGINT:
			hash = *(int64_t*)ptr;
			break;
		case XDB_TYPE_BOOL:
		case XDB_TYPE_TINYINT:
			hash = *(int8_t*)ptr;
			break;
		case XDB_TYPE_SMALLINT:
			hash = *(int16_t*)ptr;
			break;
		case XDB_TYPE_FLOAT:
			hash = *(int32_t*)ptr;
			break;
		case XDB_TYPE_DOUBLE:
			hash = *(int64_t*)ptr;
			break;
		case XDB_TYPE_VCHAR:
			voff = *(int32_t*)ptr;
			if (0 == voff) { 
				hash = 0;
				break; 
			}
			ptr = xdb_row_vdata_get (pTblm, pRow) + 4 + voff;
			// fall through
		case XDB_TYPE_CHAR:
			//hash = xdb_wyhash (ptr, *(uint16_t*)(ptr-2));
			hash = xdb_strcasehash (ptr, *(uint16_t*)(ptr-2));
			break;
		case XDB_TYPE_VBINARY:
			voff = *(int32_t*)ptr;
			if (0 == voff) { 
				hash = 0;
				break; 
			}
			ptr = xdb_row_vdata_get (pTblm, pRow) + 4 + voff;
			// fall through
		case XDB_TYPE_BINARY:
			hash = xdb_wyhash (ptr, *(uint16_t*)(ptr-2));
			break;
		default:
			hash = 0;
			break;
		}
		if (0 == hashsum) {
			hashsum = hash;
		} else {
			hashsum = hashsum*XDB_HASH_COM_MUL + hash;
		}
	}
	return hashsum;
}

XDB_STATIC uint64_t 
xdb_row_hash2 (xdb_tblm_t *pTblm, void *pRow, xdb_field_t *pFields[], int count)
{
	uint64_t hashsum = 0, hash;
	xdb_str_t *pVStr = pRow + pTblm->row_size, *pStr;

	xdb_field_t **ppField = pFields;
	for (int i = 0; i < count; ++i, ++ppField) {
		xdb_field_t *pField = *ppField;
		void *ptr = pRow+pField->fld_off;
		switch (pField->fld_type) {
		case XDB_TYPE_INT:
			hash = *(int32_t*)ptr;
			break;
		case XDB_TYPE_BIGINT:
			hash = *(int64_t*)ptr;
			break;
		case XDB_TYPE_BOOL:
		case XDB_TYPE_TINYINT:
			hash = *(int8_t*)ptr;
			break;
		case XDB_TYPE_SMALLINT:
			hash = *(int16_t*)ptr;
			break;
		case XDB_TYPE_FLOAT:
			hash = *(int32_t*)ptr;
			break;
		case XDB_TYPE_DOUBLE:
			hash = *(int64_t*)ptr;
			break;
		case XDB_TYPE_VCHAR:
			pStr = &pVStr[i];
			if (xdb_unlikely (NULL != pStr->str)) {
				hash = xdb_strcasehash (pStr->str, pStr->len);
			} else {
				hash = 0;
			}
			break;
		case XDB_TYPE_CHAR:
			//hash = xdb_wyhash (ptr, *(uint16_t*)(ptr-2));
			hash = xdb_strcasehash (ptr, *(uint16_t*)(ptr-2));
			break;
		case XDB_TYPE_VBINARY:
			pStr = &pVStr[i];
			if (xdb_unlikely (NULL != pStr->str)) {
				hash = xdb_wyhash (ptr, *(uint16_t*)(ptr-2));
			} else {
				hash = 0;
			}
			break;
		case XDB_TYPE_BINARY:
			hash = xdb_wyhash (ptr, *(uint16_t*)(ptr-2));
			break;
		default:
			hash = 0;
			break;
		}
		if (0 == hashsum) {
			hashsum = hash;
		} else {
			hashsum = hashsum*XDB_HASH_COM_MUL + hash;
		}
	}
	return hashsum;
}

XDB_STATIC uint64_t 
xdb_val_hash (xdb_value_t **ppValues, int count)
{
	uint64_t hashsum = 0, hash;
	for (int i = 0; i < count; ++i) {
		xdb_value_t *pValue = ppValues[i];
		switch (pValue->val_type) {
		case XDB_TYPE_BIGINT:
			hash = pValue->ival;
			break;
		case XDB_TYPE_DOUBLE:
			hash = (uint64_t)pValue->fval;
			break;
		case XDB_TYPE_CHAR:
			//hash = xdb_wyhash (pValue->str.str, pValue->str.len);
			hash = xdb_strcasehash (pValue->str.str, pValue->str.len);
			break;
		case XDB_TYPE_BINARY:
			hash = xdb_wyhash (pValue->str.str, pValue->str.len);
			break;
		default:
			hash = 0;
			break;
		}
		hashsum = hashsum*XDB_HASH_COM_MUL + hash;
	}
	return hashsum;
}

#if 0
XDB_STATIC xdb_ret
xdb_row_setval (xdb_conn_t *pConn, xdb_field_t *pFld, void *pRow, const char *val, int len)
{
	switch (pFld->fld_type) {
	case XDB_TYPE_INT:
		*(int32_t*)(pRow+pFld->fld_off) = atoi (val);
		break;
	case XDB_TYPE_BIGINT:
		*(int64_t*)(pRow+pFld->fld_off) = atoll (val);
		break;
	case XDB_TYPE_TINYINT:
		*(int8_t*)(pRow+pFld->fld_off) = atoi (val);
		break;
	case XDB_TYPE_SMALLINT:
		*(int16_t*)(pRow+pFld->fld_off) = atoi (val);
		break;
	case XDB_TYPE_FLOAT:
		*(float*)(pRow+pFld->fld_off) = atof (val);
		break;
	case XDB_TYPE_DOUBLE:
		*(double*)(pRow+pFld->fld_off) = atof (val);
		break;
	case XDB_TYPE_CHAR:
		*(uint16_t*)(pRow+pFld->fld_off-2) = len;
		memcpy (pRow+pFld->fld_off, val, len+1);
		break;
	}

	return XDB_OK;

error:
	return -XDB_E_PARAM;
}
#endif

XDB_STATIC int64_t 
xdb_row_getInt (uint64_t meta, void *pRow, uint16_t iCol)
{
	xdb_meta_t *pMeta = (xdb_meta_t*)meta;
	if (xdb_unlikely (iCol >= pMeta->col_count)) {
		return 0;
	}
	xdb_col_t	*pCol = ((xdb_col_t**)pMeta->col_list)[iCol];

	// check NULL
	void *pVal = pRow + pCol->col_off;
	switch (pCol->col_type) {
	case XDB_TYPE_INT:
		return *(int32_t*)pVal;
	case XDB_TYPE_BIGINT:
		return *(int64_t*)pVal;
	case XDB_TYPE_BOOL:
	case XDB_TYPE_TINYINT:
		return *(int8_t*)pVal;
	case XDB_TYPE_SMALLINT:
		return *(int16_t*)pVal;
	}

	return 0;
}

XDB_STATIC void 
xdb_fld_setInt (void *pAddr, xdb_type_t type, int64_t val)
{
	switch (type) {
	case XDB_TYPE_INT:
		*(int32_t*)pAddr = val;
		break;
	case XDB_TYPE_BIGINT:
		*(int64_t*)pAddr = val;
		break;
	case XDB_TYPE_BOOL:
	case XDB_TYPE_TINYINT:
		*(int8_t*)pAddr = val;
		break;
	case XDB_TYPE_SMALLINT:
		*(int16_t*)pAddr = val;
		break;
	default:
		break;
	}
}

XDB_STATIC void 
xdb_fld_setFloat (void *pAddr, xdb_type_t type, double val)
{
	switch (type) {
	case XDB_TYPE_FLOAT:
		*(float*)pAddr = val;
		break;
	case XDB_TYPE_DOUBLE:
		*(double*)pAddr = val;
		break;
	default:
		break;
	}
}

XDB_STATIC xdb_ret 
xdb_fld_setStr (xdb_conn_t *pConn, xdb_field_t * pField, void *pRow, const char *str, int len)
{
	if (len > pField->fld_len) {
		return -XDB_E_PARAM;
	}
	void *pAddr = pRow + pField->fld_off;
	switch (pField->fld_type) {
	case XDB_TYPE_CHAR:
		*(uint16_t*)(pAddr - 2) = len;
		memcpy (pAddr, str, len + 1);
		break;
	case XDB_TYPE_BINARY:
		*(uint16_t*)(pAddr - 2) = len;
		memcpy (pAddr, str, len);
		*(char*)(pAddr+len) = '\0';
		break;
	case XDB_TYPE_VCHAR:
		if (xdb_unlikely (len > pField->fld_len)) {
			XDB_SETERR(XDB_E_PARAM, "Field '%s' max len %d < input %d", XDB_OBJ_NAME(pField), pField->fld_len, len);
			return -XDB_E_PARAM;
		} else {
			xdb_str_t *pVstr = pRow + pField->pTblm->row_size;
			xdb_str_t *pStr = &pVstr[pField->fld_vid];
			pStr->len = len;
			pStr->str = (char*)str;
		}
		break;
	case XDB_TYPE_VBINARY:
		if (xdb_unlikely (len > pField->fld_len)) {
			XDB_SETERR(XDB_E_PARAM, "Field '%s' max len %d < input %d", XDB_OBJ_NAME(pField), pField->fld_len, len);
			return -XDB_E_PARAM;
		} else {
			xdb_str_t *pVstr = pRow + pField->pTblm->row_size;
			xdb_str_t *pStr = &pVstr[pField->fld_vid];
			pStr->len = len;
			pStr->str = (char*)str;
		}
		break;
	default:
		return -XDB_E_PARAM;
	}
	return XDB_OK;
}

XDB_STATIC void 
xdb_row_setInt (uint64_t meta, void *pRow, uint16_t iCol, int64_t val)
{
	xdb_meta_t *pMeta = (xdb_meta_t*)meta;
	xdb_col_t	*pCol = ((xdb_col_t**)pMeta->col_list)[iCol];
	xdb_fld_setInt (pRow + pCol->col_off, pCol->col_type, val);
}

XDB_STATIC double 
xdb_row_getFloat (uint64_t meta, void *pRow, uint16_t iCol)
{
	xdb_meta_t *pMeta = (xdb_meta_t*)meta;
	xdb_col_t	*pCol = ((xdb_col_t**)pMeta->col_list)[iCol];

	// check NULL
	void *pVal = pRow + pCol->col_off;
	switch (pCol->col_type) {
	case XDB_TYPE_FLOAT:
		return *(float*)pVal;
		break;
	case XDB_TYPE_DOUBLE:
		return *(double*)pVal;
		break;
	}

	return 0;
}

XDB_STATIC void 
xdb_row_setFloat (uint64_t meta, void *pRow, uint16_t iCol, double val)
{
	xdb_meta_t *pMeta = (xdb_meta_t*)meta;
	xdb_col_t	*pCol = ((xdb_col_t**)pMeta->col_list)[iCol];

	xdb_fld_setFloat (pRow + pCol->col_off, pCol->col_type, val);
}


#if 0
XDB_STATIC const char*
xdb_row_getStr (uint64_t meta, void *pRow, uint16_t iCol, int *pLen)
{
	xdb_meta_t *pMeta = (xdb_meta_t*)meta;
	if (xdb_unlikely (iCol >= pMeta->col_count)) {
		return NULL;
	}
	xdb_col_t	*pCol = ((xdb_col_t**)pMeta->col_list)[iCol];

	// check NULL
	void *pVal = pRow + pCol->col_off;
	switch (pCol->col_type) {
	case XDB_TYPE_CHAR:
		if (pLen != NULL) {
			*pLen = *(uint16_t*)(pVal-2);
		}
		return pVal;
	}

	return NULL;
}
#endif

xdb_col_t* 
xdb_column_meta (uint64_t meta, uint16_t iCol)
{
	xdb_meta_t *pMeta = (xdb_meta_t*)meta;
	if (xdb_unlikely ((NULL == pMeta) || (pMeta->col_count <= iCol))) {
		return NULL;
	}
	return (xdb_col_t*)((((uint64_t*)pMeta->col_list))[iCol]);
}

xdb_type_t 
xdb_column_type (uint64_t meta, uint16_t iCol)
{
	xdb_col_t *pCol = xdb_column_meta (meta, iCol);
	return pCol ? pCol->col_type : XDB_TYPE_NULL;
}

const char* 
xdb_column_name (uint64_t meta, uint16_t iCol)
{
	xdb_col_t *pCol = xdb_column_meta (meta, iCol);
	return pCol ? pCol->col_name : NULL;
}

int64_t xdb_column_int64 (uint64_t meta, xdb_row_t *pRow, uint16_t iCol)
{
	xdb_meta_t *pMeta = (xdb_meta_t*)meta;
	if (xdb_unlikely ((NULL == pMeta) || (iCol >= pMeta->col_count))) {
		return 0;
	}
	xdb_col_t	*pCol = ((xdb_col_t**)pMeta->col_list)[iCol];

	switch (pCol->col_type) {
	case XDB_TYPE_INT:
		return *(int32_t*)pRow[iCol];
	case XDB_TYPE_BIGINT:
		return *(int64_t*)pRow[iCol];
	case XDB_TYPE_BOOL:
	case XDB_TYPE_TINYINT:
		return *(int8_t*)pRow[iCol];
	case XDB_TYPE_SMALLINT:
		return *(int16_t*)pRow[iCol];
	}
	return 0;
}

int xdb_column_int (uint64_t meta, xdb_row_t *pRow, uint16_t iCol)
{
	return (int)xdb_column_int64 (meta, pRow, iCol);
}

double
xdb_column_double (uint64_t meta, xdb_row_t *pRow, uint16_t iCol)
{
	xdb_meta_t *pMeta = (xdb_meta_t*)meta;
	if (xdb_unlikely ((NULL == pMeta) || (iCol >= pMeta->col_count))) {
		return 0;
	}
	xdb_col_t	*pCol = ((xdb_col_t**)pMeta->col_list)[iCol];

	switch (pCol->col_type) {
	case XDB_TYPE_FLOAT:
		return *(float*)pRow[iCol];
	case XDB_TYPE_DOUBLE:
		return *(double*)pRow[iCol];
	}
	return 0;
}

float
xdb_column_float (uint64_t meta, xdb_row_t *pRow, uint16_t iCol)
{
	return (float)xdb_column_double (meta, pRow, iCol);
}

const char*
xdb_column_str2 (uint64_t meta, xdb_row_t *pRow, uint16_t iCol, int *pLen)
{
	xdb_meta_t *pMeta = (xdb_meta_t*)meta;
	if (xdb_unlikely ((NULL == pMeta) || (iCol >= pMeta->col_count))) {
		return NULL;
	}
	xdb_col_t	*pCol = ((xdb_col_t**)pMeta->col_list)[iCol];

	switch (pCol->col_type) {
	case XDB_TYPE_CHAR:
	case XDB_TYPE_VCHAR:
		if (pLen != NULL) {
			*pLen = *(uint16_t*)(pRow[iCol]-2);
		}
		return (const char*)pRow[iCol];
	}

	return NULL;
}

const char*
xdb_column_str (uint64_t meta, xdb_row_t *pRow, uint16_t iCol)
{
	return xdb_column_str2 (meta, pRow, iCol, NULL);
}

const void*
xdb_column_blob (uint64_t meta, xdb_row_t *pRow, uint16_t iCol, int *pLen)
{
	xdb_meta_t *pMeta = (xdb_meta_t*)meta;
	if (xdb_unlikely ((NULL == pMeta) || (iCol >= pMeta->col_count))) {
		return NULL;
	}
	xdb_col_t	*pCol = ((xdb_col_t**)pMeta->col_list)[iCol];

	switch (pCol->col_type) {
	case XDB_TYPE_BINARY:
	case XDB_TYPE_VBINARY:
		if (pLen != NULL) {
			*pLen = *(uint16_t*)(pRow[iCol]-2);
		}
		return (const void*)pRow[iCol];
	}

	return NULL;
}

int xdb_fprint_row (FILE *pFile, uint64_t meta, xdb_row_t *pRow, int format)
{
	xdb_meta_t	*pMeta = (xdb_meta_t*)meta;
	xdb_col_t	**pCol = (xdb_col_t**)pMeta->col_list;

	for (int i = 0; i < pMeta->col_count; ++i) {
		void *pVal = (void*)((uint64_t*)pRow[i]);
		if (NULL == pVal) {
			fprintf (pFile, "%s=NULL ", pCol[i]->col_name);
			continue;
		}
		switch (pCol[i]->col_type) {
		case XDB_TYPE_BOOL:
			fprintf (pFile, "%s=%s ", pCol[i]->col_name, *(int8_t*)pVal?"true":"false");
			break;
		case XDB_TYPE_INT:
			fprintf (pFile, "%s=%d ", pCol[i]->col_name, *(int32_t*)pVal);
			break;
		case XDB_TYPE_TINYINT:
			fprintf (pFile, "%s=%d ", pCol[i]->col_name, *(int8_t*)pVal);
			break;
		case XDB_TYPE_SMALLINT:
			fprintf (pFile, "%s=%d ", pCol[i]->col_name, *(int16_t*)pVal);
			break;
		case XDB_TYPE_BIGINT:
			fprintf (pFile, "%s=%"PRIi64" ", pCol[i]->col_name, *(int64_t*)pVal);
			break;
		case XDB_TYPE_FLOAT:
			fprintf (pFile, "%s=%f ", pCol[i]->col_name, *(float*)pVal);
			break;
		case XDB_TYPE_DOUBLE:
			fprintf (pFile, "%s=%f ", pCol[i]->col_name, *(double*)pVal);
			break;
		case XDB_TYPE_CHAR:
		case XDB_TYPE_VCHAR:
			fprintf (pFile, "%s='%s' ", pCol[i]->col_name, (char*)pVal);
			break;
		case XDB_TYPE_BINARY:
		case XDB_TYPE_VBINARY:
			fprintf (pFile, "%s=X' ", pCol[i]->col_name);
			for (int h = 0; h < *(uint16_t*)(pVal-2); ++h) {
				fprintf (pFile, "%c%c", s_xdb_hex_2_str[((uint8_t*)pVal)[h]][0], s_xdb_hex_2_str[((uint8_t*)pVal)[h]][1]);
			}
			fprintf (pFile, "' ");
			break;
		}
	}
	return 0;
}

int xdb_print_row (uint64_t meta, xdb_row_t *pRow, int format)
{
	return xdb_fprint_row (stdout, meta, pRow, format);
}

int xdb_fprint_dbrow (FILE *pFile, xdb_tblm_t *pTblm, void *pDbRow, int format)
{
	for (int i = 0; i < pTblm->fld_count; ++i) {
		xdb_field_t *pField = &pTblm->pFields[i];
		if (!XDB_IS_NOTNULL(pDbRow + pTblm->null_off, i)) {
			fprintf (pFile, "%s=NULL ", XDB_OBJ_NAME(pField));
			continue;
		}
		void *pVal = pDbRow + pField->fld_off;
		switch (pField->fld_type) {
		case XDB_TYPE_BOOL:
			fprintf (pFile, "%s=%s ", XDB_OBJ_NAME(pField), *(int8_t*)pVal?"true":"false");
			break;
		case XDB_TYPE_INT:
			fprintf (pFile, "%s=%d ", XDB_OBJ_NAME(pField), *(int32_t*)pVal);
			break;
		case XDB_TYPE_TINYINT:
			fprintf (pFile, "%s=%d ", XDB_OBJ_NAME(pField), *(int8_t*)pVal);
			break;
		case XDB_TYPE_SMALLINT:
			fprintf (pFile, "%s=%d ", XDB_OBJ_NAME(pField), *(int16_t*)pVal);
			break;
		case XDB_TYPE_BIGINT:
			fprintf (pFile, "%s=%"PRIi64" ", XDB_OBJ_NAME(pField), *(int64_t*)pVal);
			break;
		case XDB_TYPE_FLOAT:
			fprintf (pFile, "%s=%f ", XDB_OBJ_NAME(pField), *(float*)pVal);
			break;
		case XDB_TYPE_DOUBLE:
			fprintf (pFile, "%s=%f ", XDB_OBJ_NAME(pField), *(double*)pVal);
			break;
		case XDB_TYPE_VCHAR:
			pVal = xdb_fld_vdata_get (pField, pDbRow);
			// fall through
		case XDB_TYPE_CHAR:
			fprintf (pFile, "%s='%s' ", XDB_OBJ_NAME(pField), (char*)pVal);
			break;
		case XDB_TYPE_VBINARY:
			pVal = xdb_fld_vdata_get (pField, pDbRow);
		case XDB_TYPE_BINARY:
			fprintf (pFile, "%s=X' ", XDB_OBJ_NAME(pField));
			for (int h = 0; h < *(uint16_t*)(pVal-2); ++h) {
				fprintf (pFile, "%c%c", s_xdb_hex_2_str[((uint8_t*)pVal)[h]][0], s_xdb_hex_2_str[((uint8_t*)pVal)[h]][1]);
			}
			fprintf (pFile, "' ");
			break;
		}
	}
	return 0;
}

XDB_STATIC void 
xdb_rowset_init (xdb_rowset_t *pRowSet)
{
	memset (&pRowSet->count, 0, sizeof(pRowSet->count) * 4);
	pRowSet->cap	= XDB_ARY_LEN (pRowSet->rowlist);
	pRowSet->pRowList	= pRowSet->rowlist;
	pRowSet->limit	= XDB_MAX_ROWS;
	pRowSet->pBmp	= NULL;
}

XDB_STATIC int 
xdb_rowset_add (xdb_rowset_t *pRowSet, xdb_rowid rid, void *ptr)
{
	if (xdb_unlikely (pRowSet->pBmp != NULL)) {
		if (xdb_bmp_get (pRowSet->pBmp, rid)) {
			return XDB_OK;
		}
		xdb_bmp_set (pRowSet->pBmp, rid);
	}
	if (xdb_unlikely (pRowSet->count >= pRowSet->cap)) {
		xdb_rowptr_t *pRows;
		xdb_rowid cap = pRowSet->cap << 1;
		if (pRowSet->pRowList == pRowSet->rowlist) {
			pRows = xdb_malloc (cap * sizeof(xdb_rowptr_t));
			if (NULL == pRows) {
				return -XDB_E_MEMORY;
			}
			memcpy (pRows, pRowSet->rowlist, sizeof (pRowSet->rowlist));
		} else {
			pRows = xdb_realloc (pRowSet->pRowList, cap * sizeof(xdb_rowptr_t));
			if (NULL == pRows) {
				return -XDB_E_MEMORY;
			}
		}
		pRowSet->cap	= cap;
		pRowSet->pRowList	= pRows;
	}

	if (xdb_unlikely (pRowSet->offset > 0)) {
		if (++pRowSet->seqid <= pRowSet->offset) {
			return XDB_OK;
		}
	}

	xdb_rowptr_t *pRowPtr = &pRowSet->pRowList[pRowSet->count];
	pRowPtr->rid = rid;
	pRowPtr->ptr = ptr;

	if (xdb_unlikely (++pRowSet->count >= pRowSet->limit)) {
		return -XDB_E_FULL;
	}
	return XDB_OK;
}

XDB_STATIC int 
xdb_rowset_add_batch (xdb_rowset_t *pRowSet, xdb_rowptr_t *pRowPtrs, int batch)
{
	if (xdb_unlikely (pRowSet->count >= pRowSet->cap)) {
		xdb_rowptr_t *pRows;
		xdb_rowid cap = pRowSet->cap << 1;
		if (pRowSet->pRowList == pRowSet->rowlist) {
			pRows = xdb_malloc (cap * sizeof(xdb_rowptr_t) * batch);
			if (NULL == pRows) {
				return -XDB_E_MEMORY;
			}
			memcpy (pRows, pRowSet->rowlist, sizeof (pRowSet->rowlist));
		} else {
			pRows = xdb_realloc (pRowSet->pRowList, cap * sizeof(xdb_rowptr_t) * batch);
			if (NULL == pRows) {
				return -XDB_E_MEMORY;
			}
		}
		pRowSet->cap	= cap;
		pRowSet->pRowList	= pRows;
	}

	if (xdb_unlikely (pRowSet->offset > 0)) {
		if (++pRowSet->seqid <= pRowSet->offset) {
			return XDB_OK;
		}
	}

	xdb_rowptr_t *pRowPtr = &pRowSet->pRowList[pRowSet->count * batch];
	memcpy (pRowPtr, pRowPtrs, batch * sizeof (xdb_rowptr_t));

	if (xdb_unlikely (++pRowSet->count >= pRowSet->limit)) {
		return -XDB_E_FULL;
	}
	return XDB_OK;
}

XDB_STATIC void 
xdb_rowset_clean (xdb_rowset_t *pRowSet)
{
	if (xdb_unlikely (pRowSet->cap > XDB_ROWLIST_CNT * 64 * 4)) {
		uint64_t cap = XDB_ROWLIST_CNT * 64;
		xdb_rowptr_t *pRows = xdb_realloc (pRowSet->pRowList, cap * sizeof(xdb_rowptr_t));
		if (pRows != NULL) {
			pRowSet->cap		= cap;
			pRowSet->pRowList	= pRows;			
		}
	}
	// set count and seqid = 0
	memset (&pRowSet->count, 0, 2 * sizeof (pRowSet->count));
}

XDB_STATIC void 
xdb_rowset_free (xdb_rowset_t *pRowSet)
{
	if (pRowSet->rowlist != pRowSet->pRowList) {
		xdb_free ( pRowSet->pRowList);
	}
}

XDB_STATIC int 
#if defined(_WIN32) || defined(__FreeBSD__) ||  defined(__APPLE__)
xdb_sort_cmp (void *pArg, const void *pLeft, const void *pRight)
#else
xdb_sort_cmp (const void *pLeft, const void *pRight, void *pArg)
#endif
{
	int					cmp;
	xdb_stmt_select_t	*pStmt = pArg;
	const xdb_rowptr_t	*pRowL = pLeft, *pRowR = pRight;

	int count = pStmt->order_count;
	cmp = xdb_row_cmp (pRowL->ptr, pRowR->ptr, (const xdb_field_t**)pStmt->pOrderFlds, &count);

	return (pStmt->bOrderDesc[count]) ? -cmp : cmp;
}

XDB_STATIC void 
xdb_sql_orderby (xdb_stmt_select_t *pStmt, xdb_rowset_t *pRowSet)
{
#ifdef _WIN32
	qsort_s (pRowSet->pRowList, pRowSet->count, sizeof(pRowSet->pRowList[0]), xdb_sort_cmp, pStmt);
#elif defined (__FreeBSD__) || defined (__APPLE__)
	qsort_r (pRowSet->pRowList, pRowSet->count, sizeof(pRowSet->pRowList[0]), pStmt, xdb_sort_cmp);
#else
	qsort_r (pRowSet->pRowList, pRowSet->count, sizeof(pRowSet->pRowList[0]), xdb_sort_cmp, pStmt);
#endif
}

XDB_STATIC void 
xdb_sql_limit (xdb_rowset_t 	*pRowSet, int limit, int offset)
{
	if ((XDB_MAX_ROWS == limit) && (0 == offset)) {
		return;
	}
	if (offset == 0) {
		if (pRowSet->count > limit) {
			pRowSet->count = limit;
		}
	} else if (offset < pRowSet->count) {
		if (pRowSet->count - offset < limit) {
			limit = pRowSet->count - offset;
		}		
		memmove (pRowSet->pRowList, &pRowSet->pRowList[offset], limit * sizeof (xdb_rowptr_t));
		pRowSet->count = limit;
	} else {
		pRowSet->count = 0;
	}
}

XDB_STATIC int 
xdb_sql_scan (xdb_conn_t *pConn, xdb_tblm_t *pTblm, xdb_rowset_t *pRowSet, xdb_reftbl_t *pRefTbl)
{
	if (xdb_likely (pRefTbl->bUseIdx)) {
		xdb_bmp_t		rows_bmp;
		if (xdb_unlikely (pRefTbl->or_count > 1)) {
			xdb_bmp_init (&rows_bmp);
			pRowSet->pBmp = &rows_bmp;
		}
		for (int i = 0; i < pRefTbl->or_count; ++i) {
			xdb_idxfilter_t 	*pIdxFilter = pRefTbl->or_list[i].pIdxFilter;
			pIdxFilter->pIdxm->pIdxOps->idx_query (pConn, pIdxFilter, pRowSet);
		}
		if (xdb_unlikely (pRowSet->pBmp != NULL)) {
			xdb_bmp_free (pRowSet->pBmp);
			pRowSet->pBmp = NULL;
		}
		return XDB_OK;
	}

	xdb_stgmgr_t	*pStgMgr = &pTblm->stg_mgr;
	xdb_rowid max_rid = XDB_STG_MAXID(pStgMgr);
	for (xdb_rowid rid = 1; rid <= max_rid; ++rid) {
		void *pRow = XDB_IDPTR(pStgMgr, rid);
		if (xdb_row_valid (pConn, pTblm, pRow, rid)) {
			if (pRefTbl->filter_count > 0) {
				for (int i = 0; i < pRefTbl->or_count; ++i) {
					xdb_singfilter_t *pSigFlt = &pRefTbl->or_list[i];
					bool bMatch = xdb_row_and_match (pRefTbl->pRefTblm, pRow, pSigFlt->pFilters, pSigFlt->filter_count);
					if (bMatch) {
						goto match;
					}
				}
				continue;
			}
match:
			if (xdb_unlikely (-XDB_E_FULL == xdb_rowset_add (pRowSet, rid, pRow))) {
				break;
			}
		}
	}

	return XDB_OK;
}

XDB_STATIC int 
xdb_sql_join (xdb_stmt_select_t *pStmt, xdb_rowptr_t *pRowPtrs, int level, xdb_rowset_t *pRowSet)
{
	xdb_rowset_t row_set;
	xdb_filter_t filters[XDB_MAX_MATCH_COL], *pFilers[XDB_MAX_MATCH_COL];
	xdb_rowset_init (&row_set);
	xdb_reftbl_t		*pJoin = &pStmt->ref_tbl[level];
	void *pRow = pRowPtrs[level].ptr;

	for (int i = 0; i < pJoin->field_count; ++i) {
		filters[i].cmp_op = XDB_TOK_EQ;
		filters[i].pField = pJoin->pJoinField[i];
		filters[i].val.pField  = pJoin->pField[i];
		xdb_row_getval (pRow, &filters[i].val);
		pFilers[i] = &filters[i];
	}

//	xdb_sql_scan (pStmt->pConn, pJoin->pRefTblm, &row_set, pFilers, pJoin->field_count, NULL);
	(void)pFilers;
	if (row_set.count > 0) {
		int nxt_lvl = level + 1;
		for (xdb_rowid id = 0; id < row_set.count; ++id) {
			pRowPtrs[nxt_lvl] = row_set.pRowList[id];
			if (nxt_lvl == pStmt->reftbl_count) {
				// add to row
				for (int i = 0; i < nxt_lvl; ++i) {
					xdb_rowset_add_batch (pRowSet, pRowPtrs, pStmt->reftbl_count + 1);
				}
			} else {
				xdb_sql_join (pStmt, pRowPtrs, nxt_lvl, pRowSet);
			}
		}
	}
	return 0;
}

XDB_STATIC int 
xdb_sql_filter (xdb_stmt_select_t *pStmt)
{
	xdb_conn_t		*pConn = pStmt->pConn;
	xdb_tblm_t		*pTblm = pStmt->pTblm;
	xdb_rowset_t	row_set;
	xdb_rowset_t 	*pRowSet = &pConn->row_set;

	pRowSet->pTblMeta = pStmt->pTblm->pMeta;
	pRowSet->pFldMap  = NULL;
	if (xdb_unlikely (pStmt->limit <= 0)) {
		return XDB_OK;
	}

	if (xdb_unlikely (pStmt->reftbl_count > 1)) {
		xdb_rowset_init (&row_set);
		pRowSet = &row_set;
	}

	if (xdb_likely ((0 == pStmt->order_count) || (pStmt->reftbl_count > 1))) {
		// set limit and offset
		memcpy (&pRowSet->limit, &pStmt->limit, sizeof(pRowSet->limit) * 2);
	} else {
		pRowSet->limit = XDB_MAX_ROWS;
		pRowSet->offset = 0;
	}

	xdb_reftbl_t *pRefTbl = &pStmt->ref_tbl[0];
	xdb_sql_scan (pConn, pTblm, pRowSet, pRefTbl);

	if (xdb_unlikely (pStmt->reftbl_count > 1)) {
		xdb_rowptr_t row_ptrs[XDB_MAX_JOIN];
		for (xdb_rowid id = 0; id < pRowSet->count; ++id) {
			row_ptrs[0] = pRowSet->pRowList[id];
			xdb_sql_join (pStmt, row_ptrs, 1, &pConn->row_set);
		}
		xdb_rowset_free(&row_set);
		pRowSet = &pConn->row_set;
	}

	if (xdb_unlikely (pStmt->order_count > 0)) {
		xdb_sql_orderby (pStmt, pRowSet);
		memcpy (&pRowSet->limit, &pStmt->limit, sizeof(pRowSet->limit) * 2);
		xdb_sql_limit (pRowSet, pStmt->limit, pStmt->offset);
	}

	if (xdb_unlikely (pStmt->agg_count) > 0) {
		int64_t			val;
		double			fval;
		xdb_rowid 		agg_count[XDB_MAX_COLUMN];
		//union {
			int64_t 	ival_col[XDB_MAX_COLUMN];
			//char		*val_str[XDB_MAX_COLUMN];
			double		fval_col[XDB_MAX_COLUMN];
		//};
		void *pRow = pRowSet->pRowList[0].ptr;
		xdb_field_t	*pField;

		for (int i = 0; i < pStmt->pMeta->col_count; ++i) {
			if (XDB_TOK_COUNT == pStmt->sel_cols[i].exp.exp_op) {
				ival_col[i] = (pRowSet->count > 0);
				continue;
			}
			if (XDB_TOK_AVG == pStmt->sel_cols[i].exp.exp_op) {
				agg_count[i] = 1;
			}
			pField = pStmt->sel_cols[i].exp.op_val[1].pField;
			switch (pField->sup_type) {
			case XDB_TYPE_BIGINT:
				if (pRowSet->count > 0) {
					ival_col[i] = xdb_row_getInt ((uintptr_t)pTblm->pMeta, pRow, pField->fld_id);
				} else {
					ival_col[i] = 0;
				}
				break;
			case XDB_TYPE_DOUBLE:
				if (pRowSet->count > 0) {
					fval_col[i] = xdb_row_getFloat ((uintptr_t)pTblm->pMeta, pRow, pField->fld_id);
				} else {
					fval_col[i] = 0;
				}
				break;
			}
		}

		for (xdb_rowid rid = 1; rid < pRowSet->count; ++rid) {
			pRow = pRowSet->pRowList[rid].ptr;
			for (int i = 0; i < pStmt->pMeta->col_count; ++i) {
				pField = pStmt->sel_cols[i].exp.op_val[1].pField;
				switch (pStmt->sel_cols[i].exp.exp_op) {
				case XDB_TOK_COUNT:
					ival_col[i]++;
					break;
				case XDB_TOK_MAX:
					switch (pField->sup_type) {
					case XDB_TYPE_BIGINT:
						val = xdb_row_getInt ((uintptr_t)pTblm->pMeta, pRow, pField->fld_id);
						if (val > ival_col[i]) {
							ival_col[i] = val;
						}
						break;
					case XDB_TYPE_DOUBLE:
						fval = xdb_row_getFloat ((uintptr_t)pTblm->pMeta, pRow, pField->fld_id);
						if (fval > fval_col[i]) {
							fval_col[i] = fval;
						}
						break;
					}
					break;
				case XDB_TOK_MIN:
					switch (pField->sup_type) {
					case XDB_TYPE_BIGINT:
						val = xdb_row_getInt ((uintptr_t)pTblm->pMeta, pRow, pField->fld_id);
						if (val < ival_col[i]) {
							ival_col[i] = val;
						}
						break;
					case XDB_TYPE_DOUBLE:
						fval = xdb_row_getFloat ((uintptr_t)pTblm->pMeta, pRow, pField->fld_id);
						if (fval < fval_col[i]) {
							fval_col[i] = fval;
						}
						break;
					}
					break;
				case XDB_TOK_AVG:
					agg_count[i]++;
				case XDB_TOK_SUM:				
					switch (pField->sup_type) {
					case XDB_TYPE_BIGINT:
						val = xdb_row_getInt ((uintptr_t)pTblm->pMeta, pRow, pField->fld_id);
						ival_col[i] += val;
						break;
					case XDB_TYPE_DOUBLE:
						fval = xdb_row_getFloat ((uintptr_t)pTblm->pMeta, pRow, pField->fld_id);
						fval_col[i] += fval;
						break;
					}
					break;
				default:
					break;
				}
			}
		}

		pRowSet->count = 1;
		pRowSet->pRowList[0].ptr = pStmt->agg_buf;
		pRow = pRowSet->pRowList[0].ptr;
		*((uint8_t*)pRow + pStmt->pMeta->row_size - 1) = XDB_VTYPE_NONE;
		uint8_t *pNull = pRow + pStmt->pMeta->null_off;

		for (int i = 0; i < pStmt->pMeta->col_count; ++i) {
			XDB_SET_NOTNULL (pNull, i);
			pField = pStmt->sel_cols[i].exp.op_val[1].pField;
			switch (pStmt->sel_cols[i].exp.exp_op) {
				case XDB_TOK_AVG:
					switch (pField->sup_type) {
					case XDB_TYPE_BIGINT:
						//xdb_dbgprint ("%s %d %d %f\n", pCol[i]->col_name, ival_col[i], agg_count[i], (double)ival_col[i]/agg_count[i]);
						xdb_row_setFloat ((uintptr_t)pStmt->pMeta, pRow, i, (float)ival_col[i]/agg_count[i]);
						break;
					case XDB_TYPE_DOUBLE:
						//xdb_dbgprint ("%s %f %d %f\n", pCol[i]->col_name, fval_col[i], agg_count[i], fval_col[i]/agg_count[i]);
						xdb_row_setFloat ((uintptr_t)pStmt->pMeta, pRow, i, fval_col[i]/agg_count[i]);
						break;
					}					
					break;
				default:
					if (NULL == pField) {
						xdb_row_setInt ((uintptr_t)pStmt->pMeta, pRow, i, ival_col[i]);
						break;
					}
					switch (pField->sup_type) {
					case XDB_TYPE_BIGINT:
						//xdb_dbgprint ("%s %d\n", pCol[i]->col_name, ival_col[i]);
						xdb_row_setInt ((uintptr_t)pStmt->pMeta, pRow, i, ival_col[i]);
						break;
					case XDB_TYPE_DOUBLE:
						//xdb_dbgprint ("%s %f\n", pCol[i]->col_name, fval_col[i]);
						xdb_row_setFloat ((uintptr_t)pStmt->pMeta, pRow, i, fval_col[i]);
						break;
					}
					break;
			}
		}
	}

	return XDB_OK;
}

XDB_STATIC xdb_res_t* 
xdb_queryres_alloc (xdb_conn_t *pConn, xdb_size size)
{
	xdb_queryRes_t	*pQueryRes = pConn->pQueryRes;
	
	if (xdb_unlikely ((NULL == pQueryRes) || (pQueryRes->buf_free != -1LL))) {
		pQueryRes = xdb_malloc (size);
		XDB_EXPECT2 (NULL != pQueryRes);
		pQueryRes->pConn = pConn;
		pQueryRes->res.len_type = (sizeof (xdb_res_t)) | (XDB_RET_REPLY<<28);
		pQueryRes->pStmt = NULL;
		pQueryRes->buf_len = size;
		pConn->pQueryRes = pQueryRes;
	}
	pQueryRes->buf_free = pQueryRes->buf_len - (sizeof (*pQueryRes) + 4); // last 4 is end of row (len = 0)

	return NULL;

error:
	XDB_SETERR(XDB_E_MEMORY, "Run out of memory")
	if (NULL != pConn->pQueryRes) {
		pConn->pQueryRes->buf_free = -1LL;
	}
	return &pConn->conn_res;
}

XDB_STATIC xdb_res_t* 
xdb_queryres_realloc (xdb_conn_t *pConn, xdb_size size)
{
	xdb_queryRes_t	*pQueryRes = pConn->pQueryRes;

	if (pQueryRes->buf_len < size) {
		pQueryRes = xdb_realloc (pQueryRes, size);
		XDB_EXPECT2 (NULL != pQueryRes);
		pQueryRes->buf_free += (size - pQueryRes->buf_len);
		pQueryRes->buf_len = size;
		pConn->pQueryRes = pQueryRes;
	}

	return NULL;

error:
	XDB_SETERR(XDB_E_MEMORY, "Run out of memory")
	if (NULL != pConn->pQueryRes) {
		pConn->pQueryRes->buf_free = -1LL;
	}
	return &pConn->conn_res;
}

#define XDB_RES_ALLOC()	\
	if (pQueryRes->buf_free < pCurDat->len_type) {	\
		pRes = xdb_queryres_realloc (pConn, pQueryRes->buf_len<<1);	\
		if (NULL != pRes) {	\
			goto exit;	\
		}	\
		pQueryRes = pConn->pQueryRes;	\
		pRes = &pQueryRes->res;	\
		pCurDat = (void*)pQueryRes + offset;	\
	}

#define XDB_RES_ALLOC2()	\
	if (pQueryRes->buf_free < pCurDat->len_type) {	\
		pRes = xdb_queryres_realloc (pConn, pQueryRes->buf_len<<1);	\
		if (NULL != pRes) {	\
			goto exit;	\
		}	\
		pQueryRes = pConn->pQueryRes;	\
		pRes = &pQueryRes->res;	\
		pCurDat = (void*)pQueryRes + offset;	\
		pNull = (void*)pCurDat + pStmt->pMeta->null_off;	\
	}

XDB_STATIC xdb_res_t* 
xdb_sql_select (xdb_stmt_select_t *pStmt)
{
	xdb_conn_t		*pConn = pStmt->pConn;
	xdb_res_t		*pRes;

	xdb_rowset_t	*pRowSet = &pConn->row_set;

	xdb_rdlock_tblstg (pStmt->pTblm);

	xdb_sql_filter (pStmt);

	if (pStmt->callback != NULL) {
		pRes = &pConn->conn_res;
		memset (pRes, 0, sizeof (*pRes));
		for (xdb_rowid id = 0; id < pRowSet->count; ++id) {
			xdb_get_rowptr (pStmt->pMeta, pRowSet->pRowList[id].ptr, pStmt->pRow);
			pStmt->callback ((uintptr_t)pStmt->pMeta, pStmt->pRow, pStmt->pCbArg);
		}
		goto exit;
	}

	pRes = xdb_queryres_alloc (pConn, XDB_ROW_BUF_SIZE);
	if (NULL != pRes) {
		goto exit;
	}

	xdb_queryRes_t	*pQueryRes = pConn->pQueryRes;
	pRes = &pQueryRes->res;

	int				row_size = (pStmt->pMeta->row_size + 3)>>2<<2;

	pRes->errcode = 0;
	pRes->status = 0;	// dynamic alloc
	//pRes->stmt_type = pStmt->stmt_type;

	pRes->affected_rows	= 0;
	pRes->row_count = 0;
	pRes->col_count	= pStmt->pMeta->col_count;
	pRes->insert_id = 0;
	pRes->data_len  = 0;

	// RES META ROWPTR ROWDATA
	pRes->meta_len	= pStmt->pMeta->len_type & XDB_LEN_MASK;
	xdb_meta_t		*pMeta = (xdb_meta_t*)(pRes + 1);
	pRes->col_meta = (uintptr_t)pStmt->pMeta;
	// skip copy meta
	pQueryRes->buf_free -= pRes->meta_len;

	xdb_rowdat_t	*pRowDat = (void*)pMeta + pRes->meta_len;
	xdb_rowdat_t	*pCurDat = pRowDat;

	xdb_rowid jid = 0;

	for (xdb_rowid id = 0; id < pRowSet->count; ++id) {
		uint64_t	offset = (void*)pCurDat - (void*)pQueryRes;
		pCurDat->len_type = row_size + 4;
		XDB_RES_ALLOC();
		if (0 == (pStmt->exp_count)) {
			if (xdb_likely (1 == pStmt->reftbl_count)) {
				void *pPtr = pRowSet->pRowList[id].ptr;
				memcpy (pCurDat->rowdat, pPtr, pStmt->pTblm->row_size);
				*((uint8_t*)pCurDat->rowdat + pStmt->pTblm->vtype_off) = XDB_VTYPE_DATA;
				if (pStmt->pTblm->pVdatm != NULL) {
					uint8_t 	type; 
					xdb_rowid 	vid = xdb_row_vdata_info (pStmt->pMeta->row_size, pPtr, &type);
					if (XDB_VTYPE_OK(type)) {
						void *pVdat = xdb_vdata_get (pStmt->pTblm->pVdatm, type, vid);
						int vlen = *(uint32_t*)pVdat & XDB_VDAT_LENMASK;
						pCurDat->len_type += vlen;
						XDB_RES_ALLOC();
						memcpy (pCurDat->rowdat + pStmt->pTblm->row_size, pVdat + 4, vlen);
					}
				}
			} else {
				void *pJoinDat = (void*)pCurDat->rowdat;
				for (int i = 0; i < pStmt->reftbl_count; ++i, ++jid) {
					int tbl_rowsize = pStmt->ref_tbl[i].pRefTblm->row_size;
					void *pPtr = pRowSet->pRowList[jid].ptr;
					if (NULL != pPtr) {
						memcpy (pJoinDat, pPtr, tbl_rowsize);
					} else {
						memset (pJoinDat, 0, tbl_rowsize);
					}
					pJoinDat += tbl_rowsize;
				}
			}
		} else {
			void *pRow = pRowSet->pRowList[id].ptr;
			int voff = 0, vlen;
			uint8_t *pNull = (void*)pCurDat->rowdat + pStmt->pMeta->null_off;
			for (int i = 0 ; i < pStmt->pMeta->col_count; ++i) {
				xdb_col_t	*pCol = ((xdb_col_t**)pStmt->pMeta->col_list)[i];
				xdb_exp_t 		*pExp = &pStmt->sel_cols[i].exp;
				xdb_value_t 	*pVal;
				xdb_value_t 	res_val;
				pVal = xdb_exp_eval (&res_val, pExp, pRow);
				if (XDB_TYPE_NULL == pVal->val_type) {
					XDB_SET_NULL (pNull, i);
					continue;
				}
				XDB_SET_NOTNULL (pNull, i);
				xdb_type_t sup_type = s_xdb_prompt_type[pCol->col_type];
				if (xdb_unlikely (sup_type != pVal->sup_type)) {
					xdb_convert_val (pVal, sup_type);
				}
				*((uint8_t*)pCurDat->rowdat + row_size - 1) = XDB_VTYPE_DATA;
				if (xdb_unlikely (s_xdb_vdat[pCol->col_type])) {
					*(int*)((void*)pCurDat->rowdat + pCol->col_off) = voff + 2;
					vlen = XDB_ALIGN4 (pVal->str.len + 3);
					pCurDat->len_type += vlen;
					XDB_RES_ALLOC2();
					void *pVdat = (void*)pCurDat->rowdat + row_size + voff;
					*(uint16_t*)pVdat = pVal->str.len;
					memcpy (pVdat + 2, pVal->str.str, pVal->str.len + 1);
					voff += vlen;
				} else {
					xdb_col_set2 ((void*)pCurDat->rowdat + pCol->col_off, pCol->col_type, pVal);
				}
			}
		}
		pRes->row_count++;
		pQueryRes->buf_free -= pCurDat->len_type;
		pCurDat = (void*)pCurDat + pCurDat->len_type;
	}

	pRes = &pQueryRes->res;
	pMeta = (xdb_meta_t*)(pRes + 1);
	pCurDat->len_type = 0;
	pRes->data_len = (void*)pCurDat - (void*)pMeta + 4;
	pRes = &pQueryRes->res;
	pRes->row_data = (uintptr_t)&pQueryRes->rowlist;

	xdb_init_rowlist (pQueryRes);

	xdb_fetch_rows (pRes);

	pConn->ref_cnt++;

exit:
	xdb_rowset_clean (pRowSet);
	xdb_rdunlock_tblstg (pStmt->pTblm);

	return pRes;
}

XDB_STATIC void
__xdb_fetch_row (xdb_tblm_t *pTblm, void *pRow, uint64_t pRowFld[])
{
	for (int i = 0; i < pTblm->fld_count; ++i, ++pRowFld) {
		if (xdb_unlikely (s_xdb_vdat[pTblm->pFields[i].fld_type])) {
			int voff = *(int*)(pRow + pTblm->pFields[i].fld_off);
			if (voff) {
				*pRowFld =	(uintptr_t) (pRow + pTblm->row_size + voff);
			} else {
				*pRowFld = 0;
			}
		} else {
			*pRowFld =	(uintptr_t)(pRow + pTblm->pFields[i].fld_off);
		}
	}
}

static int 
xdb_sprint_field (xdb_field_t *pField, void *pRow, char *buf)
{
	int len = 0;
	void *pVal = pRow + pField->fld_off;

	switch (pField->fld_type) {
	case XDB_TYPE_INT:
		len = sprintf (buf, "%d", *(int32_t*)pVal);
		break;
	case XDB_TYPE_BOOL:
		len = sprintf (buf, "%s", *(int8_t*)pVal?"true":"false");
		break;
	case XDB_TYPE_TINYINT:
		len = sprintf (buf, "%d", *(int8_t*)pVal);
		break;
	case XDB_TYPE_SMALLINT:
		len = sprintf (buf, "%d", *(int16_t*)pVal);
		break;
	case XDB_TYPE_BIGINT:
		len = sprintf (buf, "%"PRIi64"", *(int64_t*)pVal);
		break;
	case XDB_TYPE_FLOAT:
		len = sprintf (buf, "%f", *(float*)pVal);
		break;
	case XDB_TYPE_DOUBLE:
		len = sprintf (buf, "%f", *(double*)pVal);
		break;
	case XDB_TYPE_VCHAR:
		pVal = xdb_fld_vdata_get (pField, pRow);
		if (NULL == pVal) {
			return sprintf (buf, "NULL");
		}
		// fall through
	case XDB_TYPE_CHAR:
		*(buf + len++) = '\'';
		len += xdb_str_escape (buf+len, (char*)pVal, *(uint16_t*)(pVal-2));
		*(buf + len++) = '\'';
		break;
	case XDB_TYPE_VBINARY:
		pVal = xdb_fld_vdata_get (pField, pRow);
		if (NULL == pVal) {
			return sprintf (buf, "NULL");
		}
		// fall through
	case XDB_TYPE_BINARY:
		*(buf + len++) = 'X';
		*(buf + len++) = '\'';
		for (int h = 0; h < *(uint16_t*)(pVal-2); ++h) {
			*(buf + len++) = s_xdb_hex_2_str[((uint8_t*)pVal)[h]][0];
			*(buf + len++) = s_xdb_hex_2_str[((uint8_t*)pVal)[h]][1];
		}
		*(buf + len++) = '\'';
		break;
	}

	return len;
}

static int 
xdb_dbrow_log (xdb_tblm_t *pTblm, uint32_t type, void *pNewRow, void *pOldRow, xdb_setfld_t set_flds[], int set_count)
{
	return 0;

	int 		len = 0;
	char 		buf[32768];
	xdb_field_t	**ppFields, *pField;
	int			count;

	if (pTblm->pDbm->bSysDb) {
		return XDB_OK;
	}

	switch (type) {
	case XDB_TRIG_AFT_INS:
		len = sprintf (buf, "INSERT INTO %s (", XDB_OBJ_NAME(pTblm));
		for (int i = 0; i < pTblm->fld_count; ++i) {
			pField = &pTblm->pFields[i];
			memcpy (buf+len, pField->obj.obj_name, pField->obj.nm_len);
			len += pField->obj.nm_len;
			buf[len++]=',';
		}
		buf[len-1] = ')';
		len += sprintf (buf+len, " VALUES (");
		for (int i = 0; i < pTblm->fld_count; ++i) {
			len += xdb_sprint_field (&pTblm->pFields[i], (void*)pNewRow, buf+len);
			buf[len++]=',';
		}
		buf[--len] = ')';
		buf[++len] = ';';
		buf[++len] = '\0';
		break;
	case XDB_TRIG_AFT_UPD:
		len = sprintf (buf, "UPDATE %s SET ", XDB_OBJ_NAME(pTblm));
		for (int i = 0; i < set_count; ++i) {
			pField = set_flds[i].pField;
			memcpy (buf+len, pField->obj.obj_name, pField->obj.nm_len);
			len += pField->obj.nm_len;
			buf[len++] = '=';
			len += xdb_sprint_field (pField, (void*)pNewRow, buf+len);
			buf[len++] = ',';
		}
		buf[len-1] = ' ';
		len += sprintf (buf+len, "WHERE ");
		// fall through
	case XDB_TRIG_AFT_DEL:
		if (XDB_TRIG_AFT_DEL == type) {
			len = sprintf (buf, "DELETE FROM %s WHERE ", XDB_OBJ_NAME(pTblm));
		}
		if (pTblm->bPrimary) {
			xdb_idxm_t *pIdxm = XDB_OBJM_GET(pTblm->idx_objm, 0);
			ppFields = pIdxm->pFields;
			count = pIdxm->fld_count;
		} else {
			ppFields = pTblm->ppFields;
			count = pTblm->fld_count <= XDB_MAX_MATCH_COL ? pTblm->fld_count : XDB_MAX_MATCH_COL;
		}
		for (int i = 0; i < count; ++i) {
			if (xdb_likely (i > 0)) {
				memcpy (buf + len, " AND ", 5);
				len += 5;
			}
			pField = ppFields[i];
			memcpy (buf+len, pField->obj.obj_name, pField->obj.nm_len);
			len += pField->obj.nm_len;
			buf[len++] = '=';
			len += xdb_sprint_field (pField, (void*)pOldRow, buf+len);
		}
		buf[len++] = ';';
		buf[len++] = '\0';
	}

	printf ("DBLOG: %s\n", buf);
	return 0;
}

XDB_STATIC xdb_rowid 
xdb_row_insert (xdb_conn_t *pConn, xdb_tblm_t *pTblm, void *pRow, int bUpdOrRol)
{
	xdb_stgmgr_t	*pStgMgr = &pTblm->stg_mgr;

	void *pRowDb;
	xdb_rowid vid = 0;
	uint8_t vtype = XDB_VTYPE_NONE;
	xdb_rowid rid = xdb_stg_alloc (pStgMgr, &pRowDb);
	if (xdb_unlikely (rid <= 0)) {
		xdb_dbglog ("No space");
		return -1;
	}
	//xdb_dbglog ("alloc %d max%d\n", rid, XDB_STG_CAP(pStgMgr));

	memcpy (pRowDb, pRow, pTblm->row_size);

	uint64_t	flds_ptr[XDB_MAX_COLUMN];
	int			bef_trig_cnt = XDB_OBJM_COUNT(pTblm->trig_objm[XDB_TRIG_BEF_INS]);
	int			aft_trig_cnt = XDB_OBJM_COUNT(pTblm->trig_objm[XDB_TRIG_AFT_INS]);
	int			trig_cnt = bef_trig_cnt + aft_trig_cnt;
	if (xdb_unlikely (trig_cnt && !bUpdOrRol)) {
		__xdb_fetch_row (pTblm, pRow, flds_ptr);
		if (bef_trig_cnt) {
			xdb_call_trigger (pConn, pTblm, XDB_TRIG_BEF_INS, flds_ptr, flds_ptr);
		}
	}

	if (pTblm->pVdatm != NULL) {
		int vlen = 0;
		xdb_str_t *pVStr = pRow + pTblm->row_size, *pStr;
		for (int i = 0; i < pTblm->vfld_count; ++i) {
			pStr = &pVStr[i];
			if (xdb_unlikely (NULL == pStr->str)) {
				continue;
			}
			vlen += XDB_ALIGN4 (sizeof(xdb_vchar_t) + pStr->len + 1);
		}
		void *pVdat;
		if (vlen > 0) {
			vid = xdb_vdata_alloc (pTblm->pVdatm, &vtype, &pVdat, vlen+4);
			*(uint32_t*)pVdat = (0x8 << XDB_VDAT_LENBITS) | vlen; // b1000
			pVdat += 4; // skip tot vdat len
			int offset = 0;
			for (int i = 0; i < pTblm->vfld_count; ++i) {
				xdb_vchar_t *pVchar = pVdat;
				pStr = &pVStr[i];
				if (xdb_unlikely (NULL == pStr->str)) {
					continue;
				}
				vlen = XDB_ALIGN4 (sizeof(xdb_vchar_t) + pStr->len + 1);
				pVchar->cap = vlen - sizeof(xdb_vchar_t);
				pVchar->len = pStr->len;
				memcpy (pVchar->vchar, pStr->str, pStr->len + 1);
				*(int32_t*)(pRowDb+pTblm->ppVFields[i]->fld_off) = offset + sizeof(xdb_vchar_t);
				pVdat += vlen;
				offset += vlen;
			}
		}
		*(xdb_rowid*)(pRowDb + pTblm->row_size) = vid;
	}

	*(uint8_t*)(pRowDb + pTblm->vtype_off) = vtype;

	if (xdb_likely (pTblm->bMemory && pConn->bAutoTrans)) {
		// Fast insert
		// alloc set dirty ^ XDB_ROW_TRANS => XDB_ROW_COMMIT
		XDB_ROW_CTRL (pStgMgr->pStgHdr, pRowDb) &= XDB_ROW_MASK;
		XDB_ROW_CTRL (pStgMgr->pStgHdr, pRowDb) |= XDB_ROW_COMMIT;
		int rc = xdb_idx_addRow (pConn, pTblm, rid, pRowDb);
		if (xdb_unlikely (XDB_OK != rc)) {
			xdb_stg_free (pStgMgr, rid, pRowDb);
			rid = -1;
		}
	} else {
		XDB_ROW_CTRL (pStgMgr->pStgHdr, pRowDb) |= XDB_ROW_TRANS;

		int rc = xdb_idx_addRow (pConn, pTblm, rid, pRowDb);
		if (xdb_likely (XDB_OK == rc)) {
			xdb_trans_addrow (pConn, pTblm, rid, true);
		} else {
			xdb_stg_free (pStgMgr, rid, pRowDb);
			if (XDB_VTYPE_OK(vtype)) {
				xdb_vdata_free (pTblm->pVdatm, vtype, vid);
			}
			rid = -1;
		}
	}

	if (xdb_unlikely (aft_trig_cnt && !bUpdOrRol)) {
		xdb_call_trigger (pConn, pTblm, XDB_TRIG_AFT_INS, flds_ptr, flds_ptr);
	}
	if (!bUpdOrRol) {
		xdb_dbrow_log (pTblm, XDB_TRIG_AFT_INS, pRowDb, NULL, NULL, 0);
	}

	return rid;
}

static inline void 
__xdb_row_delete (xdb_tblm_t *pTblm, xdb_rowid rid, void *pRow)
{
	xdb_idx_remRow (pTblm, rid, pRow);
	if (pTblm->vfld_count > 0) {
		xdb_row_vdata_free (pTblm, pRow);
	}
	xdb_stg_free (&pTblm->stg_mgr, rid, pRow);
}

XDB_STATIC int 
xdb_row_delete (xdb_conn_t *pConn, xdb_tblm_t *pTblm, xdb_rowid rid, void *pRow, int bUpdOrRol)
{
	uint8_t ctrl = XDB_ROW_CTRL (pTblm->stg_mgr.pStgHdr, pRow) & XDB_ROW_MASK;

	uint64_t	flds_ptr[XDB_MAX_COLUMN];
	int			bef_trig_cnt = XDB_OBJM_COUNT(pTblm->trig_objm[XDB_TRIG_BEF_DEL]);
	int			aft_trig_cnt = XDB_OBJM_COUNT(pTblm->trig_objm[XDB_TRIG_AFT_DEL]);
	int			trig_cnt = bef_trig_cnt + aft_trig_cnt;
	if (xdb_unlikely (trig_cnt && !bUpdOrRol)) {
		__xdb_fetch_row (pTblm, pRow, flds_ptr);
		if (bef_trig_cnt) {
			xdb_call_trigger (pConn, pTblm, XDB_TRIG_BEF_DEL, flds_ptr, flds_ptr);
		}
	}

	if (xdb_likely ((pTblm->bMemory && pConn->bAutoTrans) || (XDB_ROW_TRANS == ctrl))) {
		__xdb_row_delete (pTblm, rid, pRow);
		if (xdb_unlikely (XDB_ROW_TRANS == ctrl)) {
			// del from new row list
			xdb_trans_delrow (pConn, pTblm, rid);
		}
	} else {
		// add to dlt row list
		xdb_trans_addrow (pConn, pTblm, rid, false);
	}

	if (xdb_unlikely (aft_trig_cnt && !bUpdOrRol)) {
		xdb_call_trigger (pConn, pTblm, XDB_TRIG_AFT_DEL, flds_ptr, flds_ptr);
	}

	if (!bUpdOrRol) {
		xdb_dbrow_log (pTblm, XDB_TRIG_AFT_DEL, NULL, pRow, NULL, 0);
	}

	return 0;
}

XDB_STATIC int 
xdb_row_update (xdb_conn_t *pConn, xdb_tblm_t *pTblm, xdb_rowid rid, void *pRow, xdb_setfld_t set_flds[], int set_count)
{
	uint64_t idx_bmp = 0;
	XDB_BUF_DEF(pUpdRow, 4096);
	bool	bCopy = false;
	uint8_t idx_affect[XDB_MAX_INDEX];
	int		idx_count = 0;

	int 		row_vlen = 0;
	if (pTblm->vfld_count > 0) {
		row_vlen = pTblm->vfld_count * sizeof(xdb_str_t);
		XDB_BUF_ALLOC (pUpdRow, pTblm->pMeta->row_size + row_vlen);
		XDB_EXPECT (pUpdRow != NULL, XDB_E_MEMORY, "Can't alloc memory");
		memcpy (pUpdRow, pRow, pTblm->pMeta->row_size);
		memset (pUpdRow + pTblm->row_size, 0, row_vlen);
		// will expand the vdata as if of the same vdata, the update will remap and addr will be invalid
		void *pVdat = xdb_row_vdata_get2 (pTblm, pRow);
		if (pVdat != NULL) {
			pVdat += 4; // skip tot len
			xdb_str_t *pVStr = (void*)pUpdRow + pTblm->row_size, *pStr;
			for (int i = 0; i < pTblm->vfld_count; ++i) {
				int voff = *(int32_t*)(pRow + pTblm->ppVFields[i]->fld_off);
				if (voff) {
					void *pVfld = pVdat + voff;
					pStr = &pVStr[i];
					pStr->len = *(uint16_t*)(pVfld - 2);
					pStr->str = pVfld;
				}
			}
		}
		xdb_row_set (pTblm, pUpdRow, set_flds, set_count);
		bCopy = true;
	}

	uint64_t	old_flds_ptr[XDB_MAX_COLUMN];
	uint64_t	new_flds_ptr[XDB_MAX_COLUMN];
	uint64_t	upd_flds_ptr[XDB_MAX_COLUMN];
	int			bef_trig_cnt = XDB_OBJM_COUNT(pTblm->trig_objm[XDB_TRIG_BEF_UPD]);
	int			aft_trig_cnt = XDB_OBJM_COUNT(pTblm->trig_objm[XDB_TRIG_AFT_UPD]);
	int			trig_cnt = bef_trig_cnt + aft_trig_cnt;

	if (xdb_unlikely (trig_cnt)) {
		__xdb_fetch_row (pTblm, pRow, old_flds_ptr);
		__xdb_fetch_row (pTblm, pUpdRow, new_flds_ptr);
		memcpy (upd_flds_ptr, old_flds_ptr, pTblm->fld_count * 8);
	}

	for (int i = 0; i < set_count; ++i) {
		xdb_field_t 	*pField = set_flds[i].pField;
		idx_bmp |= pField->idx_bmp;
		if (xdb_unlikely (trig_cnt)) {
			upd_flds_ptr[pField->fld_id] = new_flds_ptr[pField->fld_id];
		}
	}
	if (xdb_unlikely (bef_trig_cnt)) {
		xdb_call_trigger (pConn, pTblm, XDB_TRIG_BEF_UPD, upd_flds_ptr, old_flds_ptr);
	}

	if (xdb_unlikely (idx_bmp)) {
		for (int i = 0; i < XDB_OBJM_COUNT (pTblm->idx_objm); ++i) {
			xdb_idxm_t *pIdxm = XDB_OBJM_GET (pTblm->idx_objm, pTblm->idx_order[i]);
			if ((1<<XDB_OBJ_ID(pIdxm)) & idx_bmp) {
				idx_affect[idx_count++] = pTblm->idx_order[i];
			}
			if (pIdxm->bUnique) {
				if (xdb_unlikely (!bCopy)) {
					XDB_BUF_ALLOC (pUpdRow, pTblm->pMeta->row_size);
					XDB_EXPECT (pUpdRow != NULL, XDB_E_MEMORY, "Can't alloc memory");
					memcpy (pUpdRow, pRow, pTblm->pMeta->row_size);
					xdb_row_set (pTblm, pUpdRow, set_flds, set_count);
					bCopy = true;
				}
				xdb_rowid rid2 = pIdxm->pIdxOps->idx_query2 (pConn, pIdxm, pUpdRow);
				if (xdb_unlikely ((rid2 > 0) && (rid2 != rid))) {
					XDB_EXPECT ((rid2 == rid) || (0 == rid2), XDB_E_EXISTS, "Duplicate entry");
				}
			}
		}
	}

	if (!bCopy && ((pTblm->bMemory && pConn->bAutoTrans) || (XDB_ROW_TRANS == (XDB_ROW_CTRL (pTblm->stg_mgr.pStgHdr, pRow) & XDB_ROW_MASK)))) {
		xdb_idx_remRow_bmp (pTblm, rid, pRow, idx_affect, idx_count);
		xdb_row_set (pTblm, pRow, set_flds, set_count);
		xdb_idx_addRow_bmp (pConn, pTblm, rid, pRow, idx_affect, idx_count);
	} else {
		if (!bCopy) {
			XDB_BUF_ALLOC (pUpdRow, pTblm->pMeta->row_size);
			memcpy (pUpdRow, pRow, pTblm->pMeta->row_size);
			XDB_EXPECT (pUpdRow != NULL, XDB_E_MEMORY, "Can't alloc memory");
			xdb_row_set (pTblm, pUpdRow, set_flds, set_count);
		}
		uint8_t type = XDB_VTYPE_NONE;
		xdb_rowid vid = 0;
		if (pTblm->vfld_count > 0) {
			vid = xdb_row_vdata_info (pTblm->row_size, pRow, &type);
			if (XDB_VTYPE_OK(type)) {
				// old vdata ptr is shared by new upd row, so need to hold in case free
				xdb_vdata_ref (pTblm->pVdatm, type, vid);
			}
		}
		xdb_row_delete (pConn, pTblm, rid, pRow, 1);
		xdb_row_insert (pConn, pTblm, pUpdRow, 1);	
		if (XDB_VTYPE_OK(type)) {
			xdb_vdata_free (pTblm->pVdatm, type, vid);
		}
	}

	if (xdb_unlikely (aft_trig_cnt)) {
		xdb_call_trigger (pConn, pTblm, XDB_TRIG_AFT_UPD, upd_flds_ptr, old_flds_ptr);
	}

	xdb_dbrow_log (pTblm, XDB_TRIG_AFT_UPD, pUpdRow, pRow, set_flds, set_count);

	XDB_BUF_FREE (pUpdRow);

	return 0;

error:
	XDB_BUF_FREE (pUpdRow);
	return - XDB_CONNCODE (pConn);
}

static inline void 
xdb_mark_dirty (xdb_tblm_t *pTblm)
{
	if (xdb_unlikely (!pTblm->bMemory)) {
		xdb_tbl_t *pTbl = XDB_TBLPTR(pTblm);
		xdb_atomic_add (&pTbl->lastchg_id, 1);
		if (xdb_unlikely (pTbl->flush_id + 1 == pTbl->lastchg_id)) {
			xdb_stg_sync (&pTblm->stg_mgr, 0, 4094, true);
		}

		xdb_db_t *pDb = XDB_DBPTR(pTblm->pDbm);
		xdb_atomic_add (&pDb->lastchg_id, 1);
		if (xdb_unlikely (pDb->flush_id + 1 == pDb->lastchg_id)) {
			xdb_stg_sync (&pTblm->pDbm->stg_mgr, 0, 4094, true);
		}
	}
}

XDB_STATIC xdb_rowid 
xdb_sql_insert (xdb_stmt_insert_t *pStmt)
{
	//xdb_dbgprint ("ok insert tbl '%s' count %d\n", pStmt->XDB_OBJ_NAME(pTblm), pStmt->row_count);
	xdb_rowid 		count = 0;
	xdb_conn_t		*pConn = pStmt->pConn;
	xdb_tblm_t		*pTblm = pStmt->pTblm;

	xdb_wrlock_tblstg (pTblm);

	xdb_mark_dirty (pTblm);

	for (int i = 0 ; i < pStmt->row_count; ++i) {
		void *pRow = pStmt->pRowsBuf + pStmt->row_offset[i];
		xdb_rowid rid = xdb_row_insert (pConn, pTblm, pRow, 0);
		if (rid > 0) {
			count ++;
		}
	}

	xdb_wrunlock_tblstg (pTblm);

	return count;
}

XDB_STATIC xdb_rowid 
xdb_sql_update (xdb_stmt_select_t *pStmt)
{
	// filter rows
	// inplace update for each row, update directly
	// update index
	// alloc new row, copy rows, copy fields, for var part, copy one by one
	// remove old index, update new index(if idx no touch, just update pointer)

	xdb_conn_t		*pConn = pStmt->pConn;
	xdb_tblm_t		*pTblm = pStmt->pTblm;
	xdb_res_t		*pRes = &pConn->conn_res;
	xdb_rowid 		count = 0;

	xdb_wrlock_tblstg (pTblm);

	xdb_mark_dirty (pTblm);

	xdb_rowset_t	*pRowSet = &pConn->row_set;
	xdb_sql_filter (pStmt);
	pRes->affected_rows = pRowSet->count;

	for (xdb_rowid id = 0 ; id < pRowSet->count; ++id) {
		xdb_rowptr_t *pRow = &pRowSet->pRowList[id];
		// insert may cause remap
		pRow->ptr = XDB_IDPTR (&pTblm->stg_mgr, pRow->rid);
		int rc = xdb_row_update (pConn, pTblm, pRow->rid, pRow->ptr, pStmt->set_flds, pStmt->set_count);
		if (0 == rc) {
			count ++;
		}
	}
	
	xdb_rowset_clean (pRowSet);

	xdb_wrunlock_tblstg (pTblm);

	return count;
}

XDB_STATIC xdb_rowid 
xdb_sql_delete (xdb_stmt_select_t *pStmt)
{
	xdb_conn_t		*pConn = pStmt->pConn;
	xdb_tblm_t		*pTblm = pStmt->pTblm;
	xdb_res_t		*pRes = &pConn->conn_res;
	xdb_rowid 		count = 0;

	xdb_wrlock_tblstg (pTblm);

	xdb_mark_dirty (pTblm);

	xdb_rowset_t	*pRowSet = &pConn->row_set;
	xdb_sql_filter (pStmt);
	pRes->affected_rows = pRowSet->count;

	for (xdb_rowid id = 0 ; id < pRowSet->count; ++id) {
		xdb_rowptr_t *pRow = &pRowSet->pRowList[id];
		int rc = xdb_row_delete (pConn, pTblm, pRow->rid, pRow->ptr, 0);
		if (0 == rc) {
			count ++;
		}
	}

	xdb_rowset_clean (pRowSet);

	xdb_wrunlock_tblstg (pTblm);

	return count;
}
