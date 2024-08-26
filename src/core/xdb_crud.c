/******************************************************************************
* Copyright (c) 2024 CrossDB ORG.
*
*   https://crossdb.org
*   https://github.com/crossdb-org/crossdb
*
* This program is free software: you can use, redistribute, and/or modify
* it under the terms of the GNU Affero General Public License, version 3
* or later ("AGPL"), as published by the Free Software Foundation.
*
* This program is distributed in the hope that it will be useful, but WITHOUT
* ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
* FITNESS FOR A PARTICULAR PURPOSE.
*
* You should have received a copy of the GNU Affero General Public License
* along with this program. If not, see <http://www.gnu.org/licenses/>.
******************************************************************************/

#define XDB_IS_NULL(pNull,bits)	(pNull[bits>>3] & (1<<(bits&7)))

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
					*pRowFld =  (uintptr_t)(pRow + pColBase[i]->col_off);
				}
			} else {
				uint8_t *pNull = (uint8_t*)(pRow + pMeta->null_off);
				for (int i = 0; i < pMeta->col_count; ++i, ++pRowFld) {
					if (!XDB_IS_NULL(pNull,i)) {
						*pRowFld = (uintptr_t)(pRow + pColBase[i]->col_off);
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
}

XDB_STATIC bool 
xdb_row_isequal (void *pRow, xdb_field_t **ppFields, xdb_value_t *pVals[], int count)
{
	xdb_value_t **pValues = pVals;
	xdb_field_t **ppField = ppFields;
	for (int i = 0; i < count; ++i, ++pValues, ++ppField) {
		int len;
		xdb_field_t *pField = *ppField;
		xdb_value_t	*pValue = *pValues;
		void *pRowVal = pRow + pField->fld_off;
		switch (pField->fld_type) {
		case XDB_TYPE_INT:
			if (pValue->ival != *(int32_t*)pRowVal) {
				return false;
			}
			break;
		case XDB_TYPE_TINYINT:
			if (pValue->ival != *(int8_t*)pRowVal) {
				return false;
			}
			break;
		case XDB_TYPE_SMALLINT:
			if (pValue->ival != *(int16_t*)pRowVal) {
				return false;
			}
			break;
		case XDB_TYPE_BIGINT:
			if (pValue->ival != *(int64_t*)pRowVal) {
				return false;
			}
			break;
		case XDB_TYPE_FLOAT:
			if (pValue->fval != *(float*)pRowVal) {
				return false;
			}
			break;
		case XDB_TYPE_DOUBLE:
			if (pValue->fval != *(double*)pRowVal) {
				return false;
			}
			break;
		case XDB_TYPE_CHAR:
			len = *(uint16_t*)(pRowVal-2);
			if (len != pValue->str.len) {
				return false;
			}
			//if (memcmp (pRowVal, pValue->str.ptr, len)) {
			if (strcasecmp (pRowVal, pValue->str.ptr)) {
				return false;
			}
			break;
		}
	}

	return true;
}

XDB_STATIC bool 
xdb_row_isequal2 (void *pRowL, void *pRowR, xdb_field_t **ppFields, int count)
{
	xdb_field_t **ppField = ppFields;
	for (int i = 0; i < count; ++i, ++ppField) {
		int lenL, lenR;
		xdb_field_t *pField = *ppField;
		void *pRowValL = pRowL + pField->fld_off;
		void *pRowValR = pRowR + pField->fld_off;
		switch (pField->fld_type) {
		case XDB_TYPE_INT:
			if (*(int32_t*)pRowValL != *(int32_t*)pRowValR) {
				return false;
			}
			break;
		case XDB_TYPE_BIGINT:
			if (*(int64_t*)pRowValL != *(int64_t*)pRowValR) {
				return false;
			}
			break;
		case XDB_TYPE_TINYINT:
			if (*(int8_t*)pRowValL != *(int8_t*)pRowValR) {
				return false;
			}
			break;
		case XDB_TYPE_SMALLINT:
			if (*(int16_t*)pRowValL != *(int16_t*)pRowValR) {
				return false;
			}
			break;
		case XDB_TYPE_FLOAT:
			if (*(float*)pRowValL != *(float*)pRowValR) {
				return false;
			}
			break;
		case XDB_TYPE_DOUBLE:
			if (*(double*)pRowValL != *(double*)pRowValR) {
				return false;
			}
			break;
		case XDB_TYPE_CHAR:
			lenL = *(uint16_t*)(pRowValL-2);
			lenR = *(uint16_t*)(pRowValR-2);
			if (lenL != lenR) {
				return false;
			}
			//if (memcmp (pRowValL, pRowValR, lenL)) {
			if (strcasecmp (pRowValL, pRowValR)) {
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
		}
	}

	return 0;
}

XDB_STATIC bool 
xdb_row_and_match (void *pRow, xdb_filter_t **pFilterList, int count)
{
	for (int i = 0; i < count; ++i, ++pFilterList) {
		xdb_filter_t *pFilter = *pFilterList;
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
			break;
		case XDB_TYPE_DOUBLE:
			value.fval = *(double*)pVal;
			value.val_type = XDB_TYPE_DOUBLE;
			break;
		case XDB_TYPE_CHAR:
			value.str.len = *(uint16_t*)(pVal-2);
			value.str.ptr = pVal;
			value.val_type = XDB_TYPE_CHAR;
			break;
		default:
			value.val_type = XDB_TYPE_NULL;
			break;
		}

		switch (pValue->val_type) {
		case XDB_TYPE_BIGINT:
			if (xdb_unlikely (value.val_type != XDB_TYPE_BIGINT)) {
				return 0;
			}
			switch (pFilter->cmp_op) {
			case XDB_OP_EQ: 
				if (value.ival != pValue->ival) {
					return 0;
				}
				break;
			}
			break;

		case XDB_TYPE_DOUBLE:
			if (xdb_unlikely (value.val_type != XDB_TYPE_DOUBLE)) {
				return 0;
			}
			switch (pFilter->cmp_op) {
			case XDB_OP_EQ: 
				if (value.fval == pValue->fval) {
					return 0;
				}
				break;
			}
			break;

		case XDB_TYPE_CHAR:
			if (xdb_unlikely (value.val_type != XDB_TYPE_CHAR)) {
				return 0;
			}
			switch (pFilter->cmp_op) {
			case XDB_OP_EQ: 
				if (value.str.len != pValue->str.len) {
					return 0;
				}
				if (strcasecmp (value.str.ptr, pValue->str.ptr)) {
				//if (memcmp (value.str.ptr, pValue->str.ptr, value.str.len)) {
					return 0;
				}
				break;
			}
			break;
		}
	}

	return 1;
}

XDB_STATIC void 
xdb_row_getval (void *pRow, xdb_value_t *pVal)
{
	void *pFldPtr = pRow + pVal->pField->fld_off;
	switch (pVal->pField->fld_type) {
	case XDB_TYPE_INT:
		pVal->ival = *(int32_t*)pFldPtr;
		pVal->sup_type = XDB_TYPE_BIGINT;
		break;
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
		pVal->str.ptr = (char*)pFldPtr;
		pVal->str.len = *(uint16_t*)(pFldPtr - 2);
		pVal->sup_type = XDB_TYPE_CHAR;
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

XDB_STATIC void 
xdb_row_copy (void *pRow, xdb_setfld_t set_flds[], int count)
{
	for (int i = 0; i < count; ++i) {
		xdb_setfld_t 	*pSetFld 	= &set_flds[i];
		xdb_field_t		*pField = pSetFld->pField;
		xdb_value_t		*pVal	= &pSetFld->set_val[0];
		void			*pFldPtr= pRow + pField->fld_off;
		xdb_value_t 	*pResVal; 

		if (xdb_unlikely ((pSetFld->set_op != XDB_OP_EQ) || (XDB_TYPE_FIELD == pVal->val_type))) {
			if (xdb_likely (XDB_TYPE_FIELD == pVal->val_type)) {
				xdb_row_getval (pRow, pVal);
			}
			if (xdb_likely (pSetFld->set_op != XDB_OP_EQ)) {
				xdb_value_t		*pVal2	= &pSetFld->set_val[1];
				pResVal = &pSetFld->res_val; 
				if (xdb_unlikely (XDB_TYPE_FIELD == pVal2->val_type)) {
					xdb_row_getval (pRow, pVal2);
				}
				if (xdb_unlikely (pVal->sup_type != pVal2->sup_type)) {
					xdb_convert_val (pVal2, pVal->sup_type);
				}
				pResVal->sup_type = pVal->sup_type;
				switch (pSetFld->set_op) {
				case XDB_OP_ADD:
					if (XDB_TYPE_BIGINT == pVal->sup_type) {
						pResVal->ival = pVal->ival + pVal2->ival;
					} else {
						pResVal->fval = pVal->fval + pVal2->fval;
					}
					break;
				case XDB_OP_SUB:
					if (XDB_TYPE_BIGINT == pVal->sup_type) {
						pResVal->ival = pVal->ival - pVal2->ival;
					} else {
						pResVal->fval = pVal->fval - pVal2->fval;
					}
					break;
				case XDB_OP_MUL:
					if (XDB_TYPE_BIGINT == pVal->sup_type) {
						pResVal->ival = pVal->ival * pVal2->ival;
					} else {
						pResVal->fval = pVal->fval * pVal2->fval;
					}
					break;
				case XDB_OP_DIV:
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
				pVal = pResVal;
			}
		}

		if (xdb_unlikely (pField->sup_type != pVal->sup_type)) {
			xdb_convert_val (pVal, pField->sup_type);
		}

		switch (pField->fld_type) {
		case XDB_TYPE_INT:
			*(int32_t*)(pFldPtr) = pVal->ival;
			break;
		case XDB_TYPE_BIGINT:
			*(int64_t*)(pFldPtr) = pVal->ival;
			break;
		case XDB_TYPE_TINYINT:
			*(int8_t*)(pFldPtr) = pVal->ival;
			break;
		case XDB_TYPE_SMALLINT:
			*(int16_t*)(pFldPtr) = pVal->ival;
			break;
		case XDB_TYPE_FLOAT:
			*(float*)(pFldPtr) = pVal->fval;
			break;
		case XDB_TYPE_DOUBLE:
			*(double*)(pFldPtr) = pVal->fval;
			break;
		case XDB_TYPE_CHAR:
			memcpy (pFldPtr, pVal->str.ptr, pVal->str.len+1);
			*(uint16_t*)(pFldPtr-2) = pVal->str.len;
			break;
		}
	}
}

XDB_STATIC uint64_t 
xdb_row_hash (void *pRow, xdb_field_t *pFields[], int count)
{
	uint64_t hashsum = 0, hash;
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
		case XDB_TYPE_CHAR:
			//hash = xdb_wyhash (ptr, *(uint16_t*)(ptr-2));
			hash = xdb_strcasehash (ptr, *(uint16_t*)(ptr-2));
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
xdb_val_hash (xdb_value_t *pValues[], int count)
{
	uint64_t hashsum = 0, hash;
	xdb_value_t **ppValue = pValues;
	for (int i = 0; i < count; ++i, ++ppValue) {
		xdb_value_t *pValue = *ppValue;
		switch (pValue->val_type) {
		case XDB_TYPE_BIGINT:
			hash = pValue->ival;
			break;
		case XDB_TYPE_DOUBLE:
			hash = (uint64_t)pValue->fval;
			break;
		case XDB_TYPE_CHAR:
			//hash = xdb_wyhash (pValue->str.ptr, pValue->str.len);
			hash = xdb_strcasehash (pValue->str.ptr, pValue->str.len);
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
xdb_fld_setStr (xdb_field_t * pField, void *pRow, const char *str, int len)
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

int64_t xdb_column_int64 (uint64_t meta, xdb_row_t *pRow, uint16_t iCol)
{
	xdb_meta_t *pMeta = (xdb_meta_t*)meta;
	if (xdb_unlikely (iCol >= pMeta->col_count)) {
		return 0;
	}
	xdb_col_t	*pCol = ((xdb_col_t**)pMeta->col_list)[iCol];

	switch (pCol->col_type) {
	case XDB_TYPE_INT:
		return *(int32_t*)pRow[iCol];
	case XDB_TYPE_BIGINT:
		return *(int64_t*)pRow[iCol];
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
	if (xdb_unlikely (iCol >= pMeta->col_count)) {
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
	if (xdb_unlikely (iCol >= pMeta->col_count)) {
		return NULL;
	}
	xdb_col_t	*pCol = ((xdb_col_t**)pMeta->col_list)[iCol];

	switch (pCol->col_type) {
	case XDB_TYPE_CHAR:
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
			fprintf (pFile, "%s='%s' ", pCol[i]->col_name, (char*)pVal);
			break;
		}
	}
	return 0;
}

int xdb_print_row (uint64_t meta, xdb_row_t *pRow, int format)
{
	return xdb_fprint_row (stdout, meta, pRow, format);
}

XDB_STATIC void 
xdb_rowset_init (xdb_rowset_t *pRowSet)
{
	memset (&pRowSet->count, 0, sizeof(pRowSet->count) * 4);
	if (NULL == pRowSet->pRowList) {
		pRowSet->cap	= XDB_ARY_LEN (pRowSet->rowlist);
		pRowSet->pRowList	= pRowSet->rowlist;
	}
}

XDB_STATIC int 
xdb_rowset_add (xdb_rowset_t *pRowSet, xdb_rowid rid, void *ptr)
{
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
xdb_sql_filter (xdb_stmt_select_t *pStmt)
{
	xdb_conn_t		*pConn = pStmt->pConn;
	xdb_tblm_t		*pTblm = pStmt->pTblm;
	xdb_stgmgr_t	*pStgMgr = &pTblm->stg_mgr;
	xdb_rowset_t 	*pRowSet = &pConn->row_set;

	pRowSet->pTblMeta = pStmt->pTblm->pMeta;
	pRowSet->pFldMap  = NULL;
	if (xdb_unlikely (pStmt->limit <= 0)) {
		return XDB_OK;
	}

	if (xdb_likely (0 == pStmt->order_count)) {
		// set limit and offset
		memcpy (&pRowSet->limit, &pStmt->limit, sizeof(pRowSet->limit) * 2);
	} else {
		pRowSet->limit = XDB_MAX_ROWS;
		pRowSet->offset = 0;
	}

	if (pStmt->filter_count > 0 && (NULL != pStmt->pIdxm)) {
		pStmt->pIdxm->pIdxOps->idx_query (pConn, pStmt->pIdxm, pStmt->pIdxVals, pStmt->pIdxFlts, pStmt->idx_flt_cnt, pRowSet);
	} else {
		xdb_rowid max_rid = XDB_STG_MAXID(pStgMgr);
		for (xdb_rowid rid = 1; rid <= max_rid; ++rid) {
			void *pRow = XDB_IDPTR(pStgMgr, rid);
			if (xdb_row_valid (pConn, pTblm, pRow, rid)) {
				if (pStmt->filter_count > 0) {
					bool ret = xdb_row_and_match (pRow, pStmt->pFilters, pStmt->filter_count);
					if (!ret) {
						continue;
					}
				}
				if (xdb_unlikely (-XDB_E_FULL == xdb_rowset_add (pRowSet, rid, pRow))) {
					return XDB_OK;
				}
			}
		}
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
		xdb_col_t	**pCol = (xdb_col_t**)pStmt->pMeta->col_list;
		xdb_col_t	**pTblCol = (xdb_col_t**)pTblm->pMeta->col_list;

		for (int i = 0; i < pStmt->pMeta->col_count; ++i) {
			if (XDB_AGGFUNC_COUNT == pStmt->agg_func[i]) {
				ival_col[i] = 1;
				continue;
			}
			if (XDB_AGGFUNC_AVG == pStmt->agg_func[i]) {
				agg_count[i] = 1;
			}
			switch (pTblCol[pStmt->agg_fid[i]]->col_type) {
			case XDB_TYPE_INT:
			case XDB_TYPE_BIGINT:
			case XDB_TYPE_TINYINT:
			case XDB_TYPE_SMALLINT:
				ival_col[i] = xdb_row_getInt ((uintptr_t)pTblm->pMeta, pRow, pStmt->agg_fid[i]);
				break;
			case XDB_TYPE_FLOAT:
			case XDB_TYPE_DOUBLE:
				fval_col[i] = xdb_row_getFloat ((uintptr_t)pTblm->pMeta, pRow, pStmt->agg_fid[i]);
				break;
			}
		}

		for (xdb_rowid rid = 1; rid < pRowSet->count; ++rid) {
			pRow = pRowSet->pRowList[rid].ptr;
			for (int i = 0; i < pStmt->pMeta->col_count; ++i) {
				switch (pStmt->agg_func[i]) {
				case XDB_AGGFUNC_COUNT:
					ival_col[i]++;
					break;
				case XDB_AGGFUNC_MAX:
					switch (pCol[i]->col_type) {
					case XDB_TYPE_INT:
					case XDB_TYPE_BIGINT:
					case XDB_TYPE_TINYINT:
					case XDB_TYPE_SMALLINT:
						val = xdb_row_getInt ((uintptr_t)pTblm->pMeta, pRow, pStmt->agg_fid[i]);
						if (val > ival_col[i]) {
							ival_col[i] = val;
						}
						break;
					case XDB_TYPE_FLOAT:
					case XDB_TYPE_DOUBLE:
						fval = xdb_row_getFloat ((uintptr_t)pTblm->pMeta, pRow, pStmt->agg_fid[i]);
						if (fval > fval_col[i]) {
							fval_col[i] = fval;
						}
						break;
					}
					break;
				case XDB_AGGFUNC_MIN:
					switch (pCol[i]->col_type) {
					case XDB_TYPE_INT:
					case XDB_TYPE_BIGINT:
					case XDB_TYPE_TINYINT:
					case XDB_TYPE_SMALLINT:
						val = xdb_row_getInt ((uintptr_t)pTblm->pMeta, pRow, pStmt->agg_fid[i]);
						if (val < ival_col[i]) {
							ival_col[i] = val;
						}
						break;
					case XDB_TYPE_FLOAT:
					case XDB_TYPE_DOUBLE:
						fval = xdb_row_getFloat ((uintptr_t)pTblm->pMeta, pRow, pStmt->agg_fid[i]);
						if (fval < fval_col[i]) {
							fval_col[i] = fval;
						}
						break;
					}
					break;
				case XDB_AGGFUNC_AVG:
					agg_count[i]++;
				case XDB_AGGFUNC_SUM:				
					switch (pTblCol[pStmt->agg_fid[i]]->col_type) {
					case XDB_TYPE_INT:
					case XDB_TYPE_BIGINT:
					case XDB_TYPE_TINYINT:
					case XDB_TYPE_SMALLINT:
						val = xdb_row_getInt ((uintptr_t)pTblm->pMeta, pRow, pStmt->agg_fid[i]);
						ival_col[i] += val;
						break;
					case XDB_TYPE_FLOAT:
					case XDB_TYPE_DOUBLE:
						fval = xdb_row_getFloat ((uintptr_t)pTblm->pMeta, pRow, pStmt->agg_fid[i]);
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

		for (int i = 0; i < pStmt->pMeta->col_count; ++i) {
			switch (pStmt->agg_func[i]) {
				case XDB_AGGFUNC_AVG:
					switch (pTblCol[pStmt->agg_fid[i]]->col_type) {
					case XDB_TYPE_INT:
					case XDB_TYPE_BIGINT:
					case XDB_TYPE_TINYINT:
					case XDB_TYPE_SMALLINT:
						//xdb_print ("%s %d %d %f\n", pCol[i]->col_name, ival_col[i], agg_count[i], (double)ival_col[i]/agg_count[i]);
						xdb_row_setFloat ((uintptr_t)pStmt->pMeta, pRow, i, (float)ival_col[i]/agg_count[i]);
						break;
					case XDB_TYPE_FLOAT:
					case XDB_TYPE_DOUBLE:
						//xdb_print ("%s %f %d %f\n", pCol[i]->col_name, fval_col[i], agg_count[i], fval_col[i]/agg_count[i]);
						xdb_row_setFloat ((uintptr_t)pStmt->pMeta, pRow, i, fval_col[i]/agg_count[i]);
						break;
					}					
					break;
				default:
					switch (pCol[i]->col_type) {
					case XDB_TYPE_INT:
					case XDB_TYPE_BIGINT:
					case XDB_TYPE_TINYINT:
					case XDB_TYPE_SMALLINT:
						//xdb_print ("%s %d\n", pCol[i]->col_name, ival_col[i]);
						xdb_row_setInt ((uintptr_t)pStmt->pMeta, pRow, i, ival_col[i]);
						break;
					case XDB_TYPE_FLOAT:
					case XDB_TYPE_DOUBLE:
						//xdb_print ("%s %f\n", pCol[i]->col_name, fval_col[i]);
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

XDB_STATIC xdb_res_t* 
xdb_sql_select (xdb_stmt_select_t *pStmt)
{
	xdb_conn_t		*pConn = pStmt->pConn;
	xdb_res_t		*pRes;

	xdb_rowset_t	*pRowSet = &pConn->row_set;

	xdb_rdlock_tblstg (pStmt->pTblm);

	pRes = xdb_queryres_alloc (pConn, XDB_ROW_BUF_SIZE);
	if (NULL != pRes) {
		goto exit;
	}

	xdb_sql_filter (pStmt);

	xdb_queryRes_t	*pQueryRes = pConn->pQueryRes;
	pRes = &pQueryRes->res;

	xdb_tblm_t		*pTblm = pStmt->pTblm;
	int				row_size = (pTblm->row_size + 3)>>2<<2;

	pRes->errcode = 0;
	pRes->status = 0;	// dynamic alloc
	//pRes->stmt_type = pStmt->stmt_type;

	pRes->affected_rows	= 0;
	pRes->row_count = pRowSet->count;
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

	for (xdb_rowid id = 0; id < pRowSet->count; ++id) {
		uint64_t	offset = (void*)pCurDat - (void*)pQueryRes;
		pCurDat->len_type = row_size + 4;
		if (pQueryRes->buf_free < pCurDat->len_type) {
			pRes = xdb_queryres_realloc (pConn, pQueryRes->buf_len<<1);
			if (NULL != pRes) {
				goto exit;
			}
			pQueryRes = pConn->pQueryRes;
			pCurDat = (void*)pQueryRes + offset;
		}
		memcpy (pCurDat->rowdat, pRowSet->pRowList[id].ptr, row_size);
		pQueryRes->buf_free -= pCurDat->len_type;
		pCurDat = (void*)pCurDat + sizeof(*pCurDat) + row_size;
	}

	pRes = &pQueryRes->res;
	pMeta = (xdb_meta_t*)(pRes + 1);
	pCurDat->len_type = 0;
	pRes->data_len = (void*)pCurDat - (void*)pMeta + 4;
	pRes = &pQueryRes->res;
	pRes->row_data = (uintptr_t)&pQueryRes->rowlist;

	xdb_init_rowlist (pQueryRes);

	xdb_fetch_rows (pRes);

exit:
	xdb_rowset_clean (pRowSet);
	xdb_rdunlock_tblstg (pStmt->pTblm);

	return pRes;
}

XDB_STATIC xdb_rowid 
xdb_row_insert (xdb_conn_t *pConn, xdb_tblm_t *pTblm, void *pRow)
{
	xdb_stgmgr_t	*pStgMgr = &pTblm->stg_mgr;

	void *pRowDb;
	xdb_rowid rid = xdb_stg_alloc (pStgMgr, &pRowDb);
	if (xdb_unlikely (rid <= 0)) {
		xdb_dbglog ("No space");
		return -1;
	}
	//xdb_dbglog ("alloc %d max%d\n", rid, XDB_STG_CAP(pStgMgr));

	memcpy (pRowDb, pRow, pTblm->row_size);

	if (xdb_likely (pTblm->bMemory && pConn->bAutoTrans)) {
		// Fast insert
		// alloc set dirty ^ XDB_ROW_TRANS => XDB_ROW_COMMIT
		*((uint8_t*)pRowDb+pStgMgr->pStgHdr->ctl_off) ^= XDB_ROW_TRANS;
		int rc = xdb_idx_addRow (pConn, pTblm, rid, pRowDb);
		if (xdb_unlikely (XDB_OK != rc)) {
			xdb_stg_free (pStgMgr, rid, pRowDb);
		}
	} else {
		*((uint8_t*)pRowDb+pStgMgr->pStgHdr->ctl_off) |= XDB_ROW_TRANS;

		int rc = xdb_idx_addRow (pConn, pTblm, rid, pRowDb);
		if (xdb_likely (XDB_OK == rc)) {
			xdb_trans_addrow (pConn, pTblm, rid, true);
		} else {
			xdb_stg_free (pStgMgr, rid, pRowDb);
		}
	}

	return rid;
}

XDB_STATIC int 
xdb_row_delete (xdb_conn_t *pConn, xdb_tblm_t *pTblm, xdb_rowid rid, void *pRow)
{
	uint8_t ctrl = XDB_ROW_CTRL (pTblm->stg_mgr.pStgHdr, pRow) & XDB_ROW_MASK;

	if (xdb_likely ((pTblm->bMemory && pConn->bAutoTrans) || (XDB_ROW_TRANS == ctrl))) {
		xdb_idx_remRow (pTblm, rid, pRow);
		xdb_stg_free (&pTblm->stg_mgr, rid, pRow);
		if (xdb_unlikely (XDB_ROW_TRANS == ctrl)) {
			// del from new row list
			xdb_trans_delrow (pConn, pTblm, rid);
		}
	} else {
		// add to dlt row list
		xdb_trans_addrow (pConn, pTblm, rid, false);
	}

	return 0;
}

XDB_STATIC xdb_rowid 
xdb_sql_insert (xdb_stmt_insert_t *pStmt)
{
	//xdb_print ("ok insert tbl '%s' count %d\n", pStmt->XDB_OBJ_NAME(pTblm), pStmt->row_count);
	xdb_rowid 		count = 0;
	xdb_conn_t		*pConn = pStmt->pConn;
	xdb_tblm_t		*pTblm = pStmt->pTblm;

	xdb_wrlock_table (pConn, pTblm);

	xdb_wrlock_tblstg (pTblm);

	for (int i = 0 ; i < pStmt->row_count; ++i) {
		void *pRow = pStmt->pRowsBuf + pStmt->row_offset[i];
		xdb_rowid rid = xdb_row_insert (pConn, pTblm, pRow);
		if (rid > 0) {
			count ++;
		}
	}

	xdb_wrunlock_tblstg (pTblm);

	return count;
}

XDB_STATIC int 
xdb_row_update (xdb_conn_t *pConn, xdb_tblm_t *pTblm, xdb_rowid rid, void *pRow, xdb_setfld_t set_flds[], int set_count)
{
	if ((pTblm->bMemory && pConn->bAutoTrans) || (XDB_ROW_TRANS == (XDB_ROW_CTRL (pTblm->stg_mgr.pStgHdr, pRow) & XDB_ROW_MASK))) {
		uint64_t idx_bmp = 0;
		for (int i = 0; i < set_count; ++i) {
			xdb_field_t 	*pField = set_flds[i].pField;
			idx_bmp |= pField->idx_bmp;
		}
		uint8_t idx_del[XDB_MAX_INDEX];
		int idx_count = xdb_idx_remRow_bmp (pTblm, rid, pRow, idx_bmp, idx_del);
		xdb_row_copy (pRow, set_flds, set_count);
		xdb_idx_addRow_bmp (pConn, pTblm, rid, pRow, idx_del, idx_count);
	} else {
		XDB_BUF_DEF(pUpdRow, 4096);
		XDB_BUF_ALLOC(pUpdRow, pTblm->pMeta->row_size);
		memcpy (pUpdRow, pRow, pTblm->pMeta->row_size);
		XDB_EXPECT (pUpdRow != NULL, XDB_E_MEMORY, "Can't alloc memory");
		xdb_row_copy (pUpdRow, set_flds, set_count);
		xdb_row_delete (pConn, pTblm, rid, pRow);
		xdb_row_insert (pConn, pTblm, pUpdRow);	
		XDB_BUF_FREE(pUpdRow);
	}

	return 0;

error:
	return -XDB_CONNCODE(pConn);
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

	xdb_wrlock_table (pConn, pTblm);

	xdb_wrlock_tblstg (pTblm);

	xdb_rowset_t	*pRowSet = &pConn->row_set;
	xdb_sql_filter (pStmt);
	pRes->affected_rows = pRowSet->count;

	for (xdb_rowid id = 0 ; id < pRowSet->count; ++id) {
		xdb_rowptr_t *pRow = &pRowSet->pRowList[id];
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

	xdb_wrlock_table (pConn, pTblm);

	xdb_wrlock_tblstg (pTblm);

	xdb_rowset_t	*pRowSet = &pConn->row_set;
	xdb_sql_filter (pStmt);
	pRes->affected_rows = pRowSet->count;

	for (xdb_rowid id = 0 ; id < pRowSet->count; ++id) {
		xdb_rowptr_t *pRow = &pRowSet->pRowList[id];
		int rc = xdb_row_delete (pConn, pTblm, pRow->rid, pRow->ptr);
		if (0 == rc) {
			count ++;
		}
	}

	xdb_rowset_clean (pRowSet);

	xdb_wrunlock_tblstg (pTblm);

	return count;
}
