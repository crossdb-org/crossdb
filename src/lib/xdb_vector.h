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

#ifndef __XDB_VECTOR_H__
#define __XDB_VECTOR_H__

typedef struct {
	union {
		void		**pEle;
		uint8_t		*pBuf;
	};
	int			cap;
	int			count;
} xdb_vec_t;

static inline bool 
xdb_vec_add (xdb_vec_t *pVec, void *pE)
{
	if (xdb_unlikely (pVec->count >= pVec->cap)) {
		void *pEle = xdb_realloc (pVec->pEle, pVec->cap + 64*sizeof(void*));
		if (NULL == pEle) {
			return false;
		}
		pVec->pEle = pEle;
		pVec->cap += 64;
	}
	pVec->pEle[pVec->count++] = pE;
	return true;
}

static inline bool 
xdb_vec_addStr (xdb_vec_t *pVec, const void *pPtr, int len)
{
	if (xdb_unlikely (pVec->count + len >= pVec->cap)) {
		int new_len = XDB_ALIGN1M(pVec->count + len + 1);
		void *pBuf = xdb_realloc (pVec->pEle, new_len);
		if (NULL == pBuf) {
			return false;
		}
		pVec->pBuf = pBuf;
		pVec->cap = new_len;
	}
	memcpy (&pVec->pBuf[pVec->count], pPtr, len);
	pVec->count += len;
	pVec->pBuf[pVec->count] = '\0';
	return true;
}

static inline bool 
xdb_vec_del (xdb_vec_t *pVec, void *pE)
{
	// TBD
	return true;
}

static inline void 
xdb_vec_free (xdb_vec_t *pVec)
{
	xdb_free (pVec->pEle);
	memset (pVec, 0, sizeof (*pVec));
}

#endif // __XDB_VECTOR_H__
