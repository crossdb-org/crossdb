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

#include "xdb_dynbuf.h"

static inline void 
xdb_dynbuf_init (xdb_dynbuf_t *pDynBuf)
{
	pDynBuf->pBuf 	= pDynBuf->buf;
	pDynBuf->pCur	= pDynBuf->buf;
	pDynBuf->cap 	= sizeof(pDynBuf->buf);
	pDynBuf->free	= sizeof(pDynBuf->buf);
}

static inline void* 
xdb_dynbuf_alloc (xdb_dynbuf_t *pDynBuf, uint32_t size)
{
	if (pDynBuf->free < size) {
		void *ptr = xdb_dynbuf_resize (pDynBuf, pDynBuf->cap + size);
		if (NULL == ptr) {
			return NULL;
		}
	}
	void *ptr = pDynBuf->pCur;
	pDynBuf->free -= size;
	return ptr;
}

static inline void 
xdb_dynbuf_free (xdb_dynbuf_t *pDynBuf)
{
	if (pDynBuf->pBuf != pDynBuf->buf) {
		xdb_free (pDynBuf->pBuf);
		xdb_dynbuf_init (pDynBuf);
	}
}

XDB_STATIC void* 
xdb_dynbuf_resize (xdb_dynbuf_t *pDynBuf, uint64_t size)
{
	if (0 == pDynBuf->cap) {
		xdb_dynbuf_init (pDynBuf);
	}
	if (size <= pDynBuf->cap) {
		return pDynBuf->pBuf;
	}
	int new_cap = pDynBuf->cap;
	do {
		new_cap <<= 1;
	} while (new_cap < size);

	void *pBuf;
	if (pDynBuf->pBuf == pDynBuf->buf) {
		pBuf = xdb_malloc (size);
		if (NULL == pBuf) {
			return NULL;
		}
		memcpy (pBuf, pDynBuf->buf, sizeof(pDynBuf->buf));
	} else {
		pBuf = xdb_realloc (pDynBuf->pBuf, pDynBuf->cap);
		if (NULL == pBuf) {
			return NULL;
		}
	}

	pDynBuf->pCur = pBuf + (pDynBuf->pCur - pDynBuf->pBuf);
	pDynBuf->pBuf = pBuf;
	pDynBuf->free += (new_cap - pDynBuf->cap);
	pDynBuf->cap = new_cap;

	return pBuf;
}
