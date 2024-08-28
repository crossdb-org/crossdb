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

#ifndef __XDB_DYNBUF_H__
#define __XDB_DYNBUF_H__

#define XDB_BUF_DEF(ptr,size)		char	ptr##_buf[size], *ptr=ptr##_buf; int ptr##_size=sizeof(ptr##_buf)
#define XDB_BUF_ALLOC(ptr,size)	\
	if(size > ptr##_size) { \
		if (ptr==ptr##_buf) { ptr = NULL;}	\
		ptr = xdb_realloc(ptr, size);	\
		if (NULL != ptr) {	ptr##_size = size;	}	\
	}
#define XDB_BUF_FREE(ptr)	\
	if ((NULL!=ptr) && (ptr!=ptr##_buf)) { \
		free(ptr); \
		ptr = ptr##_buf; 	\
		ptr##_size = sizeof(ptr##_buf); \
	}

typedef struct {
	void		*pBuf;
	void		*pCur;
	uint64_t	cap;
	uint64_t	free;
	uint8_t		buf[2048];
} xdb_dynbuf_t;

XDB_STATIC void* 
xdb_dynbuf_resize (xdb_dynbuf_t *pDynBuf, uint64_t size);

#endif // __XDB_DYNBUF_H__
