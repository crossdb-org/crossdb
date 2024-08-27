/******************************************************************************
* Copyright (c) 2024-present JC Wang. All rights reserved
*
*   https://crossdb.org
*   https://github.com/crossdb-org/crossdb
*
* This program is free software: you can use, redistribute, and/or modify it under
* the terms of the GNU Lesser General Public License, version 3 or later ("LGPL"),
* as published by the Free Software Foundation.
*
* This program is distributed in the hope that it will be useful, but WITHOUT
* ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS 
* FOR A PARTICULAR PURPOSE. See the GNU published General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public License along with 
* this program. If not, see <http://www.gnu.org/licenses/>.
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
