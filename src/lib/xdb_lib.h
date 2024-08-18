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

#ifndef __XDB_LIB_H__
#define __XDB_LIB_H__

#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <pthread.h>
#include <stdint.h>
#include <stdbool.h>
#include <inttypes.h>
#include <math.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <stdarg.h>
#include <time.h>
#include <sys/time.h>
#include <signal.h>
#include <dirent.h>
#include <fcntl.h>
#include <assert.h>
#include <errno.h>

#ifdef _WIN32
	#include <windows.h>
#endif

#if defined(__BIG_ENDIAN__) || (defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
  #define XDB_BIG_ENDIAN 1
#endif

#define XDB_ARY_LEN(a)			(sizeof(a)/sizeof(a[0]))
#define XDB_OFFSET(st,fld)		offsetof(st,fld)

#ifdef XDB_DEBUG
#define xdb_assert(exp)			assert(exp)
#else
#define xdb_assert(exp)
#endif

#define xdb_likely(x)			__builtin_expect(!!(x), 1)
#define xdb_unlikely(x)			__builtin_expect(!!(x), 0)

#define xdb_prefetch(addr)		__builtin_prefetch (addr, 0, 3)

// Log
#ifdef XDB_DEBUG
#define xdb_dbglog(args...)		printf("[XDB Debug] "args)
#else
#define xdb_dbglog(args...)
#endif

#define xdb_errlog(args...)		fprintf (stderr, "[XDB Error] "args)

#define xdb_print(args...)		printf (args)

// Memory
#define xdb_malloc				malloc
#define xdb_calloc(size)		calloc(1,size)
#define xdb_free(ptr)			do { if(ptr) { free(ptr); ptr=NULL; } } while (0)
#define xdb_realloc				realloc

// Byte swap
#define xdb_bswap64(val) 		__builtin_bswap64(val)
#define xdb_bswap32(val) 		__builtin_bswap32(val)
#define xdb_bswap16(val) 		__builtin_bswap16(val)

// Atomic
#define xdb_atomic_read(ptr,val) 	__atomic_load(ptr, val, __ATOMIC_SEQ_CST)
#define xdb_atomic_inc(ptr) 		__sync_add_and_fetch(ptr, 1)
#define xdb_atomic_dec(ptr) 		__sync_sub_and_fetch(ptr, 1)
#define xdb_atomic_add(ptr, val) 	__sync_add_and_fetch(ptr, val)
#define xdb_atomic_sub(ptr, val) 	__sync_sub_and_fetch(ptr, val)

// Types
typedef int64_t					xdb_ssize;
typedef uint64_t				xdb_size;

#ifndef _WIN32
	typedef int	 				xdb_fd;
	#define XDB_INV_FD			-1
	#define XDB_FDFMT			"d"
#else
	typedef HANDLE  			xdb_fd;
	#define XDB_INV_FD			INVALID_HANDLE_VALUE
	#define WIN_INVALID_HANDLE(handle) ((INVALID_HANDLE_VALUE==handle) || (NULL==handle))
	#define XDB_FDFMT			"p"
#endif

#ifndef XDB_ENABLE_SERVER
#define xdb_sock_close(fd)
#endif

#include "../3rd/wyhash.h"
static inline uint64_t 
xdb_wyhash(const void *key, int len)
{
	static const uint64_t secret[] = {0xa0761d6478bd642fLL,0xe7037ed1a0b428dbLL,0x8ebc6af09c88c6e3LL,0x589965cc75374cc3LL};
	return wyhash (key, len, 0x9E3779B97F4A7C16LL, secret);
}

XDB_STATIC uint64_t 
xdb_strcasehash(const char *key, int len);

//XDB_STATIC void xdb_hexdump (const void *addr, int len);

#ifdef XDB_ENABLE_SERVER
XDB_STATIC int 
xdb_signal_block (int signum);
#endif

#include "xdb_objm.h"
#include "xdb_bmp.h"

#endif // __XDB_LIB_H__