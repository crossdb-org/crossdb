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

#ifndef __XDB_LIB_H__
#define __XDB_LIB_H__

#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
//#ifndef _WIN32
#include <pthread.h>
//#endif
#include <stdint.h>
#include <stdbool.h>
#include <inttypes.h>
#include <math.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/stat.h>
//#ifndef _WIN32
#include <unistd.h>
//#endif
#include <string.h>
#include <stdarg.h>
#include <time.h>
//#ifndef _WIN32
#include <sys/time.h>
//#endif
#include <signal.h>
//#ifndef _WIN32
#include <dirent.h>
//#endif
#include <fcntl.h>
#include <assert.h>
#include <errno.h>

#ifdef _WIN32
#include <ws2tcpip.h>
#include <windows.h>
#define localtime_r(timep, result)	localtime_s(result, timep)
typedef unsigned int in_addr_t;
#endif

#if defined(__BIG_ENDIAN__) || (defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
  #define XDB_BIG_ENDIAN		1
#endif

#define XDB_ARY_LEN(a)			(sizeof(a)/sizeof(a[0]))
#define XDB_OFFSET(st,fld)		offsetof(st,fld)

#define XDB_ALIGN2(len)			((len + 2) & ~1)
#define XDB_ALIGN4(len)			((len + 3) & ~3)
#define XDB_ALIGN8(len)			((len + 7) & ~7)
#define XDB_ALIGN4K(len)		((len + 4093) & ~4093)
#define XDB_ALIGN1M(len)		((len + (1024*1024-1)) & ~(1024*1024-1))

#ifdef XDB_DEBUG
#define xdb_assert(exp)			assert(exp)
#else
#define xdb_assert(exp)
#endif

#define xdb_prefetch(addr)		__builtin_prefetch (addr, 0, 3)

// Log
#ifdef XDB_DEBUG
#define xdb_dbglog(...)			printf("[XDB Debug] " __VA_ARGS__)
#else
#define xdb_dbglog(...)
#endif

#define xdb_errlog(...)			fprintf (stderr, "[XDB Error] " __VA_ARGS__)

#define xdb_print(...)			printf (__VA_ARGS__)

#define xdb_dbgprint(...)		printf (__VA_ARGS__)


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

#if (XDB_ENABLE_SERVER==0)
#define xdb_sock_close(fd)
#endif

#include "../3rd/wyhash.h"
static inline uint64_t 
xdb_wyhash(const void *key, size_t len)
{
	static const uint64_t secret[] = {0xa0761d6478bd642fLL,0xe7037ed1a0b428dbLL,0x8ebc6af09c88c6e3LL,0x589965cc75374cc3LL};
	return wyhash (key, len, 0x9E3779B97F4A7C16LL, secret);
}

XDB_STATIC uint64_t 
xdb_strcasehash(const char *key, int len);

void xdb_hexdump (const void *addr, int len);

#ifndef _WIN32
#if (XDB_ENABLE_SERVER == 1)
XDB_STATIC int 
xdb_signal_block (int signum);
#endif
#endif

static int xdb_fprintf (FILE *pFile, const char *format, ...);
static int xdb_fflush (FILE *pFile);
#define xdb_fputc(c, pFile)	xdb_fprintf(pFile, "%c", c)


typedef struct {
	volatile int32_t count; // -1 when W lock held, > 0 when R locks held.
} xdb_rwlock_t;

#define XDB_RWLOCK_INIT(lock) lock.count = 0


#include "xdb_objm.h"
#include "xdb_bmp.h"

#endif // __XDB_LIB_H__
