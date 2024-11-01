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

#ifndef _WIN32
#include <sys/mman.h>
#endif

#if defined (_WIN32) || defined(__CYGWIN__) || defined(__MSYS__)
#define XDB_PAGE_BITS	16
#else
#define XDB_PAGE_BITS	12
#endif

#define XDB_PAGE_SIZE	(1<<XDB_PAGE_BITS)
#define XDB_PAGE_MASK	(XDB_PAGE_SIZE - 1)


/******************************************************************************
	XDB MMAP for Linux
******************************************************************************/

#ifndef _WIN32

XDB_STATIC void* 
xdb_mmap (xdb_fd fd, uint64_t offset, uint64_t length, void *addr)
{
	// ulimit -l size to change, only root can set it, default is only 64K
	// MAP_LOCKED can avoid page to be swapped out
	addr = mmap (addr, (size_t)length, PROT_READ|PROT_WRITE, MAP_SHARED, fd, offset);
	if ((NULL == addr) || (MAP_FAILED == addr)) {
		xdb_errlog ("Can't mmap file %d length %"PRIu64" offset %"PRIu64" -> err %d %s\n", fd, length, offset, errno, strerror(errno));
		addr = NULL;
	}

	return addr;
}

XDB_STATIC int 
xdb_munmap (void *addr, uint64_t length)
{
	int err = munmap (addr, (size_t)length);
	if (err < 0) {
		xdb_errlog ("Can't unmap addr %p length %"PRIu64" -> err %d %s\n", addr, length, err, strerror(errno));
	}
	return err;
}

XDB_STATIC void* 
xdb_remmap (xdb_fd fd, void *addr, uint64_t length, uint64_t new_len)
{
#if defined __CYGWIN__ || defined __MSYS__ || defined __APPLE__ || defined __FreeBSD__

	if (NULL != addr) {
		munmap (addr, (size_t)length);
	}

	int err = xdb_ftruncate (fd, new_len);
	if (err < 0) {
#if ! ( defined (__CYGWIN__) || defined (__MSYS__))
		// may report Permission denied 
		perror ("truncate");
#endif
	}

	if (NULL == addr) {
		return xdb_mmap (fd, 0, new_len, NULL);
	}

	addr = mmap (addr, (size_t)new_len, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
	if ((NULL == addr) || (MAP_FAILED == addr)) {
		xdb_errlog ("Can't remap addr %p length %"PRIu64" to %"PRIu64" -> err %d %s\n", addr, length, new_len, errno, strerror(errno));
		// try to recover to old len
		addr = mmap (addr, (size_t)length, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
		addr = NULL;
	}

#else

	if (new_len > length) {
		// Grow
		int err = xdb_ftruncate (fd, new_len);
		if (err < 0) {
			return NULL;
		}
	}

	if (NULL == addr) {
		if (new_len < length) {
			// Shrink
			(void) xdb_ftruncate (fd, new_len);
		}
		return xdb_mmap (fd, 0, new_len, NULL);
	}

	addr = mremap (addr, length, new_len, MREMAP_MAYMOVE);
	if ((NULL == addr) || (MAP_FAILED == addr)) {
		xdb_errlog ("Can't remap addr %p length %"PRIu64" to %"PRIu64" -> err %d %s\n", addr, length, new_len, errno, strerror(errno));
		addr = NULL;
	}

	if (new_len < length) {
		// Shrink
		int err = xdb_ftruncate (fd, new_len);
		if (err < 0) {
			xdb_errlog ("truncate shrink");
		}
	}

#endif

	return addr;
}

XDB_STATIC int 
xdb_msync (void *addr, uint64_t length, bool bAsync)
{
	length += ((uintptr_t)addr&XDB_PAGE_MASK);
	addr = (void*)((uintptr_t)addr & (~XDB_PAGE_MASK));
	int err = msync (addr, (size_t)length, bAsync?MS_ASYNC:MS_SYNC);
	if (err < 0) {
		xdb_errlog ("Can't msync addr %p length %"PRIu64" -> err %d %s\n", addr, length, err, strerror(errno));
	}
	return err;
}

#endif // Linux


/******************************************************************************
	XDB MMAP for Windows
******************************************************************************/

#ifdef _WIN32

XDB_STATIC void* 
xdb_mmap (xdb_fd fd, uint64_t offset, uint64_t length, void *addr)
{
	HANDLE hMapObj;

	hMapObj = CreateFileMapping (fd, NULL, PAGE_READWRITE, 0, 0, NULL);
	if (WIN_INVALID_HANDLE (hMapObj)) {
		xdb_errlog ("CreateFileMapping fail rc %d\n", (int)GetLastError());
		return NULL;
	}

	// The last arg is addr, if can't map at this addr, will return fail, so don't set
	addr = MapViewOfFileEx (hMapObj, FILE_MAP_ALL_ACCESS, 
							offset>>32, offset&0xFFFFFFFF, (SIZE_T)length, NULL);
	if (NULL == addr) {
		xdb_errlog ("Can't map file %p length %"PRIu64" offset %"PRIu64" rc %d\n", fd, length, offset, (int)GetLastError());
	}

	CloseHandle (hMapObj);
	return addr;
}

XDB_STATIC int 
xdb_munmap (void *addr, uint64_t length)
{
	BOOL err = UnmapViewOfFile (addr);
	if (! err) {
		xdb_errlog ("Can't UnmapViewOfFile addr %p length %"PRIu64" rc %d\n", addr, length, (int)GetLastError());
	}
	return err ? 0 : -1;
}

XDB_STATIC void* 
xdb_remmap (xdb_fd fd, void *addr, uint64_t length, uint64_t new_len)
{
	HANDLE hMapObj;

	if (NULL != addr) {
		BOOL err = UnmapViewOfFile (addr);
		if (! err) {
			xdb_errlog ("Can't UnmapViewOfFile addr %p length %"PRIu64" rc %d\n", addr, length, (int)GetLastError());
			return NULL;
		}
	}

	// truncate may fail on Windows if previous shrink failed, so still try to remap
	int err = xdb_ftruncate (fd, new_len);
	if (err < 0) {
		return NULL;
	}

	if (NULL == addr) {
		return xdb_mmap (fd, 0, new_len, NULL);
	}

	hMapObj = CreateFileMapping (fd, NULL, PAGE_READWRITE, 0, 0, NULL);
	if (WIN_INVALID_HANDLE (hMapObj)) {
		xdb_errlog ("CreateFileMapping fail rc %d\n", (int)GetLastError());
		return NULL;
	}

	// The last arg is addr, if can't map at this addr, will return fail, so don't set
	addr = MapViewOfFileEx (hMapObj, FILE_MAP_ALL_ACCESS, 0, 0, (SIZE_T)new_len, NULL);
	if (NULL == addr) {
		xdb_errlog ("Can't remap file %p to new length %"PRIu64" rc %d\n", fd, new_len, (int)GetLastError());
		// Try to remap to old size
		addr = MapViewOfFileEx (hMapObj, FILE_MAP_ALL_ACCESS, 0, 0, (SIZE_T)length, NULL);
		addr = NULL;
	}

	CloseHandle (hMapObj);
	return addr;
}

XDB_STATIC int 
xdb_msync (void *addr, uint64_t length, bool bAsync)
{
	if (bAsync) {
		return 0;
	} else {
		length += ((uintptr_t)addr&XDB_PAGE_MASK);
		addr = (void*)((uintptr_t)addr & (~XDB_PAGE_MASK));

		BOOL err = FlushViewOfFile (addr, (SIZE_T)length);
		if (! err) {
			xdb_errlog ("Can't msync addr %p length %"PRIu64" -> rc %d \n", addr, length, (int)GetLastError());
		}
		return err ? 0 : -1;
	}
}

#endif // Windows

#ifdef XDB_MOD_TEST
int main (int argc, char *argv[])
{
	return 0;
}
#endif
