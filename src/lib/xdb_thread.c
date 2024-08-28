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

#define xdb_yield() 		sched_yield()

typedef struct {
	volatile int32_t count; // -1 when W lock held, > 0 when R locks held.
} xdb_rwlock_t;

#define XDB_RWLOCK_INIT(lock) lock.count = 0

static inline void
xdb_rwlock_init(xdb_rwlock_t *rwl)
{
	rwl->count = 0;
}

static inline void
xdb_rwlock_rdlock(xdb_rwlock_t *rwl)
{
	int32_t x;
	int success = 0;

	while (success == 0) {
		x = __atomic_load_n(&rwl->count, __ATOMIC_RELAXED);
		// write lock is held
		if (xdb_unlikely (x < 0)) {
			xdb_yield ();
			continue;
		}
		success = __atomic_compare_exchange_n(&rwl->count, &x, x + 1, 1,
					__ATOMIC_ACQUIRE, __ATOMIC_RELAXED);
	}
}

static inline void
xdb_rwlock_wrlock(xdb_rwlock_t *rwl)
{
	int32_t x;
	int success = 0;

	while (success == 0) {
		x = __atomic_load_n(&rwl->count, __ATOMIC_RELAXED);
		// a lock is held
		if (xdb_unlikely (x != 0)) {
			xdb_yield ();
			continue;
		}
		success = __atomic_compare_exchange_n(&rwl->count, &x, -1, 1,
					__ATOMIC_ACQUIRE, __ATOMIC_RELAXED);
	}
}

static inline void
xdb_rwlock_rdunlock(xdb_rwlock_t *rwl)
{
	__atomic_fetch_sub(&rwl->count, 1, __ATOMIC_RELEASE);
}

static inline void
xdb_rwlock_wrunlock(xdb_rwlock_t *rwl)
{
	__atomic_store_n(&rwl->count, 0, __ATOMIC_RELEASE);
}

#if 0
#ifndef _WIN32
#include <unistd.h>
#include <pthread.h>
#else
#include <windows.h>
typedef DWORD pthread_t;
static inline int 
pthread_create(pthread_t *thread, const void *attr, void *(*start_routine) (void *), void *arg)
{
	(void) attr;
	CreateThread (0, 0, (LPTHREAD_START_ROUTINE)start_routine, arg, 0, thread);
	return 0;
}
#endif
#endif

typedef pthread_t xdb_thread_t;

static inline int 
xdb_create_thread (xdb_thread_t *pThread, const void *pAttr, void *(*start_routine) (void *), void *pArg)
{
	return pthread_create (pThread, pAttr, start_routine, pArg);
}
