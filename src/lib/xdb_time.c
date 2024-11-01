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

static inline uint64_t 
xdb_timestamp ()
{
    struct timeval time;
    gettimeofday(&time,NULL);
	return time.tv_sec;
}

static inline uint64_t 
xdb_timestamp_ms ()
{
    struct timeval time;
    gettimeofday(&time,NULL);
	return time.tv_sec * 1000LL + time.tv_usec/1000;
}

static inline uint64_t 
xdb_timestamp_us ()
{
    struct timeval time;
    gettimeofday(&time,NULL);
	return time.tv_sec * 1000000LL + time.tv_usec;
}

static inline uint64_t 
xdb_uptime ()
{
    struct timespec uptime;
    clock_gettime(CLOCK_MONOTONIC, &uptime);
	return uptime.tv_sec;
}

