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
