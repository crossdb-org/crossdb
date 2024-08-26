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

#include "xdb_lib.h"
#include "xdb_str.c"
#include "xdb_file.c"
#include "xdb_mmap.c"
#include "xdb_objm.c"
#include "xdb_bmp.c"
#include "xdb_time.c"
#include "xdb_dynbuf.c"
#include "xdb_thread.c"
#if (XDB_ENABLE_SERVER == 1)
#include "xdb_sock.c"
#endif

#if 0
XDB_STATIC void 
xdb_hexdump (const void *addr, int len)
{
	int i, j;
	const uint8_t *ptr = addr;
	xdb_print ("hexdump addr %p len %d\n", addr, len);
	for (i = 0; i < len; i+=16, ptr+=16) {
		xdb_print ("%08x  ", i);
		for (j = 0; j < 16; ++j) {
			if (i + j < len) {
				xdb_print ("%02x ", ptr[j]);
			} else {
				xdb_print ("   ");
			}
			if (j == 7) xdb_print (" ");
		}
		xdb_print (" |");
		for (j = 0; j < 16; ++j) {
			xdb_print ("%c", (i+j>=len)?' ':(isprint(ptr[j])?ptr[j]:'.'));
			if (j == 7) xdb_print (" ");
		}
		xdb_print ("|\n");
	}
}
#endif

/*
	sigaction(SIGPIPE, &(struct sigaction){{SIG_IGN}}, NULL)
	signal(SIGPIPE,SIG_IGN);
*/
#if (XDB_ENABLE_SERVER == 1)
XDB_STATIC int 
xdb_signal_block (int signum)
{
#ifndef _WIN32
	sigset_t signal_mask; 
	sigemptyset(&signal_mask); 
	sigaddset(&signal_mask, signum); 
	if (pthread_sigmask(SIG_BLOCK, &signal_mask, NULL) == -1) {
		perror ("SIGPIPE");
		return -1;
	}
#endif
	return 0;
}
#endif

XDB_STATIC uint64_t 
xdb_strcasehash(const char *key, int len)
{
	XDB_BUF_DEF(str, 2048);
	if (len > 2048) {
		XDB_BUF_ALLOC (str, len);
		if (NULL == str) {
			return -1;
		}
	}
	for (int i = 0; i< len; ++i) {
		str[i] = tolower(key[i]);
	}
	
	uint64_t hash = xdb_wyhash (str, len);

	XDB_BUF_FREE(str);

	return hash;
}
