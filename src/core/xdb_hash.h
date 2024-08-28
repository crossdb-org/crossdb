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

#ifndef __XDB_HASH_H__
#define __XDB_HASH_H__

typedef struct xdb_hashNode_t {
	xdb_rowid				next;
	xdb_rowid				prev;
	xdb_rowid				sibling;
	uint32_t 				hash_val;
} xdb_hashNode_t;

typedef struct xdb_hashHdr_t {
	xdb_stghdr_t			blk_hdr;
	uint64_t				query_times;
	xdb_rowid 				row_count;
	xdb_rowid 				node_count;
	xdb_rowid 				slot_count;
	xdb_rowid				old_count; // if !=0 in rehashing
	xdb_rowid				rehash_count; // rehash slot location	
	uint32_t				slot_mask;
	uint32_t				rsvd[4];
	xdb_hashNode_t			hash_node[];
} xdb_hashHdr_t;

typedef struct xdb_hashSlot_t {
	xdb_stghdr_t			blk_hdr;
	xdb_rowid				hash_slot[];
} xdb_hashSlot_t;

#endif // __XDB_HASH_H__
