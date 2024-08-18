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