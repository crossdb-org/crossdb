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

#ifndef __XDB_RBTREE_H__
#define __XDB_RBTREE_H__

typedef struct {
	xdb_rowid				rb_left;
	xdb_rowid				rb_right;
	xdb_rowid				rb_parent;	// in main is 0, in sib msb=1 XDB_RB_SIB_NODE 
	xdb_rowid				color_sibling;	// in main list, it points to first, for sibing first, prev = 0, sibing = parent node
} xdb_rbnode_t;

typedef struct {
	xdb_stghdr_t			blk_hdr;
	uint64_t				query_times;
	xdb_rowid          		row_count;
	xdb_rowid 				node_count;
	xdb_rowid          		rb_root;
	xdb_rowid				rsvd[3]; //
	xdb_rbnode_t			rb_node[1]; // 0 is rsvd for NULL node
} xdb_rbtree_t;

#endif // __XDB_RBTREE_H__
