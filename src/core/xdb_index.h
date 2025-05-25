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

#ifndef __XDB_INDEX_H__
#define __XDB_INDEX_H__

typedef struct {
	int (*idx_add) (xdb_conn_t *pConn, struct xdb_idxm_t* pIdxm, xdb_rowid new_rid, void *pRow);
	int (*idx_rem) (struct xdb_idxm_t* pIdxm, xdb_rowid rid, void *pRow);
	xdb_rowid (*idx_query) (xdb_conn_t *pConn, xdb_idxfilter_t *pIdxFilter, xdb_rowset_t *pRowSet);
	xdb_rowid (*idx_query2) (xdb_conn_t *pConn, struct xdb_idxm_t* pIdxm, void *pRow);
	int (*idx_create) (struct xdb_idxm_t *pIdxm);
	int (*idx_close) (struct xdb_idxm_t* pIdxm);
	int (*idx_drop) (struct xdb_idxm_t* pIdxm);
	int (*idx_sync) (struct xdb_idxm_t* pIdxm);
	int (*idx_init) (struct xdb_idxm_t* pIdxm);
} xdb_idx_ops;

typedef struct xdb_idxm_t {
	xdb_obj_t		obj;
	struct xdb_tblm_t *pTblm;
	xdb_idx_type	idx_type;
	bool			bPrimary;
	bool			bUnique;
	int				fld_count;
	xdb_field_t		*pFields[XDB_MAX_MATCH_COL];
	char			*pExtract[XDB_MAX_MATCH_COL];
	uint32_t		slot_mask;
	xdb_hashHdr_t	*pHashHdr;	
	xdb_rbtree_t	*pRbtrHdr;
	xdb_rowid		*pHashSlot;
	xdb_rowid		slot_cap;
	xdb_rowid		node_cap;
	xdb_hashNode_t	*pHashNode;
	xdb_stgmgr_t	stg_mgr;
	xdb_stgmgr_t	stg_mgr2;
	xdb_idx_ops		*pIdxOps;
} xdb_idxm_t;

XDB_STATIC int 
xdb_idx_addRow (xdb_conn_t *pConn, xdb_tblm_t *pTblm, xdb_rowid rid, void *pRow);

XDB_STATIC int 
xdb_idx_addRow_bmp (xdb_conn_t *pConn, xdb_tblm_t *pTblm, xdb_rowid rid, void *pRow, uint8_t *idx_del, int count);

XDB_STATIC int 
xdb_idx_remRow (xdb_tblm_t *pTblm, xdb_rowid rid, void *pRow);

XDB_STATIC int 
xdb_idx_remRow_bmp (xdb_tblm_t *pTblm, xdb_rowid rid, void *pRow, uint8_t *idx_del, int count);

XDB_STATIC xdb_idxm_t* 
xdb_find_index (xdb_tblm_t *pTblm, const char *idx_name);

XDB_STATIC const char* 
xdb_idx2str(xdb_idx_type tp);

#endif // __XDB_INDEX_H__
