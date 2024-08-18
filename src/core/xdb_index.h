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

#ifndef __XDB_INDX_H__
#define __XDB_INDX_H__

typedef struct {
	int (*idx_add) (xdb_conn_t *pConn, struct xdb_idxm_t* pIdxm, xdb_rowid new_rid, void *pRow);
	int (*idx_rem) (struct xdb_idxm_t* pIdxm, xdb_rowid rid, void *pRow);
	xdb_rowid (*idx_query) (xdb_conn_t *pConn, struct xdb_idxm_t* pIdxm, xdb_value_t **ppValue, xdb_filter_t **ppFilter, int count, xdb_rowset_t *pRowSet);
	int (*idx_create) (struct xdb_idxm_t *pIdxm);
	int (*idx_close) (struct xdb_idxm_t* pIdxm);
	int (*idx_drop) (struct xdb_idxm_t* pIdxm);
} xdb_idx_ops;

typedef struct xdb_idxm_t {
	xdb_obj_t		obj;
	struct xdb_tblm_t *pTblm;
	xdb_idx_type	idx_type;
	bool			bUnique;
	int				fld_count;
	xdb_field_t		*pFields[XDB_MAX_MATCH_COL];
	uint32_t		slot_mask;
	xdb_hashHdr_t	*pHashHdr;	
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
xdb_idx_remRow (xdb_tblm_t *pTblm, xdb_rowid rid, void *pRow);

XDB_STATIC xdb_idxm_t* 
xdb_find_index (xdb_tblm_t *pTblm, const char *idx_name);

#endif // __XDB_INDX_H__