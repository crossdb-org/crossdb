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

#if XDB_LOG_FLAGS & XDB_LOG_HASH
#define xdb_hashlog(...)	xdb_print(__VA_ARGS__)
#else
#define xdb_hashlog(...)
#endif

#if 0
XDB_STATIC void 
xdb_hash_dump (xdb_idxm_t* pIdxm)
{
	xdb_hashHdr_t	*pHashHdr  = pIdxm->pHashHdr;	
	xdb_rowid		*pHashSlot = pIdxm->pHashSlot;	
	xdb_hashNode_t	*pHashNode = pIdxm->pHashNode;
	xdb_stgmgr_t	*pStgMgr	= &pIdxm->pTblm->stg_mgr;

	for (xdb_rowid slot = 0; slot < pIdxm->slot_cap; ++slot) {
		xdb_rowid rid = pHashSlot[slot];
		if (0 == rid) {
			continue;
		}
		xdb_print ("slot:%d\n", slot);
		xdb_hashNode_t	*pCurNode;
		for (; rid > 0; rid = pCurNode->next) {
			pCurNode = &pHashNode[rid];
			xdb_print ("  %u: nxt %x pre %x sib %x\n", rid, pCurNode->next, pCurNode->prev, pCurNode->sibling);
			xdb_hashNode_t *pSibNode;
			for (xdb_rowid sid = pCurNode->sibling; sid > 0; sid = pSibNode->next) {
				pSibNode = &pHashNode[sid];
				xdb_print ("    %u: nxt %x pre %x sib %x\n", sid, pSibNode->next, pSibNode->prev, pSibNode->sibling);
				if (pSibNode->next == sid) {
					printf ("	 ERROR: next == sid\n", pSibNode->next, sid);
					return;
				}
			}
			if (pCurNode->next == rid) {
				xdb_print ("    ERROR: next == rid\n", pCurNode->next, rid);
				return;
			}
		}
	}

	return;
}
#endif

XDB_STATIC int 
xdb_hash_rehash (xdb_idxm_t *pIdxm, xdb_rowid old_max)
{
	xdb_hashHdr_t 		*pHashHdr = pIdxm->pHashHdr;
	xdb_rowid 			*pHashSlot = pIdxm->pHashSlot;
	xdb_hashNode_t		*pHashNode = pIdxm->pHashNode;
	xdb_rowid			rid, max_rid = pIdxm->node_cap, next_rid;
	xdb_rowid			new_slot, slot, slot_id, fisrt_rid;
	uint32_t			hash_mask = pIdxm->slot_mask;
	xdb_hashNode_t 		*pCurNode, *pNxtNode, *pPreNode;

	xdb_hashlog ("Rehash %s old_max %d new_max %d node %d rid %d Begin\n", XDB_OBJ_NAME(pIdxm), old_max, hash_mask, pIdxm->node_cap, pHashHdr->row_count);

	for (slot = 0; slot < old_max; ++slot) {
		rid = pHashSlot[slot];
		
		for (; XDB_ROWID_VALID (rid, max_rid); rid = next_rid) {
			pCurNode = &pHashNode[rid];
			next_rid = pCurNode->next;
			new_slot = pCurNode->hash_val & hash_mask;
			if (new_slot != slot) {
				xdb_hashlog ("  -- Move rid %d hashval 0x%x from %d to %d\n", rid,  pCurNode->hash_val, slot, new_slot);
				// remove from old slot
				if (pCurNode->next) {
					xdb_hashlog ("  rid %d has next %d, point to its prev %d\n", rid, pCurNode->next, pCurNode->prev & (~XDB_ROWID_MSB));
					pNxtNode = &pHashNode[pCurNode->next];
					pNxtNode->prev = pCurNode->prev;
				}
				if (pCurNode->prev & XDB_ROWID_MSB) {
					// is the first top rid
					slot_id = pCurNode->prev & XDB_ROWID_MASK;
					pHashSlot[slot_id] = pCurNode->next;
					if (0 == pCurNode->next) {
						pHashHdr->slot_count--;
					}
					xdb_hashlog ("  rid %d's next %d is 1st top rid for slot %d\n", rid, pCurNode->next, slot_id);
				} else if (pCurNode->prev) {
					xdb_hashlog ("  rid %d has prev %d, point to it's next %d\n", rid, pCurNode->prev, pCurNode->next);
					pPreNode = &pHashNode[pCurNode->prev];
					pPreNode->next = pCurNode->next;
		 		}

				// move to new_slot
				fisrt_rid = pHashSlot[new_slot];
				if (0 == fisrt_rid) {
					// first rid
					xdb_hashlog ("  insert first %d in slot %d \n", rid, new_slot);
					pHashSlot[new_slot] = rid;
					pCurNode->next = 0;
					pCurNode->prev = XDB_ROWID_MSB | new_slot;
					pHashHdr->slot_count++;
				} else {
					// insert head to current slot
					xdb_hashlog ("  Insert %d in slot %d at head %d\n", rid, new_slot, fisrt_rid);
					pHashSlot[new_slot] = rid;
					pCurNode->next = fisrt_rid;
					pCurNode->prev = XDB_ROWID_MSB | new_slot;
					pNxtNode = &pHashNode[fisrt_rid];
					pNxtNode->prev = rid;
				}
			}
		}
	}

	xdb_hashlog ("Rehash %s old_max %d new_max %d node %d rid %d End\n", XDB_OBJ_NAME(pIdxm), old_max, hash_mask, pIdxm->node_cap, pHashHdr->row_count);

	return 0;
}

XDB_STATIC int 
xdb_hash_add (xdb_conn_t *pConn, xdb_idxm_t* pIdxm, xdb_rowid new_rid, void *pRow)
{
	xdb_stgmgr_t	*pStgMgr	= &pIdxm->pTblm->stg_mgr;

	if (new_rid > pIdxm->node_cap) {
		xdb_stg_truncate (&pIdxm->stg_mgr, pIdxm->node_cap<<1);
		pIdxm->node_cap = XDB_STG_CAP(&pIdxm->stg_mgr);
		pIdxm->pHashHdr  = (xdb_hashHdr_t*)pIdxm->stg_mgr.pStgHdr;
		pIdxm->pHashNode = pIdxm->stg_mgr.pBlkDat1;
	}

	if ((pIdxm->slot_cap>>1) < pIdxm->pHashHdr->slot_count) {
		xdb_rowid old_cap = pIdxm->slot_cap;
		xdb_dbglog ("rehash slot_cap %d slot_count %d\n", pIdxm->slot_cap, pIdxm->pHashHdr->slot_count);
		xdb_stg_truncate (&pIdxm->stg_mgr2, pIdxm->slot_cap<<1);
		pIdxm->slot_cap		= XDB_STG_CAP(&pIdxm->stg_mgr2);
		pIdxm->slot_mask	= pIdxm->slot_cap - 1;
		pIdxm->pHashSlot	= pIdxm->stg_mgr2.pBlkDat;
		xdb_hash_rehash (pIdxm, old_cap);
#if 0
	if (xdb_hash_check (pConn, pIdxm, 0) < 0) { exit(-1); }
#endif
	}

	uint32_t hash_val = xdb_row_hash (pIdxm->pTblm, pRow, pIdxm->pFields, pIdxm->pExtract, pIdxm->fld_count);
	uint32_t slot_id = hash_val & pIdxm->slot_mask;

	xdb_hashlog ("add rid %d hash %x slot %d\n", new_rid, hash_val, slot_id);

	xdb_hashHdr_t	*pHashHdr  = pIdxm->pHashHdr;	
	xdb_rowid		*pHashSlot = pIdxm->pHashSlot;	
	xdb_hashNode_t	*pHashNode = pIdxm->pHashNode;
	xdb_rowid		rid, sid;

	xdb_hashNode_t	*pNewNode = &pHashNode[new_rid];
	pNewNode->hash_val = hash_val;

	xdb_rowid first_rid = pHashSlot[slot_id];

	if (0 == first_rid) {
		xdb_hashlog ("  insert first %d in slot %d \n", new_rid, slot_id);
		pHashSlot[slot_id] = new_rid;
		pNewNode->next = pNewNode->sibling = 0;
		pNewNode->prev = XDB_ROWID_MSB | slot_id;
		pHashHdr->slot_count++;
		pHashHdr->node_count++;
	} else {
		xdb_hashNode_t	*pTopNode = NULL, *pSibNode;
		for (rid = first_rid; rid > 0; rid = pTopNode->next) {
			pTopNode = &pHashNode[rid];
			if (pTopNode->hash_val != hash_val) {
				continue;
			}
			void *pRowDb = XDB_IDPTR(pStgMgr, rid);
			bool eq = xdb_row_isequal2 (pIdxm->pTblm, pRow, pRowDb, pIdxm->pFields, pIdxm->pExtract, pIdxm->fld_count);
			if (eq) {
				if (pIdxm->bUnique && pConn && xdb_row_valid (pConn, pIdxm->pTblm, pRowDb, rid)) {
					goto error;
				}
				break;
			}
		}
		if (rid) {
			if (pIdxm->bUnique && pConn) {
				for (sid = pTopNode->sibling; sid > 0; sid = pSibNode->next) {
					pSibNode = &pHashNode[sid];
					void *pRowDb = XDB_IDPTR(pStgMgr, sid);
					if (xdb_likely (xdb_row_valid (pConn, pIdxm->pTblm, pRowDb, sid))) {
						goto error;
					}
				}
			}

			xdb_hashlog ("  Duplicate insert %d in slot %d to rid %d as 1st sibling\n", new_rid, slot_id, rid);
			xdb_rowid sibling_rid = pTopNode->sibling;
			pTopNode->sibling = new_rid;
			pNewNode->prev  = 0;
			pNewNode->next = sibling_rid;
			pNewNode->sibling = rid;
			if (sibling_rid) {
				xdb_dbglog ("  1st sibling %d replace previous 1st silbing %d\n", new_rid, sibling_rid);
				pSibNode = &pHashNode[sibling_rid];
				pSibNode->prev = new_rid;
				pSibNode->sibling = XDB_ROWID_MSB;
			}
		} else {
			xdb_hashlog ("  Insert %d in slot %d at head %d\n", new_rid, slot_id, first_rid);
			pHashSlot[slot_id] = new_rid;
			pNewNode->next = first_rid;
			pNewNode->prev = XDB_ROWID_MSB | slot_id;
			pNewNode->sibling = 0;
			xdb_hashNode_t *pNxtNode = &pHashNode[first_rid];
			pNxtNode->prev = new_rid;
			pHashHdr->node_count++;
		}
	}

	pHashHdr->row_count++;

#if 0
	if (xdb_hash_check (pConn, pIdxm, 0) < 0) { exit(-1); }
#endif

	return 0;

error:
	//XDB_SETERR(XDB_E_EXISTS, "Duplicate insert for index '%s'", XDB_OBJ_NAME(pIdxm));
	return -XDB_E_EXISTS;
}

XDB_STATIC int 
xdb_hash_rem (xdb_idxm_t* pIdxm, xdb_rowid rid, void *pRow)
{
	xdb_hashNode_t *pTopNode, *pCurNode, *pNxtNode, *pPreNode, *pSibNode, *pNxtSibNode;
	uint32_t slot_id;

	xdb_hashHdr_t	*pHashHdr = pIdxm->pHashHdr;
	xdb_rowid		*pHashSlot = pIdxm->pHashSlot;
	xdb_hashNode_t *pHashNode = pIdxm->pHashNode;

	pCurNode = &pHashNode[rid];

	xdb_hashlog ("del rid %d hash %x slot %d\n", rid, pCurNode->hash_val, slot_id);

	if (xdb_unlikely (! (pCurNode->next + pCurNode->prev + pCurNode->sibling))) {
		// not in index
		return 0;
	}

	if (xdb_likely (! (pCurNode->sibling & XDB_ROWID_MASK))) {
		// normal node, either not 1st sibling or no sibling top
		if (pCurNode->next) {
			//xdb_dbglog ("  rid %d has next %d, point to its prev %d\n", rid, pCurNode->next, pCurNode->prev & XDB_ROWID_MASK);
			pNxtNode = &pHashNode[pCurNode->next];
			pNxtNode->prev = pCurNode->prev;
		}
		if (pCurNode->prev & XDB_ROWID_MSB) {
			// is the first top record
			slot_id = pCurNode->prev & XDB_ROWID_MASK;
			pHashSlot[slot_id] = pCurNode->next;
			if (0 == pCurNode->next) {
				pHashHdr->slot_count--;
			}
			xdb_hashlog ("  rid %d's next %d is 1st top record for slot %d\n", rid, pCurNode->next, slot_id);
		} else if (pCurNode->prev) {
			xdb_hashlog ("  rid %d has prev %d, point to it's next %d\n", rid, pCurNode->prev, pCurNode->next);
			pPreNode = &pHashNode[pCurNode->prev];
			pPreNode->next = pCurNode->next;
 		}
		if (! (pCurNode->sibling & XDB_ROWID_MSB)) {
			pHashHdr->node_count--;
		}
	} else {
		if (0 == pCurNode->prev) {
			// It's the 1st sibling
			pTopNode = &pHashNode[pCurNode->sibling];
			pTopNode->sibling = pCurNode->next;
			if (pCurNode->next) {
				xdb_hashlog ("  1st sibling %d has next sibling %d, prompt to 1st siblinig\n", rid, pCurNode->next);
				pNxtSibNode = &pHashNode[pCurNode->next];
				pNxtSibNode->prev = 0;
				pNxtSibNode->sibling = pCurNode->sibling;
			}
		} else {
			// It's the top node which has sibling
			xdb_hashlog ("  Top rid %d has sibling %d, prompt to top\n", rid, pCurNode->sibling);
			pSibNode = &pHashNode[pCurNode->sibling];
			// if has next silbing, prompt to first sibling
			if (pSibNode->next) {
				xdb_hashlog ("  1st Sibling %d has next sibling %d, prompt to 1st sibling\n", pCurNode->sibling, pSibNode->next);
				pNxtSibNode = &pHashNode[pSibNode->next];
				pNxtSibNode->sibling = pNxtSibNode->prev;
				pNxtSibNode->prev = 0;
			}
			pSibNode->sibling = pSibNode->next;
			pSibNode->next = pCurNode->next;
			if (pCurNode->next) {
				xdb_hashlog ("  rid %d has next %d, point to it's sibling %d\n", rid, pCurNode->next, pCurNode->sibling);
				pNxtNode = &pHashNode[pCurNode->next];
				pNxtNode->prev = pCurNode->sibling;
			}
			pSibNode->prev = pCurNode->prev;
			if (pCurNode->prev & XDB_ROWID_MSB) {
				slot_id = pCurNode->prev & XDB_ROWID_MASK;
				pHashSlot[slot_id] = pCurNode->sibling;
				xdb_hashlog ("  rid %d's 1st sibing %d prompt to 1st top for slot %d\n", rid, pCurNode->sibling, slot_id);
			} else if (pCurNode->prev) {
				xdb_hashlog ("  rid %d has prev %d, point to it's sibling %d\n", rid, pCurNode->prev, pCurNode->sibling);
				pPreNode = &pHashNode[pCurNode->prev];
				pPreNode->next = pCurNode->sibling;
			}
		}
	}

	// reset curnode
	pCurNode->next		= 0;
	pCurNode->prev		= 0;
	pCurNode->sibling	= 0;

	pHashHdr->row_count--;

#if 0
	if (xdb_hash_check (NULL, pIdxm, 0) < 0) { exit(-1); }
#endif

	return 0;
}

XDB_STATIC xdb_rowid 
xdb_hash_query (xdb_conn_t *pConn, xdb_idxfilter_t *pIdxFilter, xdb_rowset_t *pRowSet)
{
	xdb_idxm_t			*pIdxm = pIdxFilter->pIdxm;
	uint32_t hash_val = xdb_val_hash (pIdxFilter->pIdxVals, pIdxm->fld_count);
	uint32_t slot_id = hash_val & pIdxm->slot_mask;
	int				count = pIdxFilter->idx_flt_cnt;

	xdb_hashHdr_t	*pHashHdr  = pIdxm->pHashHdr;	
	xdb_rowid		*pHashSlot = pIdxm->pHashSlot;	
	xdb_hashNode_t	*pHashNode = pIdxm->pHashNode;
	xdb_stgmgr_t	*pStgMgr	= &pIdxm->pTblm->stg_mgr;

	//affect multi-thead performance
#if !defined (XDB_HPO)
	pHashHdr->query_times++;
#endif

	xdb_rowid rid = pHashSlot[slot_id];

	xdb_hashlog ("hash_get_slot hash 0x%x mod 0x%x slot %d 1st row %d\n", hash_val, pIdxm->slot_mask, slot_id, rid);

	xdb_hashNode_t	*pCurNode;
	for (; rid > 0; rid = pCurNode->next) {
		pCurNode = &pHashNode[rid];
		xdb_prefetch (pCurNode);
		void *pRow = XDB_IDPTR(pStgMgr, rid);
		xdb_prefetch (pRow);
		if (xdb_unlikely (pCurNode->hash_val != hash_val)) {
			continue;
		}
		if (xdb_unlikely (! xdb_row_isequal (pIdxm->pTblm, pRow, pIdxm->pFields, pIdxm->pExtract, pIdxFilter->pIdxVals, pIdxm->fld_count))) {
			continue;
		}
		if (xdb_likely (xdb_row_valid (pConn, pIdxm->pTblm, pRow, rid))) {
			// Compare rest fields
	 		if ((0 == count) || xdb_row_and_match (pIdxm->pTblm, pRow, pIdxFilter->pIdxFlts, count)) {
				if (xdb_unlikely (-XDB_E_FULL == xdb_rowset_add (pRowSet, rid, pRow))) {
					return XDB_OK;
				}
			}
		}

		for (rid = pCurNode->sibling; rid > 0; rid = pCurNode->next) {
			pCurNode = &pHashNode[rid];
			pRow = XDB_IDPTR(pStgMgr, rid);
			if (xdb_likely (xdb_row_valid (pConn, pIdxm->pTblm, pRow, rid))) {
				// Compare rest fields
				if ((0 == count) || xdb_row_and_match (pIdxm->pTblm, pRow, pIdxFilter->pIdxFlts, count)) {
					if (xdb_unlikely (-XDB_E_FULL == xdb_rowset_add (pRowSet, rid, pRow))) {
						return XDB_OK;
					}
				}
			}
		}

		return XDB_OK;
	}

	return XDB_OK;
}

XDB_STATIC xdb_rowid 
xdb_hash_query2 (xdb_conn_t *pConn, struct xdb_idxm_t* pIdxm, void *pRow2)
{
	uint32_t hash_val = xdb_row_hash2 (pIdxm->pTblm, pRow2, pIdxm->pFields, pIdxm->pExtract, pIdxm->fld_count);
	uint32_t slot_id = hash_val & pIdxm->slot_mask;

	xdb_hashHdr_t	*pHashHdr  = pIdxm->pHashHdr;	
	xdb_rowid		*pHashSlot = pIdxm->pHashSlot;	
	xdb_hashNode_t	*pHashNode = pIdxm->pHashNode;
	xdb_stgmgr_t	*pStgMgr	= &pIdxm->pTblm->stg_mgr;

	//affect multi-thead performance
#if !defined (XDB_HPO)
	pHashHdr->query_times++;
#endif

	xdb_rowid rid = pHashSlot[slot_id];

	xdb_hashlog ("hash_get_slot2 hash 0x%x mod 0x%x slot %d 1st row %d\n", hash_val, pIdxm->slot_mask, slot_id, rid);

	xdb_hashNode_t	*pCurNode;
	for (; rid > 0; rid = pCurNode->next) {
		pCurNode = &pHashNode[rid];
		xdb_prefetch (pCurNode);
		void *pRow = XDB_IDPTR(pStgMgr, rid);
		xdb_prefetch (pRow);
		if (xdb_unlikely (pCurNode->hash_val != hash_val)) {
			continue;
		}
		if (xdb_row_isequal2 (pIdxm->pTblm, pRow, pRow2, pIdxm->pFields, pIdxm->pExtract, pIdxm->fld_count) && 
			xdb_row_valid (pConn, pIdxm->pTblm, pRow, rid)) {
			return rid;
		}

		for (rid = pCurNode->sibling; rid > 0; rid = pCurNode->next) {
			pCurNode = &pHashNode[rid];
			pRow = XDB_IDPTR(pStgMgr, rid);
			if (xdb_likely (xdb_row_valid (pConn, pIdxm->pTblm, pRow, rid))) {
				return rid;
			}
		}

		return 0;
	}

	return 0;
}

XDB_STATIC int 
xdb_hash_close (xdb_idxm_t *pIdxm)
{
	char path[XDB_PATH_LEN + 32];
	xdb_tblm_t *pTblm = pIdxm->pTblm;

	xdb_sprintf (path, "%s/T%06d/I%02d.idx", pTblm->pDbm->db_path, XDB_OBJ_ID(pTblm), XDB_OBJ_ID(pIdxm));
	xdb_stg_close (&pIdxm->stg_mgr);

	xdb_sprintf (path, "%s/T%06d/I%02d.hash", pTblm->pDbm->db_path, XDB_OBJ_ID(pTblm), XDB_OBJ_ID(pIdxm));
	xdb_stg_close (&pIdxm->stg_mgr2); 

	return XDB_OK;
}

XDB_STATIC int 
xdb_hash_drop (xdb_idxm_t *pIdxm)
{
	char path[XDB_PATH_LEN + 32];
	xdb_tblm_t *pTblm = pIdxm->pTblm;

	xdb_sprintf (path, "%s/T%06d/I%02d.idx", pTblm->pDbm->db_path, XDB_OBJ_ID(pTblm), XDB_OBJ_ID(pIdxm));
	xdb_stg_drop (&pIdxm->stg_mgr, path);
	
	xdb_sprintf (path, "%s/T%06d/I%02d.hash", pTblm->pDbm->db_path, XDB_OBJ_ID(pTblm), XDB_OBJ_ID(pIdxm));
	xdb_stg_drop (&pIdxm->stg_mgr2, path); 

	return XDB_OK;
}

XDB_STATIC int 
xdb_hash_create (xdb_idxm_t *pIdxm)
{
	xdb_tblm_t *pTblm = pIdxm->pTblm;
	char path[XDB_PATH_LEN + 32];
	xdb_sprintf (path, "%s/T%06d/I%02d.idx", pTblm->pDbm->db_path, XDB_OBJ_ID(pTblm), XDB_OBJ_ID(pIdxm));

	xdb_stghdr_t stg_hdr = {.stg_magic = 0xE7FCFDFB, .blk_flags=XDB_STG_NOALLOC, .blk_size = sizeof(xdb_hashNode_t), 
							.ctl_off = 0, .blk_off = XDB_OFFSET(xdb_hashHdr_t, hash_node)};
	pIdxm->stg_mgr.pOps = pTblm->stg_mgr.pOps;
	pIdxm->stg_mgr.pStgHdr	= &stg_hdr;
	int rc = xdb_stg_open (&pIdxm->stg_mgr, path, NULL, NULL);
	if (rc != XDB_OK) {
		xdb_errlog ("Failed to create index %s", XDB_OBJ_NAME(pIdxm));
	}

	xdb_sprintf (path, "%s/T%06d/I%02d.hash", pTblm->pDbm->db_path, XDB_OBJ_ID(pTblm), XDB_OBJ_ID(pIdxm));

	// don't manage, clear when expand
	xdb_stghdr_t stg_hdr2 = {.stg_magic = 0xE7FCFDFB, .blk_flags=XDB_STG_NOALLOC|XDB_STG_CLEAR, .blk_size = sizeof(xdb_rowid), 
							.ctl_off = 0, .blk_off = XDB_OFFSET(xdb_hashSlot_t, hash_slot)};
	pIdxm->stg_mgr2.pOps = pTblm->stg_mgr.pOps;
	pIdxm->stg_mgr2.pStgHdr	= &stg_hdr2;
	rc = xdb_stg_open (&pIdxm->stg_mgr2, path, NULL, NULL);
	if (rc != XDB_OK) {
		xdb_errlog ("Failed to create index %s", XDB_OBJ_NAME(pIdxm));
	}

	pIdxm->pHashHdr = (xdb_hashHdr_t*)pIdxm->stg_mgr.pStgHdr;
	pIdxm->pHashNode = pIdxm->stg_mgr.pBlkDat1;
	pIdxm->node_cap = XDB_STG_CAP(&pIdxm->stg_mgr);

	pIdxm->slot_cap = XDB_STG_CAP(&pIdxm->stg_mgr2);
	pIdxm->slot_mask = pIdxm->slot_cap - 1;
	pIdxm->pHashSlot  = pIdxm->stg_mgr2.pBlkDat;

	return XDB_OK;
}

XDB_STATIC int 
xdb_hash_init (xdb_idxm_t *pIdxm)
{
	pIdxm->pHashHdr->row_count	= 0;
	pIdxm->pHashHdr->node_count	= 0;
	pIdxm->pHashHdr->slot_count = 0;
	pIdxm->pHashHdr->old_count	= 0;
	pIdxm->pHashHdr->rehash_count = 0;
	memset (pIdxm->pHashSlot, 0, sizeof(xdb_rowid) * XDB_STG_CAP(&pIdxm->stg_mgr2));
	return XDB_OK;
}

XDB_STATIC int 
xdb_hash_sync (xdb_idxm_t *pIdxm)
{
	xdb_stg_sync (&pIdxm->stg_mgr,    0, 0, false);
	xdb_stg_sync (&pIdxm->stg_mgr2,   0, 0, false);
	return 0;
}

static xdb_idx_ops s_xdb_hash_ops = {
	.idx_add 	= xdb_hash_add,
	.idx_rem 	= xdb_hash_rem,
	.idx_query 	= xdb_hash_query,
	.idx_query2	= xdb_hash_query2,
	.idx_create = xdb_hash_create,
	.idx_drop 	= xdb_hash_drop,
	.idx_close 	= xdb_hash_close,
	.idx_init	= xdb_hash_init,
	.idx_sync	= xdb_hash_sync
};
