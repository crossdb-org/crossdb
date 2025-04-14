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

#ifndef __XDB_TBL_H__
#define __XDB_TBL_H__

typedef struct xdb_tblm_t {
	xdb_obj_t		obj;
	uint16_t		fld_count;
	uint16_t		vfld_count;
	uint16_t		null_bytes;
	uint32_t		row_size;
	uint32_t		vtype_off;
	uint32_t		null_off;
	uint32_t		blk_size;

	bool			bLog;
	bool			bMemory;
	bool			bPrimary;
	xdb_lockmode_e	lock_mode;

	struct xdb_dbm_t *pDbm;

	struct	xdb_field_t *pFields;
	struct	xdb_field_t **ppFields;
	struct	xdb_field_t **ppVFields;
	uint8_t			*pNullBytes;

	xdb_objm_t		fld_objm;

	xdb_objm_t		trig_objm[XDB_TRIG_MAX];

	uint32_t		meta_size;
	xdb_meta_t		*pMeta;

	xdb_vdatm_t		*pVdatm;

	xdb_objm_t		idx_objm;
	uint8_t			idx_order[XDB_MAX_INDEX];

	xdb_objm_t		fkey_objm;
	xdb_objm_t		fkeyref_objm;

	xdb_rowid		row_cap;

	xdb_stgmgr_t	stg_mgr;
	
	xdb_rwlock_t	tbl_lock;
	xdb_rwlock_t	stg_lock;

	xdb_vec_t		sub_list;

	xdb_bmp_t		*pAuditRows;
} xdb_tblm_t;

typedef struct xdb_tbl_t {
	xdb_stghdr_t	blk_hdr;
	xdb_rowid		row_count;
	uint8_t			tbl_dirty;
	uint8_t			rsvd[3];
	uint64_t		lastchg_id;	// last commit id
	uint64_t		flush_id;	// last flush id
	//statistics
	uint64_t		rsvd2[13];
	uint8_t			pRowDat[];
} xdb_tbl_t;

#define XDB_TBLPTR(pTblm)	((xdb_tbl_t*)pTblm->stg_mgr.pStgHdr)


static inline xdb_tblm_t* 
xdb_find_table (xdb_dbm_t *pDbm, const char *tbl_name)
{
	return xdb_objm_get (&pDbm->db_objm, tbl_name);
}

static inline xdb_field_t* 
xdb_find_field (xdb_tblm_t *pTblm, const char *fld_name, int len)
{
	return xdb_objm_get2 (&pTblm->fld_objm, fld_name, len);
}

XDB_STATIC int 
__xdb_repair_table (xdb_tblm_t *pTblm, uint32_t flags);

XDB_STATIC int 
xdb_close_table (xdb_tblm_t *pTblm);

XDB_STATIC int 
xdb_drop_table (xdb_tblm_t *pTblm);

XDB_STATIC int 
xdb_dump_create_table (xdb_tblm_t *pTblm, char buf[], xdb_size size, uint32_t flags);

XDB_STATIC int 
xdb_flush_table (xdb_tblm_t *pTblm, uint32_t flags);

#endif // __XDB_TBL_H__
