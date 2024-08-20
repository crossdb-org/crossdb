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

#ifndef __XDB_TBL_H__
#define __XDB_TBL_H__

typedef struct xdb_tblm_t {
	xdb_obj_t		obj;
	uint16_t		fld_count;
	uint16_t		null_off;
	uint32_t		row_size;
	uint32_t		blk_size;

	bool			bMemory;
	xdb_lockmode_t	lock_mode;

	struct xdb_dbm_t *pDbm;

	struct	xdb_field_t *pFields;

	xdb_objm_t		fld_objm;

	uint32_t		meta_size;
	xdb_meta_t		*pMeta;

	xdb_objm_t		idx_objm;

	uint8_t			idx_order[XDB_MAX_INDEX];

	xdb_rowid		row_cap;

	xdb_stgmgr_t	stg_mgr;
	
	xdb_rwlock_t	tbl_lock;
	xdb_rwlock_t	stg_lock;
} xdb_tblm_t;

typedef struct xdb_tbl_t {
	xdb_stghdr_t	blk_hdr;
	xdb_rowid		row_count;
	xdb_rowid		rsvd;
	uint64_t		rsvd2[15];
	//statistics
	uint8_t			pRowDat[];
} xdb_tbl_t;

XDB_STATIC xdb_tblm_t* 
xdb_find_table (xdb_dbm_t *pDbm, const char *tbl_name);

XDB_STATIC int 
xdb_close_table (xdb_tblm_t *pTblm);

XDB_STATIC int 
xdb_drop_table (xdb_tblm_t *pTblm);

XDB_STATIC int 
xdb_find_field (xdb_tblm_t *pTblm, const char *fld_name, int len);

XDB_STATIC int 
xdb_dump_create_table (xdb_tblm_t *pTblm, char buf[], xdb_size size, uint32_t flags);

XDB_STATIC int 
xdb_flush_table (xdb_tblm_t *pTblm, uint32_t flags);

#endif // __XDB_TBL_H__
