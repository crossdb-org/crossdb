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

#ifndef __XDB_VDATA_H__
#define __XDB_VDATA_H__

typedef struct {
	uint16_t 	cap;
	uint16_t 	len;
	uint8_t		vchar[];
} xdb_vchar_t;

typedef struct xdb_vdat_t {
	xdb_stghdr_t	blk_hdr;
	uint64_t		rsvd[4];
	uint8_t			pVdat[];
} xdb_vdat_t;

typedef struct xdb_vdatm_t {
	struct xdb_tblm_t *pTblm;
	xdb_stgmgr_t	stg_mgr[];
} xdb_vdatm_t;

XDB_STATIC xdb_rowid
xdb_vdata_alloc (xdb_conn_t *pConn, xdb_vdatm_t *pVdatm, uint8_t *pType, void **ppVdatDb, int len);

XDB_STATIC void 
xdb_vdata_close (xdb_vdatm_t *pVdatm);

XDB_STATIC void 
xdb_vdata_drop (xdb_vdatm_t *pVdatm);

XDB_STATIC void
xdb_vdata_free (xdb_vdatm_t *pVdatm, uint8_t type, xdb_rowid vid);

static inline void* 
xdb_vdata_get (xdb_vdatm_t *pVdatm, uint8_t type, xdb_rowid vid)
{
	return XDB_IDPTR(&pVdatm->stg_mgr[type], vid);
}

typedef struct {
	uint32_t vdat_len: 28, ref_cnt: 4;
} xdb_vdathdr_t;

static inline void 
xdb_vdata_ref (xdb_vdatm_t *pVdatm, uint8_t type, xdb_rowid vid)
{
	xdb_vdathdr_t *pVdatHdr = xdb_vdata_get (pVdatm, type, vid);
	pVdatHdr->ref_cnt++;
}

static inline void
xdb_vdata_free (xdb_vdatm_t *pVdatm, uint8_t type, xdb_rowid vid)
{
	xdb_stgmgr_t *pStgMgr = &pVdatm->stg_mgr[type];
	xdb_vdathdr_t *pVdatHdr = XDB_IDPTR(pStgMgr, vid);

	if (pVdatHdr->ref_cnt  <= 1) {
		//xdb_print ("free t %d v %d\n", type, vid);
		xdb_stg_free (pStgMgr, vid, pVdatHdr);
	} else {
		--pVdatHdr->ref_cnt;
	}
}

static inline int
xdb_vdata_expand (xdb_vdatm_t *pVdatm, uint8_t type)
{
	return xdb_stg_expand (&pVdatm->stg_mgr[type]);
}

#endif // __XDB_VDATA_H__
