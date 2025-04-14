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

#define XDB_VTYPE_NONE		255 // NO vdata
//#define XDB_VTYPE_VID		
#define XDB_VTYPE_DATA		254	// vdata is after row
#define XDB_VTYPE_PTR		253 // vdata is in ptr array
#define XDB_VTYPE_MAX		76  // vdata is in db

#define XDB_VTYPE_OK(type)	(type<XDB_VTYPE_MAX)

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
xdb_vdata_alloc (xdb_vdatm_t *pVdatm, uint8_t *pType, void **ppVdatDb, int len);

XDB_STATIC void 
xdb_vdata_close (xdb_vdatm_t *pVdatm);

XDB_STATIC void 
xdb_vdata_drop (xdb_vdatm_t *pVdatm);

XDB_STATIC void
xdb_vdata_free (xdb_vdatm_t *pVdatm, uint8_t type, xdb_rowid vid);

XDB_STATIC int 
xdb_vdata_create (xdb_vdatm_t *pVdatm, int type);

static inline void* 
xdb_vdata_get (xdb_vdatm_t *pVdatm, uint8_t type, xdb_rowid vid)
{
	xdb_stgmgr_t *pStgMgr = &pVdatm->stg_mgr[type];
	if (xdb_unlikely (NULL == pStgMgr->pStgHdr)) {
		xdb_vdata_create (pVdatm, type);
	}
	return XDB_IDPTR(pStgMgr, vid);
}

// MSB=1 is for use/free mark, next 3b is refcnt
#define XDB_VDAT_LENBITS	28
#define XDB_VDAT_LENMASK	((1<<XDB_VDAT_LENBITS) - 1)

static inline void 
xdb_vdata_ref (xdb_vdatm_t *pVdatm, uint8_t type, xdb_rowid vid)
{
	uint32_t *pVdat = xdb_vdata_get (pVdatm, type, vid);
	if (xdb_likely (pVdat != NULL)) {
		*pVdat += (1<<XDB_VDAT_LENBITS);
	}
}

static inline int
xdb_vdata_expand (xdb_vdatm_t *pVdatm, uint8_t type)
{
	xdb_stgmgr_t *pStgMgr = &pVdatm->stg_mgr[type];
	if (xdb_unlikely (NULL == pStgMgr->pStgHdr)) {
		xdb_vdata_create (pVdatm, type);
	}
	return xdb_stg_expand (&pVdatm->stg_mgr[type]);
}

#endif // __XDB_VDATA_H__
