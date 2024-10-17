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

static uint8_t s_xdb_vdat_4k[4096/16], s_xdb_vdat_256k[256*1024/1024 - 4], s_xdb_vdat_16m[16*1024/64 - 4];
static int s_xdb_vdat_size[] = {
	16,				32,				48,				64,				// 16
	80,				96,				112,			128,			// 16
	160,			192,			224,			256,			// 32
	320,			384,			448,			512,			// 64
	640,			768,			896,			1024,			// 128
	1280,			1536,			1792,			2048,			// 256
	2560, 			3072, 			3584, 			4096,			// 512
	5*1024,			6*1024, 		7*1024,			8*1024,			// 1k
	10*1024,		12*1024, 		14*1024,		16*1024,		// 2k
	20*1024,		24*1024,		28*1024,		32*1024,		// 4k
	40*1024,		48*1024,		56*1024,		64*1024,		// 8k
	80*1024,		96*1024,		112*1024,		128*1024,		// 16k
	160*1024,		192*1024,		224*1024,		256*1024,		// 32k
	320*1024,		384*1024,		448*1024,		512*1024,		// 64k
	640*1024,		768*1024,		896*1024,		1024*1024,		// 128k
	1280*1024,		1536*1024,		1792*1024,		2048*1024,		// 256k
	2560*1024,		3072*1024,		3584*1024,		4096*4096,		// 512k
	5*1024*1024,	6*1024*1024,	7*1024*1024,	8*1024*1024,	// 1m
	10*1024*1024,	12*1024*1024,	14*1024*1024,	16*1024*1024	// 2m
};

XDB_STATIC xdb_vdatm_t *
xdb_create_vdata (xdb_tblm_t *pTblm)
{
	xdb_vdatm_t *pVdatm = xdb_calloc (sizeof(xdb_vdatm_t) + XDB_ARY_LEN(s_xdb_vdat_size) * sizeof(xdb_stgmgr_t));
	if (NULL == pVdatm) {
		return NULL;
	}
	pVdatm->pTblm = pTblm;
	return pVdatm;
}

XDB_STATIC uint8_t xdb_vdat_find (int size)
{
	int min = 0, max = XDB_ARY_LEN (s_xdb_vdat_size) - 1;

	while (min < max) {
		int mid = (min + max) >> 1;
		if (size == s_xdb_vdat_size[mid]) {
			return mid;
		} else if (size > s_xdb_vdat_size[mid]) {
			min = mid + 1;
		} else {
			max = mid - 1;
			if (size > s_xdb_vdat_size[max]) {
				return mid;
			}
		}
	}

	return max;
}

XDB_STATIC void xdb_vdat_init ()
{
	for (int i = 0; i < XDB_ARY_LEN(s_xdb_vdat_4k); ++i) {
		s_xdb_vdat_4k[i] = xdb_vdat_find ((i+1)<<4);	// 16
	}
	for (int i = 0; i < XDB_ARY_LEN(s_xdb_vdat_256k); ++i) {
		s_xdb_vdat_256k[i] = xdb_vdat_find (4096 + ((i+1)<<10));	// 1k
	}
	for (int i = 0; i < XDB_ARY_LEN(s_xdb_vdat_16m); ++i) {
		s_xdb_vdat_16m[i] = xdb_vdat_find (256*1024 + ((i+1)<<16));		// 64k
	}
}

XDB_STATIC uint8_t xdb_vdat_type (int size)
{
	if (size <= 4096) {
		return s_xdb_vdat_4k[size>>4];
	} else if (size <= 256*1024) {
		return s_xdb_vdat_256k[size>>10];
	} else {
		return s_xdb_vdat_16m[size>>16];
	}
}

XDB_STATIC int 
xdb_vdata_create (xdb_conn_t *pConn, xdb_vdatm_t *pVdatm, int type)
{
	xdb_stghdr_t stg_hdr = {.stg_magic = 0xE7FCFDFB, .blk_size = s_xdb_vdat_size[type], .ctl_off = 0, .blk_off = XDB_OFFSET(xdb_vdat_t, pVdat)};

	xdb_stgmgr_t *pStgMgr = &pVdatm->stg_mgr[type];

	char path[XDB_PATH_LEN + 32];
	xdb_sprintf (path, "%s/T%06d/vdat/V%02d.vdat", pVdatm->pTblm->pDbm->db_path, XDB_OBJ_ID(pVdatm->pTblm), type);

	pStgMgr->pOps = pVdatm->pTblm->bMemory ? &s_xdb_store_mem_ops : &s_xdb_store_file_ops;
	pStgMgr->pStgHdr	= &stg_hdr;
	int rc = xdb_stg_open (pStgMgr, path, NULL, NULL);
	XDB_EXPECT (rc == XDB_OK, XDB_ERROR, "Failed to create vdat '%d'", type);

	return XDB_OK;

error:
	return rc;
}

XDB_STATIC void 
xdb_vdata_close (xdb_vdatm_t *pVdatm)
{
	if (NULL == pVdatm) {
		return;
	}
	xdb_tblm_t *pTblm = pVdatm->pTblm;
	for (int i = 0; i < XDB_ARY_LEN(s_xdb_vdat_size); ++i) {
		xdb_stg_close (&pTblm->pVdatm->stg_mgr[i]);
	}
	xdb_free (pTblm->pVdatm);
}

XDB_STATIC void 
xdb_vdata_drop (xdb_vdatm_t *pVdatm)
{
	if (NULL == pVdatm) {
		return;
	}
	xdb_tblm_t *pTblm = pVdatm->pTblm;
	for (int i = 0; i < XDB_ARY_LEN(s_xdb_vdat_size); ++i) {
		char path[XDB_PATH_LEN + 32];
		xdb_sprintf (path, "%s/T%06d/vdat/V%02d.vdat", pTblm->pDbm->db_path, XDB_OBJ_ID(pTblm->pVdatm->pTblm), i);
		xdb_stg_drop (&pVdatm->pTblm->pVdatm->stg_mgr[i], path);
	}
	xdb_free (pTblm->pVdatm);
}

XDB_STATIC xdb_rowid
xdb_vdata_alloc (xdb_conn_t *pConn, xdb_vdatm_t *pVdatm, uint8_t *pType, void **ppVdatDb, int len)
{
	*pType = xdb_vdat_type (len);
	xdb_stgmgr_t *pStgMgr = &pVdatm->stg_mgr[*pType];
	if (xdb_unlikely (NULL == pStgMgr->pStgHdr)) {
		xdb_vdata_create (pConn, pVdatm, *pType);
	}

	xdb_rowid vid = xdb_stg_alloc (pStgMgr, ppVdatDb);
	if (xdb_unlikely (vid <= 0)) {
		xdb_dbglog ("No space");
		return -1;
	}

	//xdb_print ("alloc t %d v %d\n", *pType, vid);

	return vid;
}
