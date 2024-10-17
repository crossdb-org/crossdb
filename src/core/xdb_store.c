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

XDB_STATIC int 
xdb_store_mem_open (const char *file, xdb_fd *pFd, xdb_size *pSize)
{
	(void)file;

	*pFd = XDB_INV_FD;
	*pSize = 0;

	return 0;
}

XDB_STATIC int 
xdb_store_mem_close (xdb_fd *pFd, xdb_size size, void **ptr)
{
	(void)size;

	xdb_free (*ptr);
	*ptr = NULL;
	*pFd = XDB_INV_FD;

	return 0;
}

XDB_STATIC int 
xdb_store_mem_drop (char *file)
{
	(void)file;
	return 0;
}

XDB_STATIC int 
xdb_store_mem_map (xdb_fd fd, xdb_size size, bool bTruncate, void **ptr)
{
	(void)fd;
	(void)bTruncate;

	*ptr = xdb_malloc (size);
	if (NULL == *ptr) {
		return -XDB_E_MEMORY;
	}

	return 0;
}

XDB_STATIC int 
xdb_store_mem_remap (xdb_fd fd, xdb_size old_size, xdb_size new_size, void **ptr)
{
	void *newptr;
	(void)fd;

	newptr = xdb_realloc (*ptr, new_size);
	if (NULL == newptr) {
		return -XDB_E_MEMORY;
	}

	*ptr = newptr;
	return 0;
}

XDB_STATIC int 
xdb_store_mem_sync (xdb_fd fd, xdb_size size, void *ptr, bool bAsync)
{
	(void)fd;
	(void)size;
	(void)ptr;
	(void)bAsync;
	return 0;
}

static xdb_store_ops s_xdb_store_mem_ops = {
	.store_open		= xdb_store_mem_open,
	.store_close	= xdb_store_mem_close,
	.store_drop		= xdb_store_mem_drop,
	.store_map		= xdb_store_mem_map,
	.store_remap	= xdb_store_mem_remap,
	.store_sync		= xdb_store_mem_sync
};

XDB_STATIC int 
xdb_store_file_open (const char *file, xdb_fd *pFd, xdb_size *pSize)
{
	int	ret;
	char *pDir = strrchr (file, '/');
	if (NULL == pDir) {
		pDir = strrchr (file, '\\');
	}

	if (NULL != pDir) {
		*pDir = '\0';
		if (! xdb_fexist (file)) {
			ret = xdb_mkdir (file);
			if (ret < 0) {
				return -XDB_E_FILE;
			}
		}
		*pDir = '/';
	}

	*pFd = xdb_fdopen (file);
	if (XDB_INV_FD == *pFd) {
		return -XDB_E_FILE;
	}
	*pSize = xdb_fsize(*pFd);

	return 0;
}

XDB_STATIC int 
xdb_store_file_close (xdb_fd *pFd, xdb_size size, void **ptr)
{
	if (size > 0 && (ptr != NULL) && (NULL != *ptr)) {
		xdb_munmap (*ptr, size);
		*ptr = NULL;
	}
	xdb_fdclose (*pFd);
	return 0;
}

XDB_STATIC int 
xdb_store_file_drop (char *file)
{
	int ret;
	ret = remove (file);
	if (ret < 0) {
		xdb_errlog ("  remove %s ret -> %d %s\n", file, ret, strerror(errno));
	}

	return 0;
}

XDB_STATIC int 
xdb_store_file_map (xdb_fd fd, xdb_size size, bool bTruncate, void **ptr)
{
	int	ret;
	xdb_size fsize = xdb_fsize (fd);

	if (size == 0) {
		return -XDB_E_PARAM;
	}

	if (0 == fsize) {
		ret = xdb_ftruncate (fd, size);
		if (ret < 0) {
			return -XDB_E_FILE;
		}
	} else if (fsize < size) {
		xdb_errlog ("%"XDB_FDFMT" size is %"PRIi64" < %"PRIi64"\n", fd, fsize, size);
		return -XDB_E_FILE;
	} else if ((fsize > size) && bTruncate) {
		ret = xdb_ftruncate (fd, size);
		if (ret < 0) {
			return ret;
		}
	}

	*ptr = xdb_mmap (fd, 0, size, NULL);
	if (NULL == *ptr) {
		return -XDB_E_MEMORY;
	}

	return 0;
}

XDB_STATIC int 
xdb_store_file_remap (xdb_fd fd, xdb_size old_size, xdb_size new_size, void **ptr)
{
	void 		*newptr;

	newptr = xdb_remmap (fd, *ptr, old_size, new_size);
	if (NULL == newptr) {
		return -XDB_E_MEMORY;
	}

	*ptr = newptr;
	return 0;
}

XDB_STATIC int 
xdb_store_file_sync (xdb_fd fd, xdb_size size, void *ptr, bool bAsync)
{
	(void)fd;
	if (size > 0) {
		xdb_msync (ptr, size, bAsync);
	} else {
		xdb_fsync (fd);
	}
	return 0;
}

static xdb_store_ops s_xdb_store_file_ops = {
	.store_open		= xdb_store_file_open,
	.store_close	= xdb_store_file_close,
	.store_drop		= xdb_store_file_drop,
	.store_map		= xdb_store_file_map,
	.store_remap	= xdb_store_file_remap,
	.store_sync		= xdb_store_file_sync
};

XDB_STATIC void 
xdb_stg_free (xdb_stgmgr_t *pStgMgr, xdb_rowid rowid, void *pRow)
{
	xdb_stghdr_t	*pStgHdr = pStgMgr->pStgHdr;
	xdb_rowid *pNext = pRow;
	
	if (pStgHdr->ctl_off) {
		// mark free
		XDB_ROW_CTRL(pStgHdr,pNext) = 0;
	}
	*pNext = 0;
	if (pStgHdr->blk_tail) {
		xdb_rowid *pTail = XDB_IDPTR(pStgMgr, pStgHdr->blk_tail);
		*pTail = rowid;
	} else {
		pStgHdr->blk_head = rowid;
	}
	pStgHdr->blk_tail = rowid;
	//TBD
	if (pStgHdr->blk_maxid == rowid) {
		// find smaller in use fd
	}

	pStgHdr->blk_alloc--;
}

#if 0
XDB_STATIC void 
xdb_stg_free_range (xdb_stgmgr_t *pStgMgr, xdb_rowid rowid, xdb_rowid count)
{
	if (pStgMgr->pStgHdr->blk_flags & 1) {
		return;
	}
	xdb_rowid maxid = rowid + count;
	for (xdb_rowid rid = rowid; rid < maxid; ++rid) {
		xdb_stg_free (pStgMgr, rid);
	}
}
#endif

XDB_STATIC int 
xdb_stg_truncate (xdb_stgmgr_t *pStgMgr, xdb_rowid new_maxid)
{
	xdb_stghdr_t	*pStgHdr = pStgMgr->pStgHdr;
	xdb_size oldsize = pStgHdr->blk_off + pStgHdr->blk_size * pStgHdr->blk_cap;
	xdb_size newsize = pStgHdr->blk_off + pStgHdr->blk_size * new_maxid;

	//xdb_dbglog ("truncate %d -> %d\n", pStgHdr->blk_cap, new_maxid);

	int rc = pStgMgr->pOps->store_remap (pStgMgr->stg_fd, oldsize, newsize, (void**)&pStgMgr->pStgHdr);
	if (rc < 0) {
		return rc;
	}

	pStgHdr = pStgMgr->pStgHdr;
	pStgHdr->blk_cap = new_maxid;
	pStgMgr->pBlkDat = (void*)pStgHdr + pStgHdr->blk_off;
	pStgMgr->pBlkDat1 = pStgMgr->pBlkDat - pStgHdr->blk_size;
	if ((newsize > oldsize) && (pStgHdr->blk_flags & 0x2)) {
		memset ((void*)pStgMgr->pStgHdr + oldsize, 0, newsize - oldsize);
	}
	return 0;
}

XDB_STATIC int 
xdb_stg_expand (xdb_stgmgr_t *pStgMgr)
{
	xdb_stghdr_t	*pStgHdr = pStgMgr->pStgHdr;
	if (0 == pStgHdr->blk_head) {
		if (pStgHdr->blk_maxid == pStgHdr->blk_cap) {
			int rc = xdb_stg_truncate (pStgMgr, pStgHdr->blk_cap<<1);
			//rc = xdb_stg_truncate (pStgMgr, pStgHdr->blk_cap+1);
			if (rc < 0) {
				return rc;
			}
		}
	}
	return 0;
}

XDB_STATIC xdb_rowid 
xdb_stg_alloc (xdb_stgmgr_t *pStgMgr, void **ppRow)
{
	int rc;
	if (pStgMgr->pStgHdr->blk_flags & 1) {
		return -1;
	}
	xdb_stghdr_t	*pStgHdr = pStgMgr->pStgHdr;
	xdb_rowid 		rid = pStgHdr->blk_head;

	if (0 == rid) {
		if (pStgHdr->blk_maxid == pStgHdr->blk_cap) {
			rc = xdb_stg_truncate (pStgMgr, pStgHdr->blk_cap<<1);
			//rc = xdb_stg_truncate (pStgMgr, pStgHdr->blk_cap+1);
			if (rc < 0) {
				return -1;
			}
		}
		pStgHdr = pStgMgr->pStgHdr;
		rid = ++pStgHdr->blk_maxid;
		*ppRow = XDB_IDPTR(pStgMgr, rid);
 	} else {
		xdb_rowid *pNext = XDB_IDPTR(pStgMgr, rid);
		pStgHdr->blk_head = *pNext;
		if (pStgHdr->blk_tail == rid) {
			pStgHdr->blk_tail = 0;
		}
		*ppRow = pNext;
	}

	if (pStgHdr->ctl_off > 0) {
		XDB_ROW_CTRL(pStgHdr, *ppRow) = XDB_ROW_DIRTY;
	}
	pStgHdr->blk_alloc++;

	return rid;
}

XDB_STATIC int 
xdb_stg_open (xdb_stgmgr_t *pStgMgr, const char *file, void (*init_cb)(xdb_stgmgr_t *pStgMgr, void *pArg), void *pArg)
{
	xdb_stghdr_t	*pStgHdr = pStgMgr->pStgHdr;
	xdb_size fsize = 0, size;

	int rc = pStgMgr->pOps->store_open (file, &pStgMgr->stg_fd, &fsize);
	if (rc < 0) {
		return rc;
	}

	size = fsize;
	if (0 == size) {
		pStgHdr->blk_cap = 8;
		size = pStgHdr->blk_off + pStgHdr->blk_cap * pStgHdr->blk_size;
	}
	// pStgMgr->pStgHdr will point to new location
	rc = pStgMgr->pOps->store_map (pStgMgr->stg_fd, size, false, (void**)&pStgMgr->pStgHdr);
	if (rc < 0) {
		return rc;
	}
	pStgMgr->blk_size = pStgHdr->blk_size;
	pStgMgr->pBlkDat = (void*)pStgMgr->pStgHdr + pStgHdr->blk_off;
	pStgMgr->pBlkDat1 = pStgMgr->pBlkDat - pStgHdr->blk_size;
	if (0 == fsize) {
		memset (pStgMgr->pStgHdr, 0, pStgHdr->blk_off);
		memcpy (pStgMgr->pStgHdr, pStgHdr, sizeof(xdb_stghdr_t));
		if (pStgHdr->blk_flags & 0x2) {
			memset (pStgMgr->pBlkDat, 0, pStgHdr->blk_cap * pStgHdr->blk_size);
		}
		if (init_cb != NULL) {
			init_cb (pStgMgr, pArg);
		}
	}

	return XDB_OK;
}

XDB_STATIC int 
xdb_stg_close (xdb_stgmgr_t *pStgMgr)
{
	if (NULL == pStgMgr->pStgHdr) {
		return XDB_OK;
	}
	xdb_size size = pStgMgr->pStgHdr->blk_off + pStgMgr->pStgHdr->blk_size * pStgMgr->pStgHdr->blk_cap;

	int rc = pStgMgr->pOps->store_close (&pStgMgr->stg_fd, size, (void**)&pStgMgr->pStgHdr);
	if (rc < 0) {
		return rc;
	}
	return XDB_OK;
}

XDB_STATIC int 
xdb_stg_drop (xdb_stgmgr_t *pStgMgr, char *file)
{
	if (NULL == pStgMgr->pStgHdr) {
		return XDB_OK;
	}
	xdb_dbglog ("drop file %s\n", file);
	xdb_stg_close (pStgMgr);
	int rc = pStgMgr->pOps->store_drop (file);
	if (rc < 0) {
		return rc;
	}
	return XDB_OK;
}

XDB_STATIC int 
xdb_stg_sync (xdb_stgmgr_t *pStgMgr, xdb_size offset, xdb_size size, bool bAsync)
{
	int rc = pStgMgr->pOps->store_sync (pStgMgr->stg_fd, size, (void*)pStgMgr->pStgHdr + offset, bAsync);	
	if (rc < 0) {
		return rc;
	}
	return XDB_OK;
}
