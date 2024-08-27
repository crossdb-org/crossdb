/******************************************************************************
* Copyright (c) 2024-present JC Wang. All rights reserved
*
*   https://crossdb.org
*   https://github.com/crossdb-org/crossdb
*
* This program is free software: you can use, redistribute, and/or modify it under
* the terms of the GNU Lesser General Public License, version 3 or later ("LGPL"),
* as published by the Free Software Foundation.
*
* This program is distributed in the hope that it will be useful, but WITHOUT
* ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS 
* FOR A PARTICULAR PURPOSE. See the GNU published General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public License along with 
* this program. If not, see <http://www.gnu.org/licenses/>.
******************************************************************************/

#ifndef __XDB_STORE_H__
#define __XDB_STORE_H__

#define XDB_IDPTR(pStgMgr, id)	((pStgMgr)->pBlkDat1 + id * (pStgMgr)->blk_size)

typedef struct xdb_store_ops {
	int  (*store_open) 	(const char *file, xdb_fd *pFd, xdb_size *pSize);
	int  (*store_close) (xdb_fd *pFd, xdb_size size, void **ptr);
	int  (*store_drop)  (char *file);
	int  (*store_map) 	(xdb_fd fd, xdb_size size, bool bTruncate, void **ptr);
	int  (*store_remap) (xdb_fd fd, xdb_size old_size, xdb_size new_size, void **ptr);
	int  (*store_sync)  (xdb_fd fd, xdb_size size, void *ptr, bool bAsync);
} xdb_store_ops;

#define XDB_ROW_MASK 0x3

typedef enum {
	XDB_ROW_FREE,   // 00 ->dirty->trans->commit
	XDB_ROW_DIRTY,  // 01
	XDB_ROW_COMMIT,	// 10
	XDB_ROW_TRANS, 	// 11 in trans, not commit
} xdb_rowCtrl_e;

#define XDB_ROW_CTRL(pStgHdr,pRow)	*((uint8_t*)(pRow) + pStgHdr->ctl_off)

typedef struct xdb_stghdr_t {
	uint32_t		stg_magic;
	uint32_t		revision;
	uint32_t		blk_size;
	uint32_t		ctl_off;
	uint16_t		blk_off;
	uint8_t			blk_type;
	uint8_t			blk_flags;
	xdb_rowid		blk_head;
	xdb_rowid		blk_tail;
	xdb_rowid		blk_alloc;
	xdb_rowid		blk_maxid;
	xdb_rowid		blk_cap;
	xdb_rowid		blk_limit;
	uint32_t		rsvd2[5];
} xdb_stghdr_t;

typedef struct xdb_stgmgr_t {
	xdb_stghdr_t	*pStgHdr;
	xdb_fd 			stg_fd;
	xdb_store_ops	*pOps;
	void			*pBlkDat;
	void			*pBlkDat1;
	uint32_t		blk_size;
	char			*file;
} xdb_stgmgr_t;

#define XDB_STG_CAP(pStgMgr)	(pStgMgr)->pStgHdr->blk_cap
#define XDB_STG_MAXID(pStgMgr)	(pStgMgr)->pStgHdr->blk_maxid
#define XDB_STG_ALLOC(pStgMgr)	(pStgMgr)->pStgHdr->blk_alloc

#define XDB_ROW_VALID(pTblm,pRow)	((XDB_ROW_CTRL (pTblm->stg_mgr.pStgHdr, pRow) & 0x3) >= XDB_ROW_COMMIT)

#endif // __XDB_STORE_H__
