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

#ifndef __XDB_COMMON_H__
#define __XDB_COMMON_H__

#ifndef __restrict
#define __restrict	restrict
#endif


typedef int	xdb_rowid;


/******************************************************************************
	MACRO
******************************************************************************/
	
#define XDB_LEN_MASK				0xFFFFFFF

#define XDB_ROWID_VALID(row_id, max_id)	((row_id > 0) && (row_id <= max_id))
#define XDB_ROWID_MSBIT				((sizeof(xdb_rowid)<<3)-1)
#define XDB_ROWID_MSB				(1<<XDB_ROWID_MSBIT)
#define XDB_ROWID_MASK				(~XDB_ROWID_MSB)

#define XDB_SETERR(code, errmsgfmt...)	\
		memset (&pConn->conn_res, 0, sizeof(pConn->conn_res));	\
		pConn->conn_res.len_type = (sizeof (xdb_res_t)) | (XDB_RET_REPLY<<28);	\
		pConn->conn_res.errcode = code; \
		pConn->conn_res.row_data = (uintptr_t)pConn->conn_msg.msg;	\
		pConn->conn_msg.len = snprintf(pConn->conn_msg.msg, sizeof(pConn->conn_msg.msg)-1, errmsgfmt);	\
		pConn->conn_msg.len_type = (XDB_RET_MSG<<28) | (pConn->conn_msg.len + 7);	\
		pConn->conn_res.data_len = pConn->conn_msg.len;

#define XDB_EXPECT_BRK(expr, code, errmsgfmt...)	\
	if (xdb_unlikely (!(expr))) {	\
		XDB_SETERR (code, errmsgfmt) \
		break;	\
	}

#define XDB_EXPECT_GOTO(expr, gototag, code, errmsgfmt...)	\
	if (xdb_unlikely (!(expr))) {	\
		XDB_SETERR (code, errmsgfmt) \
		goto gototag;	\
	}

#define XDB_EXPECT_RET(expr, retval, code, errmsgfmt...)	\
	if (xdb_unlikely (!(expr))) {	\
		XDB_SETERR (code, errmsgfmt) \
		return retval;	\
	}

#define XDB_EXPECT2(expr)	\
	if (xdb_unlikely (!(expr))) {	\
		goto error;	\
	}

#define XDB_EXPECT_RETE(expr, code, errmsgfmt...)	XDB_EXPECT_RET(expr, -code, code, errmsgfmt)

#define XDB_EXPECT(expr, code, errmsgfmt...)		XDB_EXPECT_GOTO(expr, error, code, errmsgfmt)

#define XDB_EXPECT_SOCK(expr, code, errmsgfmt...)	\
	if (xdb_unlikely (!(expr))) {	\
		XDB_SETERR (code, errmsgfmt) \
		xdb_sock_close (pConn->sockfd);	\
		pConn->sockfd = -1;	\
		goto error;	\
	}

#define XDB_CONNCODE(pConn)		pConn->conn_res.errcode

//#define XDB_LOG_FLAGS	(XDB_LOG_DB|XDB_LOG_TBL|XDB_LOG_TRANS|XDB_LOG_WAL)
//#define XDB_LOG_FLAGS	XDB_LOG_DB
//#define XDB_LOG_FLAGS	XDB_LOG_SVR
//#define XDB_LOG_FLAGS	XDB_LOG_PUBSUB
#ifndef XDB_LOG_FLAGS
#define XDB_LOG_FLAGS	0
#endif

#define XDB_LOG_DB		(1<<0)
#define XDB_LOG_TBL		(1<<1)
#define XDB_LOG_IDX		(1<<2)
#define XDB_LOG_HASH	(1<<3)
#define XDB_LOG_SQL		(1<<6)
#define XDB_LOG_CRUD	(1<<7)
#define XDB_LOG_STG		(1<<9)
#define XDB_LOG_TRANS	(1<<10)
#define XDB_LOG_WAL		(1<<11)
#define XDB_LOG_VDAT	(1<<12)
#define XDB_LOG_SVR		(1<<13)
#define XDB_LOG_BINLOG	(1<<14)
#define XDB_LOG_PUBSUB	(1<<15)

#define XDB_IS_NOTNULL(pNull,bits)	(((uint8_t*)(pNull))[bits>>3] & (1<<(bits&7)))
#define XDB_SET_NOTNULL(pNull,bits)	(((uint8_t*)(pNull))[bits>>3] |= (1<<(bits&7)))
#define XDB_SET_NULL(pNull,bits)	(((uint8_t*)(pNull))[bits>>3] &= ~(1<<(bits&7)))

static bool s_xdb_vdat[XDB_TYPE_MAX];


/******************************************************************************
	Types
******************************************************************************/

typedef enum {
	XDB_FLD_NOTNULL = 1<<0,
	XDB_FLD_PRIKEY 	= 1<<1,
	XDB_FLD_UNIKEY	= 1<<2,
	XDB_FLD_MULKEY	= 1<<3,
	XDB_FLD_BINARY 	= 1<<7,
} xdb_fld_flag_e;

typedef struct xdb_field_t {
	xdb_obj_t		obj;
	struct xdb_tblm_t	*pTblm;
	uint8_t			fld_type;
	uint8_t			sup_type;
	uint16_t		fld_flags;
	uint8_t			fld_decimal;
	uint8_t			fld_charset;
	uint16_t		fld_id;
	uint16_t		fld_vid;
	uint32_t		fld_off;
	int				fld_len;
	uint8_t			fld_idxnum;
	int8_t			idx_fid[XDB_MAX_MATCH_COL];
	uint64_t		idx_bmp;
} xdb_field_t;


/******************************************************************************
	RowSet
******************************************************************************/

typedef struct {
	xdb_rowid 	rid;
	void		*ptr;
} xdb_rowptr_t;

#define XDB_ROWLIST_CNT		16

// multiple table rowset map
typedef struct {
	// when change, use change xdb_rowset_init
	xdb_rowid 		count;
	xdb_rowid 		seqid;
	xdb_rowid		limit;
	xdb_rowid		offset;
	
	xdb_rowid 		cap;
	xdb_meta_t		*pTblMeta;
	uint16_t		*pFldMap;
	xdb_rowptr_t 	*pRowList;
	xdb_rowptr_t 	rowlist[XDB_ROWLIST_CNT];
	xdb_bmp_t		*pBmp, bmp;
	//uint8_t		buf[xxx]; // if no lock, then cache result ?
} xdb_rowset_t;

static volatile bool s_xdb_bInit;

int 
xdb_init ();

int 
xdb_exit ();

static xdb_type_t s_xdb_prompt_type[];
static bool s_xdb_cli;

#endif // __XDB_COMMON_H__
