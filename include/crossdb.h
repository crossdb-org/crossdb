#ifndef __CROSS_DB_H__
#define __CROSS_DB_H__

#include <inttypes.h>

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
	CrossDB Return Codes
******************************************************************************/

typedef enum {
	CROSS_OK = 0,
	CROSS_E_NOTFOUND = -1
} cross_ret;

const char *cross_rcName (cross_ret rc);
const char *cross_errMsgLast ();


/******************************************************************************
	CrossDB Types
******************************************************************************/

typedef int			cross_rowid;
typedef void*		cross_db_h;
typedef void*		cross_tbl_h;
typedef void*		cross_schema_h;
typedef void*		cross_cursor_h;
typedef void*		cross_match_h;


/******************************************************************************
	CrossDB Schema MACROS
******************************************************************************/

typedef enum {
	CROSS_FIELD_TYPE_INT 			= 0, // integer: for char, short, int, long long
	CROSS_FIELD_TYPE_UINT			= 1, // unsigned integer
	CROSS_FIELD_TYPE_FLOAT			= 2, // float and double
	CROSS_FIELD_TYPE_CHAR			= 3, // C string
	CROSS_FIELD_TYPE_BYTE			= 4, // byte arrary (unsigned char)
	CROSS_FIELD_TYPE_BITFLD			= 5, // C bitfield
	CROSS_FIELD_TYPE_STRUCT			= 7, // C Struct
	CROSS_FIELD_TYPE_UNION			= 8, // C Union
} cross_field_type;

typedef enum {
	CROSS_FIELD_FORMAT_DFT 			= 0,  // INT/UINT: decimal, BIN: hex
	CROSS_FIELD_FORMAT_BOOL			= 1,  // TRUE/FALSE, input: 0/1 or T/F or t/f
	CROSS_FIELD_FORMAT_HEX			= 2,  // 0xA3BF, input: 0xA3BF or 0xa3bf
	CROSS_FIELD_FORMAT_BIN			= 3,  // Binary format: b1000.0000.0110.0000
	CROSS_FIELD_FORMAT_BMP			= 4,  // bitmap format: B15~13.10~8.4.1
	CROSS_FIELD_FORMAT_ENUM			= 10, // enum type
	CROSS_FIELD_FORMAT_TIMESTAMP	= 12, // Date and time: 2019/11/15T10:26:39
	CROSS_FIELD_FORMAT_MAC			= 20, // MAC address: 00:00:1F:0a:00:AD
	CROSS_FIELD_FORMAT_IPv4			= 21, // IPv4 address: 169.254.98.1
	CROSS_FIELD_FORMAT_IPv6			= 22, // IPv6 address
} cross_field_format;

#define CROSS_NAME_LEN				64
#define CROSS_FIELD_NAME_LEN		85

typedef struct {
	uint64_t					header;
	char 						fld_name[CROSS_FIELD_NAME_LEN];
	uint8_t						fld_nmlen;
	uint8_t 					fld_type; 		// cross_field_type	
	uint8_t 					fld_format; 	// cross_field_format
	uint32_t 					fld_offset;
	uint32_t 					fld_size;		// each element size
	uint32_t 					fld_flags; 		// cross_field_flag
	uint32_t 					fld_count; 		// array count or bits count, arrary [1-3][] 10b[D3] 10b[D2] 10b[D1]
	uint16_t 					fld_bitoffset; 	// for bits type, float precision (8b.8b)
	uint16_t 					fld_ref_id; 	// reference object ID like enum/set/check
	uint32_t					fld_idxbmp;		// 
	int							fld_union_val;
	uint16_t 					fld_union_id; 
	uint16_t					fld_dsize_id; 
	union {
		uint64_t				data64;
		struct cross_schema_t	*pSchema;
	};
} cross_fld_entry;

typedef struct {
	uint64_t					header;
	int64_t						enum_val;
	char						enum_name[CROSS_NAME_LEN];
	union {
		struct cross_schema_t	*pSchema;
		uint64_t				data;
	};
} cross_enum_t;

typedef struct cross_field_t {
	cross_fld_entry				field_entry;
	struct cross_field_t		*pField;
	uint32_t					schema_count;
	cross_enum_t				*ref_obj;
	char						*obj_name;
	uint16_t					ref_count;
} cross_field_t;

#if defined(__BIG_ENDIAN__) || (defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
#define CROSS_MAGIC_NUM			0x0C0D0B80
#else
#define CROSS_MAGIC_NUM			0x800B0D0C
#endif

#define CROSS_SCHEMA_MAGIC() 	{(CROSS_MAGIC_NUM>>16), (CROSS_MAGIC_NUM&0xFFFF)}
#define CROSS_ENUM_MAGIC() 		{(CROSS_MAGIC_NUM&0xFFFF), (CROSS_MAGIC_NUM>>16)}

#define CROSS_FIELD_ALL(fld_name, type, format, offset, size, flags, bits, boffset) \
		{CROSS_SCHEMA_MAGIC(), fld_name, sizeof(fld_name), CROSS_FIELD_TYPE_##type, CROSS_FIELD_FORMAT_##format, offset, size, flags, bits, boffset, \
		0, sizeof (CROSS_STRUCT_NAME), 0, 0xffff}

#define CROSS_FLD(st_field, type, format, fld_name, flags, bits, boffset) \
		CROSS_FIELD_ALL(fld_name, type, format, offsetof(CROSS_STRUCT_NAME,st_field), sizeof(((CROSS_STRUCT_NAME*)0)->st_field), flags, bits, boffset)

#define CROSS_FIELDN(st_field, type, format, fld_name, flags) \
		{CROSS_FLD(st_field, type, format, fld_name, flags), NULL, 0}

#define CROSS_FIELD(st_field, type, format, flags)		CROSS_FIELDN(st_field, type, format, #st_field, flags)

#define CROSS_FIELD_END(name) 	{CROSS_SCHEMA_MAGIC(), name, 0,0,0,0,0,0,0,0,0, sizeof (CROSS_STRUCT_NAME)}


/******************************************************************************
	CrossDB Schema APIs
******************************************************************************/

cross_ret cross_schemaCreate (cross_schema_h *phSchema, cross_field_t *pRootField);


/******************************************************************************
	CrossDB DB APIs
******************************************************************************/

/*
#define CROSS_DB_CREATE
#define CROSS_DB_ONDISK
#define CROSS_DB_RAMDISK
#define CROSS_DB_INMEM
#define CROSS_DB_GLOBAL
#define CROSS_DB_RDONLY

#define CROSS_DB_AUTO_UPGRADE
#define CROSS_DB_AUTO_RECOVERY
*/
cross_ret cross_dbOpen (cross_db_h *phDb, const char *name, uint32_t flags);
cross_ret cross_dbClose (cross_db_h hDb, uint32_t flags);
cross_ret cross_dbDrop (cross_db_h hDb, uint32_t flags);


/******************************************************************************
	CrossDB Table APIs
******************************************************************************/

/*
#define CROSS_TBL_CREATE
#define CROSS_TBL_FIFO
#define CROSS_TBL_MINHEAP
#define CROSS_TBL_MAXHEAP
#define CROSS_TBL_AUTO_UPGRADE
#define CROSS_TBL_AUTO_RECOVERY
#define CROSS_TBL_BTREE
*/
cross_ret cross_tblOpen (cross_tbl_h *phTbl, cross_db_h hDb, const char *name, cross_schema_h hSchema, 
								const char *priKey, cross_rowid minRows, cross_rowid maxRows, uint32_t flags);
cross_ret cross_tblDrop (cross_tbl_h hTbl);


/******************************************************************************
	CrossDB Index APIs
******************************************************************************/

/*
#define CROSS_IDX_UNIQUE
#define CROSS_IDX_HASH
#define CROSS_IDX_BTREE
*/
cross_ret cross_IdxCreate (cross_tbl_h hTbl, const char *name, const char *colList, uint32_t flags);
cross_ret cross_IdxDrop (cross_tbl_h hTbl, const char *name);


/******************************************************************************
	CrossDB CURD APIs
******************************************************************************/

/*
 CROSS_AUTOLOCK
 */
cross_ret cross_dbInsertRow (cross_tbl_h hTbl, const void *pRow, uint32_t flags);

cross_ret cross_dbGetRowByPk (cross_tbl_h hTbl, const void *pInRow, void *pOutRow, uint32_t flags);

cross_ret cross_dbUpdRowByPk (cross_tbl_h hTbl, const void *pInRow, void *pUpdRow, uint32_t flags);

cross_ret cross_dbDelRowByPk (cross_tbl_h hTbl, const void *pRow, uint32_t flags);

cross_ret cross_dbGetAnyRow (cross_tbl_h hTbl, const void *pMatRow, cross_match_h matCols, void *pRow, uint32_t flags);

cross_ret cross_dbUpdRows (cross_tbl_h hTbl, const void *pMatRow, const char *matchCols, void *UpdRow, uint32_t flags);

cross_ret cross_dbDelRows (cross_tbl_h hTbl, const void *pMatRow, const char *matchCols, uint32_t flags);

cross_ret cross_dbGetRowsCount (cross_tbl_h hTbl, const void *pMatRow, const char *matCols, uint32_t flags);


/******************************************************************************
	CrossDB Cursor APIs
******************************************************************************/

/*
CROSS_SELECT_UNION
CROSS_SELECT_SNAPSHOT // no need to lock table
CROSS_SELECT_DISTINCT
 */

cross_ret cross_dbGetRows (cross_cursor_h *phCursor, cross_tbl_h hTbl, cross_match_h matCols, const void *pMatRow, 
			cross_match_h ordCols, cross_rowid offset, cross_rowid limit, uint32_t flags);

cross_ret cross_cursorGetNext (cross_cursor_h hCursor, void *pOutRow);

cross_ret cross_cursorGetBatch (cross_cursor_h hCursor, void *pOutRow, int count);

cross_ret cross_cursorGetCount (cross_cursor_h *phCursor);

// Close and free cursor
cross_ret cross_cursorFree (cross_cursor_h hCursor);


/******************************************************************************
	CrossDB Lock APIs
******************************************************************************/

cross_ret cross_tblRdLock (cross_tbl_h hTbl); 
cross_ret cross_tblRdUnLock (cross_tbl_h hTbl); 

cross_ret cross_tblWrLock (cross_tbl_h hTbl); 
cross_ret cross_tblWrUnLock (cross_tbl_h hTbl); 

cross_ret cross_tblBatchLock (cross_tbl_h wrTbl[], int wrTblNum, cross_tbl_h rdTbl[], int rdTblNum); 
cross_ret cross_tblBatchUnLock (cross_tbl_h wrTbl[], int wrTblNum, cross_tbl_h rdTbl[], int rdTblNum); 


/******************************************************************************
	CrossDB Transactions APIs
******************************************************************************/

cross_ret cross_dbTransBegin (cross_db_h hDb, uint32_t flags);
cross_ret cross_dbTransCommit (cross_db_h hDb, uint32_t flags);
cross_ret cross_dbTransRollback (cross_db_h hDb, uint32_t flags);


/******************************************************************************
	CrossDB License APIs
******************************************************************************/

// default is free license, eval mode: "trial", "free"
cross_ret cross_license (const char *license, const char *upgLic);

#ifdef __cplusplus
}
#endif

#endif // __CROSS_DB_H__

