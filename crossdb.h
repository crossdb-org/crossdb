/*
 * Copyright 2022-2023 CrossDB.ORG. All rights reserved.
 *
 *   https://crossdb.org
 *   https://github.com/crossdb-org/crossdb
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef __CROSS_DB_H__
#define __CROSS_DB_H__

#include <inttypes.h>
#include <stdbool.h>
#include <stddef.h>


#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
	CrossDB Return Codes
******************************************************************************/

typedef enum {
	CROSS_OK 			= 0,
	CROSS_E_HANDLE		= -1,
	CROSS_E_PARAM 		= -2,
	CROSS_E_NOTFOUND 	= -4,
	CROSS_E_EXISTS 		= -5,
	CROSS_E_MEMORY		= -20,
} cross_ret;

const char *cross_errMsg (cross_ret ret);


/******************************************************************************
	CrossDB Types
******************************************************************************/

typedef int				cross_rowid;
typedef void*			cross_db_h;
typedef void*			cross_tbl_h;
typedef void*			cross_cursor_h;
typedef void*			cross_fields_h;
typedef void*			cross_match_h;


/******************************************************************************
	CrossDB Schema Types
******************************************************************************/

typedef enum {
	CROSS_FLDTYPE_INT 			= 0, // integer: for char, short, int, long long
	CROSS_FLDTYPE_UINT			= 1, // unsigned integer
	CROSS_FLDTYPE_FLOAT			= 2, // float and double
	CROSS_FLDTYPE_CHAR			= 3, // C string
	CROSS_FLDTYPE_BYTE			= 4, // byte arrary (unsigned char)
	CROSS_FLDTYPE_STRUCT		= 7, // C Struct
} cross_fldType_e;

typedef enum {
	CROSS_FLDFMT_DFT 			= 0,  // INT/UINT: decimal, BIN: hex
	CROSS_FLDFMT_BOOL			= 1,  // TRUE/FALSE, input: 0/1 or T/F or t/f
	CROSS_FLDFMT_HEX			= 2,  // 0xA3BF, input: 0xA3BF or 0xa3bf
	CROSS_FLDFMT_BIN			= 3,  // Binary format: b1000.0000.0110.0000
	CROSS_FLDFMT_BMP			= 4,  // bitmap format: B15~13.10~8.4.1
	CROSS_FLDFMT_TIMESTAMP		= 12, // Date and time: 2019/11/15T10:26:39
	CROSS_FLDFMT_TS				= CROSS_FLDFMT_TIMESTAMP,
	CROSS_FLDFMT_MAC			= 20, // MAC address: 00:00:1F:0a:00:AD
	CROSS_FLDFMT_IPv4			= 21, // IPv4 address: 169.254.98.1
	CROSS_FLDFMT_IPv6			= 22, // IPv6 address
} cross_fldFmt_e;

#define CROSS_NAME_LEN			64
#define CROSS_FLDNAME_LEN		85

typedef struct {
	uint16_t	magic_code;
	uint16_t	magic_code2;
	uint16_t	prev;
	uint16_t	next;
} cross_list_node;

typedef struct {
	cross_list_node				list_node;
	char 						fld_name[CROSS_FLDNAME_LEN];
	uint8_t						fld_priv;
	uint8_t 					fld_type; 		// cross_fldType_e
	uint8_t 					fld_format; 	// cross_fldFmt_e
	uint32_t 					fld_offset;
	uint32_t 					fld_size;		// each element size
	uint32_t 					fld_flags; 		// cross_field_flag
	uint32_t 					fld_count; 		// array count or bits count, arrary [1-3][] 10b[D3] 10b[D2] 10b[D1]
	uint16_t 					fld_bitoffset; 	// for bits type, float precision (8b.8b)
	uint16_t 					fld_priv1;
	uint32_t					fld_priv2;
	int							fld_priv3;
	uint16_t 					fld_priv4; 
	uint16_t					fld_priv5; 
	union {
		uint64_t				data64;
		void					*pSchema;
	} priv6;
} cross_fldEntry_t;

typedef struct cross_field_t {
	cross_fldEntry_t			field_entry;
	struct cross_field_t		*pField;
	uint32_t					rsvd;
	void						*ref_obj;
	char						*obj_name;
	uint16_t					rsvd2;
} cross_field_t;


/******************************************************************************
	CrossDB Schema Macros
******************************************************************************/

#if defined(__BIG_ENDIAN__) || (defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
#define CROSS_MAGIC_NUM			0xCDBCDBCD
#else
#define CROSS_MAGIC_NUM			0xCDDBBCCD
#endif

#define CROSS_SCHEMA_MAGIC() 	{(CROSS_MAGIC_NUM>>16), (CROSS_MAGIC_NUM&0xFFFF)}
#define CROSS_ENUM_MAGIC() 		{(CROSS_MAGIC_NUM&0xFFFF), (CROSS_MAGIC_NUM>>16)}

#define __CROSS_FIELD_ENTRY(fld_name, type, format, offset, size, flags, bits, boffset) \
		{CROSS_SCHEMA_MAGIC(), fld_name, sizeof(fld_name), CROSS_FLDTYPE_##type, CROSS_FLDFMT_##format, \
			offset, size, flags, bits, boffset, 0, sizeof (CROSS_STRUCT_NAME), 0, 0xffff}

#define __CROSS_FIELD(st_field, type, format, fld_name, flags, bits, boffset) \
		__CROSS_FIELD_ENTRY(fld_name, type, format, offsetof(CROSS_STRUCT_NAME,st_field), \
						sizeof(((CROSS_STRUCT_NAME*)0)->st_field), flags, bits, boffset)

#define CROSS_FIELD2(st_field, type, format, fld_name, flags) \
		{__CROSS_FIELD(st_field, type, format, fld_name, flags, 0, 0), NULL, 0}

#define CROSS_FIELD(st_field, type, format, flags)		CROSS_FIELD2(st_field, type, format, #st_field, flags)

#define CROSS_FLDEND(name) 		{CROSS_SCHEMA_MAGIC(), name, 0,0,0,0,0,0,0,0,0, sizeof (CROSS_STRUCT_NAME)}
#define CROSS_END(name) 		{CROSS_FLDEND(#name), NULL, 0}


/******************************************************************************
	CrossDB DB Flags
******************************************************************************/

/* DB DDL flags
 */
// DB Storage mode, default is CROSS_ONDISK
#define	CROSS_ONDISK 			(0<<0)	// DB is on persistent disk, survive with power cycle
#define	CROSS_RAMDISK			(1<<0)	// DB is on ramdisk/tmpfs/ramfs, survive with process restart, lose after power cycle
#define	CROSS_INMEM 			(2<<0)	// DB is in memory, survie when process is runnig, lose after process terminates

// DB Access mode, default is CROSS_EXCLUSIVE
#define	CROSS_EXCLUSIVE 		(0<<2)	// DB is used exclusively by single process
#define	CROSS_SHARED 			(1<<2)	// DB is shared by multiple processes

// DB Lock mode, default is CROSS_AUTOLOCK
#define	CROSS_AUTOLOCK 			(0<<3)	// DB will do lock automatically
//#define	CROSS_NOLOCK 		(1<<3)	// TBD: User is responsible for call locking APIs

#define	CROSS_OPEN				(1<<4)	// don't create if not exist

// Index Type
#define	CROSS_HASH				(0<<8)	// hash index
#define	CROSS_RBTREE			(1<<8)	// rbtree index
// Unique index
#define	CROSS_UNIQUE			(1<<11)	// unique index


/* DB DML flags
 */
#define	CROSS_REUSE				(1<<16)	// reuse handle


/******************************************************************************
	CrossDB DDL(Data Definition Language) APIs
******************************************************************************/

// Create/Open database
cross_ret cross_dbCreate (cross_db_h *phDb, const char *dbName, uint32_t flags);

// Close database
cross_ret cross_dbClose (cross_db_h hDb, uint32_t flags);

// Drop database
cross_ret cross_dbDrop (cross_db_h hDb, uint32_t flags);

// Create/Open table
cross_ret cross_dbTblCreate (cross_db_h hDb, cross_tbl_h *phTbl, const char *tblName, 
	const cross_field_t *pFields, const char *priKey, uint32_t flags);

// Drop table
cross_ret cross_dbTblDrop (cross_tbl_h hTbl, uint32_t flags);

// Create index
cross_ret cross_dbIdxCreate (cross_tbl_h hTbl, const char *idxName, const char *fldsStr, uint32_t flags);

// Drop Index
cross_ret cross_dbIdxDrop (cross_tbl_h hTbl, const char *idxName, uint32_t flags);


/******************************************************************************
	CrossDB DML(Data Manipulation Language) APIs
******************************************************************************/

// Insert row to table
cross_ret cross_dbInsertRow (cross_tbl_h hTbl, void *pRow, uint32_t flags);

// Replace row if exists then replace else update
cross_ret cross_dbReplaceRow (cross_tbl_h hTbl, void *pInRow, uint32_t flags);

// Get row by Primary Key
cross_ret cross_dbGetRowByPK (cross_tbl_h hTbl, const void *pInRow, void *pOutRow, uint32_t flags);

// Update row by Primary Key
cross_ret cross_dbUpdRowByPK (cross_tbl_h hTbl, const void *pInRow, const void *pUpdFlds, void *pUpdRow, uint32_t flags);

// Delete row by Primary Key
cross_ret cross_dbDelRowByPK (cross_tbl_h hTbl, void *pInRow, uint32_t flags);

// Get one row by match filter
cross_ret cross_dbGetOneRow (cross_tbl_h hTbl, const void *pMatFlds, const void *pMatRow, void *pOutRow, uint32_t flags);

// Update rows by match filter
cross_rowid cross_dbUpdateRows (cross_tbl_h hTbl, const void *pMatFlds, const void *pMatRow, 
											const void *pUpdFlds, const void *pUpdRow, uint32_t flags);

// Delete rows by match filter
cross_rowid cross_dbDeleteRows (cross_tbl_h hTbl, const void *pMatFlds, const void *pMatRow, uint32_t flags);

// Get rows count by match filter
cross_rowid cross_dbGetRowsCount (cross_tbl_h hTbl, const void *pMatFlds, const void *pMatRow, uint32_t flags);


/******************************************************************************
	CrossDB Cursor APIs
******************************************************************************/

// Query rows and return rows in cursor
cross_rowid cross_dbQueryRows (cross_tbl_h hTbl, cross_cursor_h *phCursor, const void *pMatFlds, const void *pMatRow, uint32_t flags);

// Get cursor rows count
cross_rowid cross_cursorGetCount (cross_cursor_h hCursor, uint32_t flags);

// Close and free cursor
cross_ret cross_cursorClose (cross_cursor_h hCursor, uint32_t flags);

// Get next row in cursor
cross_ret cross_cursorGetNextRow (cross_cursor_h hCursor, void *pOutRow, uint32_t flags);


/******************************************************************************
	CrossDB Transaction APIs
******************************************************************************/

// Begin Transaction
cross_ret cross_dbTransBegin (cross_db_h hDb, uint32_t flags);

// Commit Transaction
cross_ret cross_dbTransCommit (cross_db_h hDb, uint32_t flags);

// Rollback Transaction

cross_ret cross_dbTransRollback (cross_db_h hDb, uint32_t flags);


/******************************************************************************
	CrossDB Misc APIs
******************************************************************************/

cross_ret	cross_fieldsCreate (cross_tbl_h hTbl, cross_fields_h *phFlds, const char *FldsStr, uint32_t flags);
void		cross_fieldsFree (cross_fields_h hFlds);

cross_ret	cross_matchCreate (cross_tbl_h hTbl, cross_match_h *phMatch, const char *MatchStr, uint32_t flags);
void		cross_matchFree (cross_match_h hMatch);

#ifdef __cplusplus
}
#endif

#endif // __CROSS_DB_H__

