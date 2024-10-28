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

#ifndef __XDB_TRIGGER_H__
#define __XDB_TRIGGER_H__

#define	XDB_TRIG_BEF_INS 0
#define	XDB_TRIG_AFT_INS 1
#define	XDB_TRIG_ROL_INS 2
#define	XDB_TRIG_BEF_UPD 3
#define	XDB_TRIG_AFT_UPD 4
#define	XDB_TRIG_ROL_UPD 5
#define	XDB_TRIG_BEF_DEL 6
#define	XDB_TRIG_AFT_DEL 7
#define	XDB_TRIG_ROL_DEL 8

typedef int (*xdb_trig_callback) (xdb_conn_t *pConn, uint64_t meta, uint32_t type, xdb_row_t *pNewRow, xdb_row_t *pOldRow, void *pArg);

int xdb_create_trigger (xdb_conn_t *pConn, const char *name, const char *tbl_name, 
								int type, xdb_trig_callback cb_func, void *pArg);

int xdb_call_trigger (xdb_conn_t *pConn, xdb_tblm_t *pTblm, uint32_t type, xdb_row_t *pNewRow, xdb_row_t *pOldRow);

#endif // __XDB_TRIGGER_H__
