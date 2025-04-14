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

int xdb_call_trigger (xdb_conn_t *pConn, xdb_tblm_t *pTblm, xdb_trig_e type, xdb_row_t *pNewRow, xdb_row_t *pOldRow);

#endif // __XDB_TRIGGER_H__
