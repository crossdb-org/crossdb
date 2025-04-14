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

#ifndef __XDB_FKEY_H__
#define __XDB_FKEY_H__

typedef struct xdb_fkeym_t {
	xdb_obj_t		obj;
	struct xdb_tblm_t *pTblm;
	struct xdb_tblm_t *pRefTblm;
	int				fld_count;
	xdb_fkey_action	on_del_act;
	xdb_fkey_action	on_upd_act;
	//xdb_field_t		*pFields[XDB_MAX_MATCH_COL];
	xdb_singfilter_t	filter;
} xdb_fkeym_t;

#endif // __XDB_FKEY_H__
