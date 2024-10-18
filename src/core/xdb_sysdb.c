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

static xdb_conn_t *s_xdb_sysdb_pConn = NULL;

XDB_STATIC void 
xdb_sysdb_add_db (xdb_dbm_t *pDbm)
{
	if (!s_xdb_bInit) {
		return;
	}
	xdb_res_t *pRes = xdb_pexec (s_xdb_sysdb_pConn, "INSERT INTO databases (database,engine) VALUES('%s','%s')", 
		XDB_OBJ_NAME(pDbm), pDbm->bMemory?"MEMORY":"MMAP");
	if (pRes->errcode != XDB_E_NOTFOUND) {
		XDB_RESCHK(pRes, xdb_errlog("Can't add to system table database %s\n", XDB_OBJ_NAME(pDbm)));
	}
}

XDB_STATIC void 
xdb_sysdb_del_db (xdb_dbm_t *pDbm)
{
	if (!s_xdb_bInit) {
		return;
	}
	xdb_res_t *pRes = xdb_pexec (s_xdb_sysdb_pConn, "DELETE FROM databases WHERE database='%s'", XDB_OBJ_NAME(pDbm));
	if (pRes->errcode != XDB_E_NOTFOUND) {
		XDB_RESCHK(pRes, xdb_errlog("Can't del from system table database %s\n", XDB_OBJ_NAME(pDbm)));
	}
}

XDB_STATIC void 
xdb_sysdb_add_col (xdb_tblm_t *pTblm)
{
	if (!s_xdb_bInit) {
		return;
	}
	for (int i = 0; i < pTblm->fld_count; ++i) {
		xdb_field_t *pFld = &pTblm->pFields[i];
		xdb_res_t *pRes = xdb_pexec (s_xdb_sysdb_pConn, "INSERT INTO columns (database,table,column,type,len) VALUES('%s','%s','%s','%s',%d)", 
			XDB_OBJ_NAME(pTblm->pDbm), XDB_OBJ_NAME(pTblm), XDB_OBJ_NAME(pFld), xdb_type2str (pFld->fld_type), pFld->fld_len);
		if (pRes->errcode != XDB_E_NOTFOUND) {
			XDB_RESCHK(pRes, xdb_errlog("Can't add to system table column %s %s %s\n", 
				XDB_OBJ_NAME(pTblm->pDbm), XDB_OBJ_NAME(pTblm), XDB_OBJ_NAME(pFld)));
		}
	}
}

XDB_STATIC char*
xdb_fld2str (char buf[], int len, xdb_field_t *pFields[], int cnt)
{
	char *p = buf;
	for (int i = 0; i < cnt; ++i) {
		int n = sprintf (p, "%s,", XDB_OBJ_NAME(pFields[i]));
		p += n;
	}
	if (p > buf) {
		*(p - 1) = '\0';
	}
	return buf;
}

XDB_STATIC void 
xdb_sysdb_upd_tbl_schema (xdb_tblm_t *pTblm)
{
	if (!s_xdb_bInit) {
		return;
	}
	char buf[65536];
	xdb_dump_create_table (pTblm, buf, sizeof(buf), 0);
	xdb_res_t *pRes = xdb_pexec (s_xdb_sysdb_pConn, "UPDATE tables SET schema='%s' WHERE database='%s' AND table='%s'", 
		buf, XDB_OBJ_NAME(pTblm->pDbm), XDB_OBJ_NAME(pTblm));
	if (pRes->errcode != XDB_E_NOTFOUND) {
		XDB_RESCHK(pRes, xdb_errlog("Can't update system table table schema %s\n", XDB_OBJ_NAME(pTblm->pDbm)));
	}
}

XDB_STATIC void 
xdb_sysdb_add_idx (xdb_idxm_t *pIdxm)
{
	if (!s_xdb_bInit) {
		return;
	}
	char collist[255];
	xdb_tblm_t *pTblm = pIdxm->pTblm;
	xdb_fld2str (collist, sizeof(collist), pIdxm->pFields, pIdxm->fld_count);
	xdb_res_t *pRes = xdb_pexec (s_xdb_sysdb_pConn, "INSERT INTO indexes (database,table,idx_key,type,col_list) VALUES('%s','%s','%s','%s','%s')",
			XDB_OBJ_NAME(pTblm->pDbm), XDB_OBJ_NAME(pTblm), XDB_OBJ_NAME(pIdxm), "HASH", collist);
	if (pRes->errcode != XDB_E_NOTFOUND) {
		XDB_RESCHK (pRes, xdb_errlog("Can't add system table index '%s','%s','%s','%s','%s')\n", XDB_OBJ_NAME(pTblm->pDbm), XDB_OBJ_NAME(pTblm), XDB_OBJ_NAME(pIdxm), "HASH", collist));
	}
	xdb_sysdb_upd_tbl_schema (pTblm);
}

XDB_STATIC void 
xdb_sysdb_del_idx (xdb_idxm_t *pIdxm)
{
	if (!s_xdb_bInit) {
		return;
	}
	xdb_tblm_t *pTblm = pIdxm->pTblm;
	xdb_res_t *pRes = xdb_pexec (s_xdb_sysdb_pConn, "DELETE FROM indexes WHERE database='%s' AND table='%s' AND idx_key='%s'",
			XDB_OBJ_NAME(pTblm->pDbm), XDB_OBJ_NAME(pTblm), XDB_OBJ_NAME(pIdxm));
	XDB_RESCHK (pRes);
	xdb_sysdb_upd_tbl_schema (pTblm);
}

XDB_STATIC void 
xdb_sysdb_add_tbl_idx (xdb_tblm_t *pTblm)
{
	if (!s_xdb_bInit) {
		return;
	}
	int count = XDB_OBJM_MAX(pTblm->idx_objm);
	for (int i = 0; i < count; ++i) {
		xdb_idxm_t *pIdxm = XDB_OBJM_GET(pTblm->idx_objm, i);
		if (NULL != pIdxm) {
			xdb_sysdb_add_idx (pIdxm);
		}
	}
}

XDB_STATIC void 
xdb_sysdb_del_col (xdb_tblm_t *pTblm)
{
	if (!s_xdb_bInit) {
		return;
	}
	for (int i = 0; i < pTblm->fld_count; ++i) {
		xdb_field_t *pFld = &pTblm->pFields[i];
		xdb_res_t *pRes = xdb_pexec (s_xdb_sysdb_pConn, "DELETE FROM columns WHERE database='%s' AND table='%s' AND column='%s'", 
			XDB_OBJ_NAME(pTblm->pDbm), XDB_OBJ_NAME(pTblm), XDB_OBJ_NAME(pFld));
		XDB_RESCHK(pRes);
	}
}

XDB_STATIC void 
xdb_sysdb_add_tbl (xdb_tblm_t *pTblm, bool bAddTbl, bool bAddCol, bool bAddIdx)
{
	if (!s_xdb_bInit) {
		return;
	}
	if (bAddTbl) {
		xdb_res_t *pRes = xdb_pexec (s_xdb_sysdb_pConn, "INSERT INTO tables (database,table,engine) VALUES('%s','%s','%s')", 
			XDB_OBJ_NAME(pTblm->pDbm), XDB_OBJ_NAME(pTblm), pTblm->bMemory?"MEMORY":"MMAP");
		if (pRes->errcode != XDB_E_NOTFOUND) {
			XDB_RESCHK(pRes, xdb_errlog("Can't add system table table %s\n", XDB_OBJ_NAME(pTblm)));
		}
	}
	if (bAddCol) {
		xdb_sysdb_add_col (pTblm);
	}
	if (bAddIdx) {
		xdb_sysdb_add_tbl_idx (pTblm);
	}
	xdb_sysdb_upd_tbl_schema (pTblm);
}

XDB_STATIC void 
xdb_sysdb_del_tbl (xdb_tblm_t *pTblm)
{
	if (!s_xdb_bInit) {
		return;
	}
	xdb_sysdb_del_col (pTblm);
	xdb_res_t *pRes = xdb_pexec (s_xdb_sysdb_pConn, "DELETE FROM tables WHERE database='%s' AND table='%s'", XDB_OBJ_NAME(pTblm->pDbm), XDB_OBJ_NAME(pTblm));
	XDB_RESCHK(pRes);
}

XDB_STATIC int 
xdb_sysdb_init ()
{
	if (NULL != s_xdb_sysdb_pConn) {
		return XDB_OK;
	}
	xdb_conn_t *pConn  = xdb_calloc (sizeof (xdb_conn_t));
	if (NULL == pConn) {
		return XDB_E_MEMORY;
	}
	xdb_conn_init (pConn);
	s_xdb_sysdb_pConn = pConn;

	xdb_res_t *pRes = xdb_exec (pConn, "CREATE DATABASE IF NOT EXISTS system ENGINE=MEMORY");
	XDB_RESCHK(pRes, xdb_errlog ("Can't create system database\n"));

	pRes = xdb_exec (pConn, "CREATE TABLE IF NOT EXISTS columns (database CHAR(64), table CHAR(64), column CHAR(64), type CHAR(16), len INT, PRIMARY KEY (database,table,column))");
	// will update tables which doesn't exist yet
	if (pRes->errcode != XDB_E_NOTFOUND) {
		XDB_RESCHK(pRes, xdb_errlog ("Can't create system table columns\n"));
	}

	// will update indexes which doesn't exist yet
	pRes = xdb_exec (pConn, "CREATE TABLE IF NOT EXISTS tables (database CHAR(64), table CHAR(64), engine CHAR(8), primary_key VARCHAR(1024), total_rows INT, data_path VARCHAR(255), schema VARCHAR(65535), PRIMARY KEY (database,table))");
	if (pRes->errcode != XDB_E_NOTFOUND) {
		XDB_RESCHK(pRes, xdb_errlog ("Can't create system table tables\n"));
	}

	pRes = xdb_exec (pConn, "CREATE TABLE IF NOT EXISTS indexes (database CHAR(64), table CHAR(64), idx_key CHAR(64), type CHAR(8), col_list VARCHAR(255), PRIMARY KEY (database,table,idx_key))");
	XDB_RESCHK(pRes, xdb_errlog ("Can't create system table indexes\n"));

	xdb_tblm_t *pTblm = xdb_find_table (s_xdb_sysdb_pConn->pCurDbm, "columns");
	if (NULL != pTblm) {
		xdb_sysdb_add_tbl (pTblm, true, false, true);
	}
	pTblm = xdb_find_table (s_xdb_sysdb_pConn->pCurDbm, "tables");
	if (NULL != pTblm) {
		xdb_sysdb_add_tbl (pTblm, false, false, true);
	}

	pRes = xdb_exec (pConn, "CREATE TABLE IF NOT EXISTS databases (database CHAR(64), engine CHAR(8), data_path VARCHAR(1024), PRIMARY KEY (database))");
	XDB_RESCHK(pRes, xdb_errlog ("Can't create system table databases\n"));

	xdb_sysdb_add_db (pConn->pCurDbm);

	//xdb_exec (pConn, "CREATE DATABASE IF NOT EXISTS information_schema ENGINE=MEMORY");
	//xdb_exec (pConn, "CREATE TABLE IF NOT EXISTS SCHEMATA (catalog_name CHAR(3), schema_name CHAR(64), schema_owner CHAR(7), default_character_set_catalog CHAR(1), default_character_set_schema CHAR(1), default_character_set_name CHAR(1), sql_path CHAR(1), data_path VARCHAR(255), engine CHAR(8))");

	return XDB_OK;
}

XDB_STATIC void 
xdb_sysdb_exit ()
{
	xdb_close (s_xdb_sysdb_pConn);
	s_xdb_sysdb_pConn = NULL;
}
