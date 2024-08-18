/******************************************************************************
* Copyright (c) 2024-present JC Wang. All rights reserved
*
*   https://crossdb.org
*   https://github.com/crossdb-org/crossdb
*
* This program is free software: you can redistribute it and/or modify it under
* the terms of the GNU General Public License, version 3 or later, as published 
* by the Free Software Foundation.
*
* This program is distributed in the hope that it will be useful, but WITHOUT
* ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS 
* FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License along with 
* this program. If not, see <http://www.gnu.org/licenses/>.
******************************************************************************/

static xdb_objm_t	s_xdb_db_list;

static char			s_xdb_datadir[XDB_PATH_LEN + 1];

XDB_STATIC xdb_dbm_t* 
xdb_find_db (const char *db_name)
{
	return xdb_objm_get (&s_xdb_db_list, db_name);
}

XDB_STATIC int 
xdb_use_db (xdb_stmt_db_t *pStmt)
{
	xdb_conn_t		*pConn = pStmt->pConn;
	//xdb_dbglog ("use db '%s'\n", pStmt->db_name);
	pConn->pCurDbm = xdb_find_db (pStmt->db_name);
	XDB_EXPECT_RETE(NULL != pConn->pCurDbm, XDB_E_NOTFOUND, "Unknown database '%s'", pStmt->db_name);
	xdb_strcpy (pConn->cur_db, pStmt->db_name);
	return XDB_OK;
}

XDB_STATIC int 
xdb_open_datadir (xdb_stmt_db_t *pStmt)
{
	char		*datadir = pStmt->db_name;
    DIR 		*d = NULL;
    struct 		dirent *dp = NULL;
    struct 		stat st;    
    char 		dbpath[XDB_PATH_LEN + 1] = {0};
	int			rc;

    if(stat(datadir, &st) < 0 || !S_ISDIR(st.st_mode)) {
        xdb_errlog ("Invalid datadir: %s\n", datadir);
        return XDB_E_NOTFOUND;
    }

    if(!(d = opendir (datadir))) {
        xdb_errlog ("Can't opendir[%s]\n", datadir);
        return XDB_E_NOTFOUND;
    }
	
    while ((dp = readdir(d)) != NULL) {
        if ((!strncmp(dp->d_name, ".", 1)) || (!strncmp(dp->d_name, "..", 2))) {
            continue;
        }
        snprintf (dbpath, sizeof(dbpath), "%s/%s", datadir, dp->d_name);
		// snprintf on Linux will force last '\0', while Windows doesn't
		dbpath[sizeof(dbpath)-1] = '\0';
        rc = stat (dbpath, &st);
		if (rc < 0) {
			continue;
		}
        if (S_ISDIR(st.st_mode)) {
			xdb_res_t *pRes = xdb_pexec (pStmt->pConn, "OPEN DATABASE '%s'", dbpath);
			if (XDB_OK != pRes->errcode) {
				xdb_errlog ("ERROR %d: %s\n", pRes->errcode, xdb_errmsg(pRes));
			}
        }
    }

    closedir (d);
	return XDB_OK;
}

XDB_STATIC int 
xdb_create_db (xdb_stmt_db_t *pStmt)
{
	if (NULL != pStmt->pDbm) {
		return XDB_OK;
	}

	xdb_dbm_t *pDbm = NULL;

	char db_path[XDB_PATH_LEN + XDB_NAME_LEN + 1];
	char *real_db_name = pStmt->db_name;
	xdb_conn_t		*pConn = pStmt->pConn;
	char *db_name = strrchr (real_db_name, '/');
	if (NULL == db_name) {
		db_name = strrchr (real_db_name, '\\');
	}
	if ((NULL != db_name) && ('\0' == db_name[1])) {
		*db_name = '\0';
		db_name = strrchr (real_db_name, '/');
		if (NULL == db_name) {
			db_name = strrchr (real_db_name, '\\');
		}
	}

	XDB_EXPECT ((0 == pConn->sockfd) || (NULL == db_name), XDB_E_AUTH, "Can't create database '%s' with path, not allowed in server mode", db_name);
	if (NULL == db_name) {
		db_name = real_db_name;
		if (*s_xdb_datadir) {
			xdb_sprintf (db_path, "%s/%s", s_xdb_datadir, db_name);
			real_db_name = db_path;
		}
	} else {
		db_name++;
	}

	xdb_dbm_t *pDbm2 = xdb_find_db (db_name);
	if (pStmt->bIfExistOrNot && (NULL != pDbm2)) {
		return XDB_OK;
	}
	XDB_EXPECT (NULL==pDbm2, XDB_E_EXISTS, "Can't create database '%s', database exists", db_name);

	XDB_EXPECT (XDB_OBJM_COUNT (s_xdb_db_list) < XDB_MAX_DB, XDB_E_FULL, "Can create/open at most %d databases", XDB_MAX_DB);

	char path[XDB_PATH_LEN * 2];
	xdb_sprintf (path, "%s/xdb.db", real_db_name);

	if (XDB_STMT_OPEN_DB == pStmt->stmt_type) {
		XDB_EXPECT (xdb_fexist(path), XDB_E_NOTFOUND, "Can't open database '%s', database doesn't exist", db_name);
	}

	pDbm = xdb_calloc (sizeof(xdb_dbm_t));
	XDB_EXPECT (NULL!=pDbm, XDB_E_MEMORY, "Can't alloc memory");
	pDbm->bMemory = pStmt->bMemory;

	xdb_stghdr_t stg_hdr = {.stg_magic = 0xE7FCFDFB, .blk_flags=1, .blk_size = sizeof(xdb_dbobj_t), 
							.ctl_off = 0, .blk_off = XDB_OFFSET(xdb_dbHdr_t, dbobj)};
	pDbm->stg_mgr.pOps = pDbm->bMemory ? &s_xdb_store_mem_ops : &s_xdb_store_file_ops;
	pDbm->stg_mgr.pStgHdr	= &stg_hdr;
	int rc = xdb_stg_open (&pDbm->stg_mgr, path, NULL, NULL);
	pDbm->lock_mode = pStmt->lock_mode;

	xdb_dbHdr_t *pDbHdr = (xdb_dbHdr_t*)pDbm->stg_mgr.pStgHdr;
	if (XDB_OK == rc) {
		pDbHdr->lock_mode = pStmt->lock_mode;
	} else if (0 == pDbm->lock_mode) {
		pDbm->lock_mode = pDbHdr->lock_mode;
	}

	XDB_EXPECT (strlen(real_db_name) < sizeof (pDbm->db_path), XDB_E_PARAM, "Too long path");
	xdb_strcpy (pDbm->db_path, real_db_name);

	xdb_strcpy (XDB_OBJ_NAME(pDbm), db_name);
	XDB_OBJ_ID(pDbm) = -1;

	xdb_objm_add (&s_xdb_db_list, pDbm);

	if (NULL == pConn->pCurDbm) {
		pConn->pCurDbm = pDbm;
		xdb_strcpy (pConn->cur_db, db_name);
	}
	if (!pDbm->bMemory) {
		xdb_sprintf (path, "%s/xdb.xql", real_db_name);
		if (xdb_fexist (path)) {
			xdb_source (pConn, path);
		}
	}

	if (strcmp (XDB_OBJ_NAME(pDbm), "system")) {
		xdb_sysdb_add_db (pDbm);
	} else {
		pDbm->bSysDb = true;
	}

	return 0;

error:
	xdb_free (pDbm);
	return -pConn->conn_res.errcode;
}

XDB_STATIC int 
xdb_close_db (xdb_conn_t *pConn, xdb_dbm_t *pDbm)
{
	char path[XDB_PATH_LEN + 32];
	xdb_sprintf (path, "%s/xdb.db", pDbm->db_path);

	xdb_dbglog ("------ close db '%s' ------\n", XDB_OBJ_NAME(pDbm));

	int count = XDB_OBJM_MAX(pDbm->db_objm);
	for (int i = 0; i < count; ++i) {
		xdb_tblm_t *pTblm = XDB_OBJM_GET(pDbm->db_objm, i);
		if (NULL != pTblm) {
			xdb_close_table (pTblm);
		}
	}

	xdb_stg_close (&pDbm->stg_mgr);

	xdb_objm_del (&s_xdb_db_list, pDbm);

	if ((NULL != pConn) && pDbm == pConn->pCurDbm) {
		pConn->pCurDbm = NULL;
	}
	if (!pDbm->bMemory) {
		// remove DB folder
		//xdb_rmdir (pDbm->db_path);
		xdb_dbglog ("rmdir %s\n", pDbm->db_path);
	}

	if (strcmp (XDB_OBJ_NAME(pDbm), "system")) {
		xdb_sysdb_del_db (pDbm);
	}

	xdb_objm_free (&pDbm->db_objm);

	xdb_free (pDbm);

	return 0;
}

XDB_STATIC int 
xdb_drop_db (xdb_conn_t *pConn, xdb_dbm_t *pDbm)
{
	char path[XDB_PATH_LEN + 32];
	xdb_sprintf (path, "%s/xdb.db", pDbm->db_path);

	int count = XDB_OBJM_MAX(pDbm->db_objm);
	for (int i = 0; i < count; ++i) {
		xdb_tblm_t *pTblm = XDB_OBJM_GET(pDbm->db_objm, i);
		if (NULL != pTblm) {
			xdb_drop_table (pTblm);
		}
	}

	xdb_stg_drop (&pDbm->stg_mgr, path);

	xdb_objm_del (&s_xdb_db_list, pDbm);

	if ((NULL != pConn) && pDbm == pConn->pCurDbm) {
		pConn->pCurDbm = NULL;
	}
	if (!pDbm->bMemory) {
		// remove DB folder
		xdb_rmdir (pDbm->db_path);
	}

	if (strcmp (XDB_OBJ_NAME(pDbm), "system")) {
		xdb_sysdb_del_db (pDbm);
	}

	xdb_free (pDbm);

	return 0;
}

XDB_STATIC void 
xdb_close_all_db (xdb_conn_t *pConn)
{
	xdb_dbglog ("====== close all db ======\n");
	
	for (int i = 0; i < XDB_OBJM_MAX(s_xdb_db_list); ++i) {
		xdb_dbm_t *pDbm = XDB_OBJM_GET(s_xdb_db_list, i);
		if (NULL != pDbm) {
			xdb_close_db (pConn, pDbm);
		}
	}
	xdb_objm_free (&s_xdb_db_list);
}

XDB_STATIC int 
xdb_dump_db_schema (xdb_dbm_t *pDbm, const char *out)
{
	FILE *pFile = fopen (out, "w");

	for (int i = 0; i < XDB_OBJM_MAX(pDbm->db_objm); ++i) {
		xdb_tblm_t *pTblm = XDB_OBJM_GET(pDbm->db_objm, i);
		if (NULL != pTblm) {
			char buf[65536];
			int schema_len = xdb_dump_create_table (pTblm, buf, sizeof(buf), 1);
			int len = fwrite (buf, 1, schema_len, pFile);
			if (len  != schema_len) {
				xdb_errlog ("Failed to write schema file\n");
			}
		}
	}

	xdb_fclose (pFile);
	return XDB_OK;
}

XDB_STATIC int 
xdb_gen_db_schema (xdb_dbm_t *pDbm)
{
	if (!pDbm->bMemory) {
		char file[XDB_PATH_LEN + 32];
		sprintf (file, "%s/xdb.xql", pDbm->db_path);
		xdb_dump_db_schema (pDbm, file);
	}
	return 0;
}