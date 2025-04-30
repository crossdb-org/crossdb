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

#if XDB_LOG_FLAGS & XDB_LOG_DB
#define xdb_dblog(...)	xdb_print(__VA_ARGS__)
#else
#define xdb_dblog(...)
#endif

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
	char		datadir[XDB_PATH_LEN + 1];;
    DIR 		*d = NULL;
    struct 		dirent *dp = NULL;
    struct 		stat st;    
    char 		dbpath[XDB_PATH_LEN + 256 + 1] = {0};
	int			rc;

	xdb_strcpy (datadir, pStmt->db_name);
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
		xdb_dblog ("Open DB '%s'\n", dbpath);
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

XDB_STATIC void
xdb_init_db (xdb_stgmgr_t *pStgMgr, void *pArg, xdb_size fsize)
{
	xdb_db_t *pDb = (xdb_db_t*)pStgMgr->pStgHdr;
	xdb_dbm_t *pDbm = pArg;
	if (0 == fsize) {
		pDb->lock_mode = pDbm->lock_mode;
		pDb->sync_mode = pDbm->sync_mode;
	} else {
		pDbm->lock_mode = pDb->lock_mode;
		pDbm->sync_mode = pDb->sync_mode;
	}
}

XDB_STATIC int 
xdb_create_db (xdb_stmt_db_t *pStmt)
{
	xdb_conn_t		*pConn = pStmt->pConn;
	xdb_dbm_t 		*pDbm = NULL;

	if (NULL != pStmt->pDbm) {
		if (NULL == pConn->pCurDbm) {
			XDB_EXPECT (strlen(XDB_OBJ_NAME(pStmt->pDbm)) <= XDB_NAME_LEN, XDB_E_STMT, "DB name too long"); 
			xdb_strcpy (pConn->cur_db, XDB_OBJ_NAME(pStmt->pDbm));
			pConn->pCurDbm = pStmt->pDbm;
		}
		return XDB_OK;
	}

	char db_path[XDB_PATH_LEN + XDB_NAME_LEN + 1];
	char *real_db_name = pStmt->db_name;
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

	xdb_stghdr_t stg_hdr = {.stg_magic = 0xE7FCFDFB, .blk_flags=XDB_STG_NOALLOC, .blk_size = sizeof(xdb_dbobj_t), 
							.ctl_off = 0, .blk_off = XDB_OFFSET(xdb_db_t, dbobj)};
	pDbm->stg_mgr.pOps = pDbm->bMemory ? &s_xdb_store_mem_ops : &s_xdb_store_file_ops;
	pDbm->stg_mgr.pStgHdr	= &stg_hdr;
	int rc = xdb_stg_open (&pDbm->stg_mgr, path, xdb_init_db, pDbm);
	XDB_EXPECT (XDB_OK == rc, XDB_E_FILE, "Can't create DB in path '%s'", path);

	pDbm->lock_mode = pStmt->lock_mode;
	pDbm->sync_mode = pStmt->sync_mode;

	XDB_RWLOCK_INIT(pDbm->db_lock);

	xdb_db_t *pDb = (xdb_db_t*)pDbm->stg_mgr.pStgHdr;

	XDB_EXPECT (strlen(real_db_name) < sizeof (pDbm->db_path), XDB_E_PARAM, "Too long path");
	xdb_strcpy (pDbm->db_path, real_db_name);
	if (!pDbm->bMemory) {
		char pwd[XDB_PATH_LEN+1];
		xdb_chdir (real_db_name, pwd);
		xdb_getcwd (pDbm->db_path);
		xdb_chdir (pwd, NULL);
	}

	xdb_strcpy (XDB_OBJ_NAME(pDbm), db_name);
	XDB_OBJ_ID(pDbm) = -1;

	XDB_RWLOCK_INIT(pDbm->wal_lock);

	xdb_wal_create (pConn, pDbm);

	xdb_objm_add (&s_xdb_db_list, pDbm);

	if (NULL == pConn->pCurDbm) {
		pConn->pCurDbm = pDbm;
		xdb_strcpy (pConn->cur_db, db_name);
	}
	if (!pDbm->bMemory) {
		xdb_sprintf (path, "%s/xdb.xql", real_db_name);
		if (xdb_fexist (path)) {
			xdb_dbm_t*	pCurDbm = pConn->pCurDbm;
			pConn->pCurDbm = pDbm;
			xdb_source (pConn, path);
			pConn->pCurDbm = pCurDbm;
		}
	}

	if ((pDb->flush_time < xdb_timestamp()-xdb_uptime()) && (pDb->lastchg_id != pDb->flush_id)) {
		if (s_xdb_cli) {
			xdb_errlog ("'%s' is broken, run 'REPAIR DATABASE to repaire DB'\n", XDB_OBJ_NAME(pDbm));
		} else {
			xdb_errlog ("Database '%s' was broken, repaire now\n", XDB_OBJ_NAME(pDbm));
			xdb_repair_db (pDbm, 0);
		}
	}

	if (strcmp (XDB_OBJ_NAME(pDbm), "system")) {
		xdb_sysdb_add_db (pDbm);
	} else {
		pDbm->bSysDb = true;
	}

	return XDB_OK;

error:
	xdb_free (pDbm);
	return -pConn->conn_res.errcode;
}

XDB_STATIC int 
xdb_close_db (xdb_conn_t *pConn, xdb_dbm_t *pDbm)
{
	char path[XDB_PATH_LEN + 32];
	xdb_sprintf (path, "%s/xdb.db", pDbm->db_path);

	xdb_dblog ("------ close db '%s' ------\n", XDB_OBJ_NAME(pDbm));
	xdb_yield ();

	xdb_flush_db (pDbm, 0);

	int count = XDB_OBJM_MAX(pDbm->db_objm);
	for (int i = 0; i < count; ++i) {
		xdb_tblm_t *pTblm = XDB_OBJM_GET(pDbm->db_objm, i);
		if (NULL != pTblm) {
			xdb_close_table (pTblm);
		}
	}

	xdb_wal_close (pDbm);

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

	xdb_wal_drop (pDbm);

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
			xdb_dbglog ("====== close db %s ======\n", XDB_OBJ_NAME(pDbm));
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
			int schema_len = xdb_dump_create_table (pTblm, buf, sizeof(buf), XDB_DUMP_XOID);
			buf[schema_len++]='\n';
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

XDB_STATIC int 
xdb_flush_db (xdb_dbm_t *pDbm, uint32_t flags)
{
	if (NULL == pDbm || pDbm->bMemory) {
		return XDB_OK;
	}

	xdb_wrlock_db (pDbm);

	xdb_db_t *pDb = XDB_DBPTR(pDbm);
	
	if (pDb->flush_id == pDb->lastchg_id) {
		goto exit;
	}

	xdb_dblog ("Flush DB '%s' %"PRIu64" -> %"PRIu64"\n", XDB_OBJ_NAME(pDbm), pDb->flush_id, pDb->lastchg_id);

	pDb->lastchg_id = pDb->flush_id;

	// Switch wal if has commit, then flush tables, afterward backup wal can be recycled
	bool bSwitch = xdb_wal_switch (pDbm);

	// flush tables
	int count = XDB_OBJM_MAX(pDbm->db_objm);
	for (int i = 0; i < count; ++i) {
		xdb_tblm_t *pTblm = XDB_OBJM_GET(pDbm->db_objm, i);
		if ((NULL != pTblm)) {
			xdb_flush_table (pTblm, flags);
		}
	}

	// flush backup wal if switched
	if (bSwitch) {
		xdb_wal_wrlock (pDbm);
		__xdb_wal_flush (pDbm->pWalmBak, false);
		if (XDB_WAL_PTR(pDbm->pWalm)->commit_size > sizeof (xdb_wal_t)) {
			// Wait for next round flush
		} else {
			xdb_db_t *pDb = XDB_DBPTR(pDbm);
			pDb->flush_id = pDb->lastchg_id;
		}
		xdb_wal_wrunlock (pDbm);

		// fsync may take long time, so do it out of wal lock
		xdb_stg_sync (&pDbm->pWalm->stg_mgr,    0, 0, false);
		xdb_stg_sync (&pDbm->pWalmBak->stg_mgr, 0, 0, false);
	}

	pDb->flush_time = xdb_timestamp();

exit:
	xdb_wrunlock_db (pDbm);
	return 0;
}

XDB_STATIC xdb_ret
xdb_repair_db (xdb_dbm_t *pDbm, int flags)
{
	// Lock DB
	xdb_print ("=== Begin Repair Database %s ===\n", XDB_OBJ_NAME (pDbm));

	// check all tables
	int count = XDB_OBJM_MAX(pDbm->db_objm);
	for (int i = 0; i < count; ++i) {
		xdb_tblm_t *pTblm = XDB_OBJM_GET(pDbm->db_objm, i);
		if (NULL != pTblm) {
			xdb_wrlock_tblstg (pTblm);
			xdb_tbl_t *pTbl = XDB_TBLPTR(pTblm);
			if (pTbl->flush_id != pTbl->lastchg_id) {
				__xdb_repair_table (pTblm, flags);
			}
			xdb_wrunlock_tblstg (pTblm);
		}	
	}

	xdb_wal_redo (pDbm);
	xdb_flush_db (pDbm, 0);

	xdb_print ("=== End Recover database %s ===\n", XDB_OBJ_NAME (pDbm));

	return XDB_OK;
}

XDB_STATIC int 
xdb_dump_create_db (xdb_dbm_t *pDbm, char buf[], xdb_size size, uint32_t flags)
{
	xdb_size			len = 0;

	len += sprintf (buf+len, "CREATE DATABASE%s %s%s;", (flags & XDB_DUMP_EXIST) ? " IF NOT EXISTS" : "", XDB_OBJ_NAME(pDbm), pDbm->bMemory?" ENGINE=MEMORY":"");

	return len;
}
