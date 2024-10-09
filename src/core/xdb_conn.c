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

static uint32_t s_xdb_conn_count = 0;

XDB_STATIC void 
xdb_conn_init (xdb_conn_t* pConn)
{
	pConn->pConn = pConn;
	pConn->pSql = pConn->sql_buf;
	pConn->sqlbuf_len = sizeof (pConn->sql_buf);
	pConn->conn_res.len_type = (sizeof (xdb_res_t)) | (XDB_RET_REPLY<<28);
	xdb_rowset_init (&pConn->row_set);
	pConn->bInTrans = false;
	pConn->bAutoTrans = false;
	pConn->bAutoCommit = true;
	pConn->conn_stdout = stdout;
	pConn->ref_cnt = 1;
	xdb_lv2bmp_init (&pConn->dbTrans_bmp);
	xdb_atomic_inc (&s_xdb_conn_count);
}

xdb_conn_t*
xdb_open (const char *dbpath)
{
	xdb_conn_t* pConn = xdb_calloc (sizeof (xdb_conn_t));
	if (NULL == pConn) {
		return NULL;
	}

	xdb_init ();
	xdb_sysdb_init ();

	xdb_conn_init (pConn);

	if (NULL != dbpath) {
		xdb_res_t *pRes = xdb_pexec (pConn, "CREATE DATABASE IF NOT EXISTS \'%s\'", dbpath);
		XDB_RESCHK (pRes);
	}

	return pConn;
}

void
xdb_close (xdb_conn_t* pConn)
{
	if (NULL == pConn) {
		return;
	}
	if (--pConn->ref_cnt > 0) {
		return;
	}

	xdb_rollback (pConn);
	
	if (pConn->conn_stdout != stdout) {
		fclose (pConn->conn_stdout);
	} else if (pConn->sockfd > 0) {
		xdb_sock_close (pConn->sockfd);
	}

	xdb_trans_free (pConn);
	xdb_rowset_free (&pConn->row_set);
	xdb_free (pConn->pQueryRes);
	memset (pConn, 0, sizeof (*pConn));
	xdb_free (pConn);

	xdb_atomic_dec (&s_xdb_conn_count);

	if (1 == s_xdb_conn_count) {
		xdb_exit ();
	}
}

const char*
xdb_curdb (xdb_conn_t* pConn)
{
	return pConn->cur_db;
}
