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
	xdb_lv2bmp_init (&pConn->dbTrans_bmp);
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
		if (!strcasecmp (dbpath, ":memory:")) {
			xdb_res_t *pRes = xdb_exec (pConn, "CREATE DATABASE IF NOT EXISTS memory ENGINE=MEMORY");
			XDB_CHECK (pRes);
			xdb_exec (pConn, "USE memory");
		} else {
			xdb_res_t *pRes = xdb_pexec (pConn, "CREATE DATABASE IF NOT EXISTS \'%s\'", dbpath);
			XDB_CHECK (pRes);
		}
	}

	xdb_atomic_inc (&s_xdb_conn_count);
	return pConn;
}

void
xdb_close (xdb_conn_t* pConn)
{
	if (NULL == pConn) {
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

	if (0 == s_xdb_conn_count) {
		xdb_exit ();
	}
}

const char*
xdb_curdb (xdb_conn_t* pConn)
{
	return pConn->cur_db;
}