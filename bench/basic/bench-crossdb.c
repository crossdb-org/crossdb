#include <crossdb.h>

#define BENCH_DBNAME	"CrossDB"

int LKUP_COUNT = 10000000;

#include "bench.h"

void* bench_open (const char *db)
{
	xdb_conn_t* pConn;
	if (!s_bench_svr) {
		pConn = xdb_open (db);
	} else {
		pConn = xdb_connect (NULL, NULL, NULL, NULL, 7777);
		xdb_bexec (pConn, "CREATE DATABASE school ENGINE=MEMORY");
		xdb_bexec (pConn, "USE school");
		LKUP_COUNT = 100000;
	}
	XDB_CHECK (NULL != pConn, printf ("Can't open connection:\n"); return NULL;);
	return pConn;
}

void bench_close (void *pConn)
{
	xdb_close (pConn);
}

bool bench_sql (void *pConn, const char *sql)
{
	xdb_res_t*	pRes = xdb_exec (pConn, sql);
	XDB_RESCHK (pRes, printf ("Can't exec '%s'\n", sql); return false;);
	return true;
}

bool bench_sql_insert (void *pConn, const char *sql, int id, const char *name, int age, const char *cls, int score)
{
	xdb_res_t	*pRes = xdb_bexec (pConn, sql, id, name, age, cls, score);
	return 1 == pRes->affected_rows;
}

bool bench_sql_get_byid (void *pConn, const char *sql, int id, stu_callback callback, void *pArg)
{
	xdb_res_t	*pRes = xdb_bexec (pConn, sql, id);
	xdb_row_t	*pRow = xdb_fetch_row (pRes);
	if (NULL != pRow) { 
		callback (pArg, *(int*)pRow[0], (char*)pRow[1], *(int*)pRow[2], (char*)pRow[3], *(int*)pRow[4]);
	}
	xdb_free_result (pRes);
	return pRow != NULL;
}

bool bench_sql_updAge_byid (void *pConn, const char *sql, int id, int age)
{
	xdb_res_t	*pRes = xdb_bexec (pConn, sql, age, id);
	if (pRes->affected_rows != 1) {
		printf ("wrong\n"); 
	}
	return 1 == pRes->affected_rows;
}

bool bench_sql_del_byid (void *pConn, const char *sql, int id)
{
	xdb_res_t	*pRes = xdb_bexec (pConn, sql, id);
	return 1 == pRes->affected_rows;
}


void* bench_stmt_prepare (void *pConn, const char *sql)
{
	return xdb_stmt_prepare (pConn, sql);
}

void bench_stmt_close (void *pStmt)
{
	xdb_stmt_close (pStmt);
}

bool bench_stmt_insert (void *pStmt, int id, const char *name, int age, const char *cls, int score)
{
	xdb_res_t	*pRes = xdb_stmt_bexec (pStmt, id, name, age, cls, score);
	return 1 == pRes->affected_rows;
}

bool bench_stmt_get_byid (void *pStmt, int id, stu_callback callback, void *pArg)
{
	xdb_res_t	*pRes = xdb_stmt_bexec (pStmt, id);
	xdb_row_t	*pRow = xdb_fetch_row (pRes);
	if (NULL != pRow) { 
		callback (pArg, *(int*)pRow[0], (char*)pRow[1], *(int*)pRow[2], (char*)pRow[3], *(int*)pRow[4]);
	}
	xdb_free_result (pRes);
	return pRow != NULL;
}

bool bench_stmt_updAge_byid (void *pStmt, int id, int age)
{
	xdb_res_t	*pRes = xdb_stmt_bexec (pStmt, age, id);
	return 1 == pRes->affected_rows;
}

bool bench_stmt_del_byid (void *pStmt, int id)
{
	xdb_res_t	*pRes = xdb_stmt_bexec (pStmt, id);
	return 1 == pRes->affected_rows;
}
