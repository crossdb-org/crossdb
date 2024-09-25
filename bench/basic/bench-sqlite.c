#define _GNU_SOURCE
#include <sqlite3.h>

#define BENCH_DBNAME	"SQLite"
#define LKUP_COUNT		1000000

#include "bench.h"

static inline void error_check (int status) 
{
	if (status != SQLITE_OK) {
		printf("sqlite3 error: status = %d\n", status);
		exit(1);
	}
}

static inline void exec_error_check (int status, char *err_msg) 
{
	if (status != SQLITE_OK) {
		printf ("SQL error: %s\n", err_msg);
		sqlite3_free (err_msg);
		exit(1);
	}
}

static inline void step_error_check (int status) 
{
	if (status != SQLITE_DONE) {
		printf ("SQL step error: status = %d\n", status);
		exit(1);
	}
}

void* bench_open (const char *db, bool bLock)
{
	int status;
	char* err_msg = NULL;
	sqlite3* pDb = NULL;

	status = sqlite3_open (db, &pDb);
	error_check (status);

	char * sqlite3_cfg[] = {
		"PRAGMA synchronous = OFF",
		"PRAGMA journal_mode = OFF",
		"PRAGMA temp_store = memory",
		"PRAGMA optimize",
		NULL
	};
	for (int i = 0; NULL != sqlite3_cfg[i]; ++i) {
		status = sqlite3_exec (pDb, sqlite3_cfg[i], NULL, NULL, &err_msg);
		exec_error_check (status, err_msg);
	}

	return pDb;

error:
	return NULL;
}

void bench_close (void *pDb)
{
	int status = sqlite3_close (pDb);
	error_check (status);
}

bool bench_sql (void *pDb, const char *sql)
{
	char* err_msg = NULL;
	int status = sqlite3_exec (pDb, sql, NULL, NULL, &err_msg);
	exec_error_check (status, err_msg);
	return true;
}

bool bench_sql_insert (void *pDb, const char *sql, int id, const char *name, int age, const char *cls, int score)
{
	sqlite3_stmt *pStmt = bench_stmt_prepare (pDb, sql);
	bool ok = bench_stmt_insert (pStmt, id, name, age, cls, score);
	bench_stmt_close (pStmt);
	return ok;
}

bool bench_sql_get_byid (void *pDb, const char *sql, int id, stu_callback callback, void *pArg)
{
	sqlite3_stmt *pStmt = bench_stmt_prepare (pDb, sql);
	bool ok = bench_stmt_get_byid (pStmt, id, callback, pArg);
	bench_stmt_close (pStmt);
	return ok;
}

bool bench_sql_updAge_byid (void *pDb, const char *sql, int id, int age)
{
	sqlite3_stmt *pStmt = bench_stmt_prepare (pDb, sql);
	bool ok = bench_stmt_updAge_byid (pStmt, id, age);
	bench_stmt_close (pStmt);
	return ok;
}

bool bench_sql_del_byid (void *pDb, const char *sql, int id)
{
	sqlite3_stmt *pStmt = bench_stmt_prepare (pDb, sql);
	bool ok = bench_stmt_del_byid (pStmt, id);
	bench_stmt_close (pStmt);
	return ok;
}

void* bench_stmt_prepare (void *pDb, const char *sql)
{
	sqlite3_stmt *pStmt;
	int status = sqlite3_prepare_v2 (pDb, sql, -1, &pStmt, NULL);
	error_check (status);
	return pStmt;
}

void bench_stmt_close (void *pStmt)
{
	sqlite3_finalize (pStmt);
}

bool bench_stmt_insert (void *pStmt, int id, const char *name, int age, const char *cls, int score)
{
	sqlite3_bind_int (pStmt, 1, id);
	sqlite3_bind_text (pStmt, 2, name, strlen(name), SQLITE_STATIC);
	sqlite3_bind_int (pStmt, 3, age);
	sqlite3_bind_text (pStmt, 4, cls, strlen(cls), SQLITE_STATIC);
	sqlite3_bind_int (pStmt, 5, score);

	int status = sqlite3_step (pStmt);
	step_error_check(status);

	// Reset SQLite statement for next call
	sqlite3_clear_bindings (pStmt);
	sqlite3_reset (pStmt);
	return true;
}

bool bench_stmt_get_byid (void *pStmt, int id, stu_callback callback, void *pArg)
{
	int ok = false;
	int status;

	sqlite3_bind_int (pStmt, 1, id);
	// Execute query_stmt
	if ((status = sqlite3_step (pStmt)) == SQLITE_ROW) {
		int 		id		= sqlite3_column_int (pStmt, 0);
		const char *name	= (const char *)sqlite3_column_text (pStmt, 1);
		int 		age 	= sqlite3_column_int (pStmt, 2);	
		const char *cls		= (const char *)sqlite3_column_text (pStmt, 3);
		int 		score	= sqlite3_column_int (pStmt, 4);
		callback (pArg, id, name, age, cls, score);
		ok = true;
	} else {
		bench_print ("sqlite3 error: status = %d, no row found\n", status);
	}

	// Reset SQLite statement for next call
	sqlite3_clear_bindings (pStmt);
	sqlite3_reset (pStmt);
	return ok;
}

bool bench_stmt_updAge_byid (void *pStmt, int id, int age)
{
	sqlite3_bind_int (pStmt, 1, age);
	sqlite3_bind_int (pStmt, 2, id);
	int status = sqlite3_step (pStmt);
	step_error_check(status);

	// Reset SQLite statement for next call
	sqlite3_clear_bindings (pStmt);
	sqlite3_reset (pStmt);
	return true;
}

bool bench_stmt_del_byid (void *pStmt, int id)
{
	sqlite3_bind_int (pStmt, 1, id);
	int status = sqlite3_step (pStmt);
	step_error_check(status);

	// Reset SQLite statement for next call
	sqlite3_clear_bindings (pStmt);
	sqlite3_reset (pStmt);
	return true;
}
