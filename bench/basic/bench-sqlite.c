
#define BENCH_DBNAME	"SQLite"

#define LKUP_COUNT		1000000
#define SQL_LKUP_COUNT	LKUP_COUNT/5
#define UPD_COUNT		LKUP_COUNT/10

#include "bench.h"
#include <sqlite3.h>

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

void* bench_create ()
{
	int status;
	char* err_msg = NULL;
	sqlite3* pDb = NULL;

	status = sqlite3_open (":memory:", &pDb);
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

	status = sqlite3_exec (pDb, "CREATE TABLE student (id INT PRIMARY KEY, name CHAR(16), age INT, class CHAR(16), score INT)", NULL, NULL, &err_msg);
	exec_error_check (status, err_msg);

	return pDb;

error:
	return NULL;
}

void bench_close (void *pDb)
{
	int status = sqlite3_close (pDb);
	error_check (status);
}

void bench_sql_test (void *pDb, int STU_COUNT, bool bRand, bench_result_t *pResult)
{
	int 	status;
	char 	sql[1024];
	char* 	err_msg = NULL;
	sqlite3_stmt *pStmt;

	bench_print ("\n[============= SQL Test =============]\n\n");

	bench_print ("------------ INSERT %d ------------\n", STU_COUNT);
	bench_ts_beg();
	for (int i = 0; i < STU_COUNT; ++i) {
		sprintf (sql, "INSERT INTO student (id,name,age,class,score) VALUES (%d,'%s',%d,'%s',%d)", 
								i, STU_NAME(i), STU_AGE(i), STU_CLASS(i), STU_SCORE(i));
		status = sqlite3_exec (pDb, sql, NULL, NULL, &err_msg);
		exec_error_check (status, err_msg);
	}
	pResult->insert_qps += bench_ts_end (STU_COUNT);

	bench_print ("------------ %s LKUP %d ------------\n", ORDER_STR(bRand), LKUP_COUNT/2);
	uint64_t qps_sum = 0;
	for (int t = 0; t < 5; ++t) {
		int count = 0;
		bench_ts_beg();
		for (int i = 0; i < SQL_LKUP_COUNT; ++i) {
			status = sqlite3_prepare_v2 (pDb, "SELECT * FROM student WHERE id=?", -1, &pStmt, NULL);
			error_check (status);
			sqlite3_bind_int (pStmt, 1, STU_ID(i)%STU_COUNT);
			// Execute query_stmt
			if ((status = sqlite3_step(pStmt)) == SQLITE_ROW) {
				int 		id		= sqlite3_column_int (pStmt, 0);
				const char *name 	= sqlite3_column_text (pStmt, 1);
				int 		age 	= sqlite3_column_int (pStmt, 2);	
				const char *class 	= sqlite3_column_text (pStmt, 3);
				int 		score	= sqlite3_column_int (pStmt, 4);
				(void)id; (void)name; (void)age; (void)class; (void)score;
				count++;
			} else {
				bench_print ("sqlite3 error: status = %d, no row found\n", status);
			}
			sqlite3_finalize(pStmt);
		}
		qps_sum += bench_ts_end (SQL_LKUP_COUNT);
		if (count != SQL_LKUP_COUNT) {
			bench_print ("OK %d != LKUP %d\n", count, SQL_LKUP_COUNT);
			return;
		}
	}
	pResult->query_qps += qps_sum / 5;


	bench_print ("------------ %s UPDATE %d ------------\n", ORDER_STR(bRand), UPD_COUNT);
	bench_ts_beg();
	for (int i = 0; i < UPD_COUNT; ++i) {
		sprintf (sql, "UPDATE student SET age=age+%d WHERE id=%d", i, STU_ID(i));
		status = sqlite3_exec (pDb, sql, NULL, NULL, &err_msg);
		exec_error_check (status, err_msg);
	}
	pResult->update_qps += bench_ts_end (UPD_COUNT);


	bench_print ("------------ %s DELETE %d ------------\n", ORDER_STR(bRand), STU_COUNT);
	bench_ts_beg();
	for (int i = 0; i < STU_COUNT; ++i) {
		sprintf (sql, "DELETE FROM student WHERE id=%d", STU_ID(i));
		status = sqlite3_exec (pDb, sql, NULL, NULL, &err_msg);
		exec_error_check (status, err_msg);
	}
	pResult->delete_qps += bench_ts_end (STU_COUNT);

	return;

error:	
	return;
}

void bench_pstmt_test (void *pDb, int STU_COUNT, bool bRand, bench_result_t *pResult)
{
	int 	status;
	char* 	err_msg = NULL;
	sqlite3_stmt *pStmt;

	bench_print ("\n[============= Prepared STMT Test =============]\n");


	bench_print ("\n------------ INSERT %d ------------\n", STU_COUNT);
	status = sqlite3_prepare_v2 (pDb, "INSERT INTO student (id,name,age,class,score) VALUES (?,?,?,?,?)", -1, &pStmt, NULL);
	error_check (status);
	bench_ts_beg();
	for (int i = 0; i < STU_COUNT; ++i) {
		sqlite3_bind_int (pStmt, 1, STU_ID(i));
		sqlite3_bind_text (pStmt, 2, STU_NAME(i), strlen(STU_NAME(i)), SQLITE_STATIC);
		sqlite3_bind_int (pStmt, 3, STU_AGE(i));
		sqlite3_bind_text (pStmt, 4, STU_CLASS(i), strlen(STU_NAME(i)), SQLITE_STATIC);
		sqlite3_bind_int (pStmt, 5, STU_SCORE(i));
		status = sqlite3_step (pStmt);
		step_error_check(status);
		// Reset SQLite statement for next call
		sqlite3_clear_bindings (pStmt);
		sqlite3_reset (pStmt);
	}
	pResult->insert_qps += bench_ts_end (STU_COUNT);
	sqlite3_finalize (pStmt);


	bench_print ("------------ %s LKUP %d ------------\n", ORDER_STR(bRand), LKUP_COUNT);
	status = sqlite3_prepare_v2 (pDb, "SELECT * FROM student WHERE id=?", -1, &pStmt, NULL);
	error_check (status);
	uint64_t qps_sum = 0;
	for (int t = 0; t < 5; ++t) {
		int count = 0;
		bench_ts_beg();
		for (int i = 0; i < LKUP_COUNT; ++i) {
			sqlite3_bind_int (pStmt, 1, STU_ID(i)%STU_COUNT);
			// Execute query_stmt
			if ((status = sqlite3_step (pStmt)) == SQLITE_ROW) {
				int 		id		= sqlite3_column_int (pStmt, 0);
				const char *name 	= sqlite3_column_text (pStmt, 1);
				int 		age 	= sqlite3_column_int (pStmt, 2);	
				const char *class 	= sqlite3_column_text (pStmt, 3);
				int 		score	= sqlite3_column_int (pStmt, 4);
				(void)id; (void)name; (void)age; (void)class; (void)score;
				count++;
			} else {
				bench_print ("sqlite3 error: status = %d, no row found\n", status);
			}
			// Reset SQLite statement for next call
			sqlite3_clear_bindings (pStmt);
			sqlite3_reset (pStmt);
		}
		qps_sum += bench_ts_end (LKUP_COUNT);
		if (count != LKUP_COUNT) {
			bench_print ("OK %d != LKUP %d\n", count, LKUP_COUNT);
			return;
		}
	}
	status = sqlite3_finalize (pStmt);
	error_check (status);			
	pResult->query_qps += qps_sum / 5;

	bench_print ("------------ %s UPDATE %d ------------\n", ORDER_STR(bRand), UPD_COUNT);
	status = sqlite3_prepare_v2 (pDb, "UPDATE student SET age=age+? WHERE id=?", -1, &pStmt, NULL);
	error_check (status);
	bench_ts_beg();
	for (int i = 0; i < UPD_COUNT; ++i) {
		sqlite3_bind_int (pStmt, 1, 1);
		sqlite3_bind_int (pStmt, 2, STU_ID(i));
		status = sqlite3_step (pStmt);
		step_error_check(status);
		// Reset SQLite statement for next call
		sqlite3_clear_bindings (pStmt);
		sqlite3_reset (pStmt);
	}
	pResult->update_qps += bench_ts_end (UPD_COUNT);
	sqlite3_finalize (pStmt);

	bench_print ("------------ %s DELETE %d ------------\n", ORDER_STR(bRand), STU_COUNT);
	status = sqlite3_prepare_v2 (pDb, "DELETE FROM student WHERE id=?", -1, &pStmt, NULL);
	error_check (status);
	bench_ts_beg();
	for (int i = 0; i < STU_COUNT; ++i) {
		sqlite3_bind_int (pStmt, 1, STU_ID(i));
		status = sqlite3_step (pStmt);
		step_error_check(status);
		// Reset SQLite statement for next call
		sqlite3_clear_bindings (pStmt);
		sqlite3_reset (pStmt);
	}
	pResult->delete_qps += bench_ts_end (STU_COUNT);
	sqlite3_finalize (pStmt);
}
