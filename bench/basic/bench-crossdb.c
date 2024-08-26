
#define BENCH_DBNAME	"CrossDB"

#define LKUP_COUNT		10000000
#define SQL_LKUP_COUNT	LKUP_COUNT/5
#define UPD_COUNT		LKUP_COUNT/10

#include "bench.h"
#include <crossdb.h>

void* bench_create ()
{
	xdb_conn_t* pConn = xdb_open (":memory:");
	XDB_CHECK (NULL != pConn, printf ("Can't open connection:\n"); return NULL;);

	xdb_res_t*	pRes = xdb_exec (pConn, "CREATE TABLE student (id INT PRIMARY KEY, name CHAR(16), age INT, class CHAR(16), score INT)");
	XDB_RESCHK (pRes, printf ("Can't create table student\n"); xdb_close(pConn); return NULL;);

	return pConn;
}

void bench_close (void *pConn)
{
	xdb_close (pConn);
}

void bench_sql_test (void *pConn, int STU_COUNT, bool bRand, bench_result_t *pResult)
{
	xdb_res_t*	pRes;
	xdb_row_t	*pRow;

	bench_print ("\n[============= SQL Test =============]\n\n");


	bench_print ("------------ INSERT %d ------------\n", STU_COUNT);
	bench_ts_beg();
	for (int i = 0; i < STU_COUNT; ++i) {
		pRes = xdb_bexec (pConn, "INSERT INTO student (id,name,age,class,score) VALUES (?,?,?,?,?)", 
							i, STU_NAME(i), STU_AGE(i), STU_CLASS(i), STU_SCORE(i));
		XDB_RESCHK (pRes, bench_print ("Can't insert table student id=%d\n", i); return;);
	}
	pResult->insert_qps += bench_ts_end (STU_COUNT);


	bench_print ("------------ %s LKUP %d ------------\n", ORDER_STR(bRand), SQL_LKUP_COUNT);
	uint64_t qps_sum = 0;
	for (int t = 0; t < 5; ++t) {
		int count = 0;
		bench_ts_beg();
		for (int i = 0; i < SQL_LKUP_COUNT; ++i) {
			pRes = xdb_bexec (pConn, "SELECT * FROM student WHERE id=?", STU_ID(i));
			pRow = xdb_fetch_row (pRes);
			if (NULL != pRow) {	
				int 		id		= *(int*)pRow[0];
				const char *name 	=  (char*)pRow[1];
				int 		age 	= *(int*)pRow[2];
				const char *class 	=  (char*)pRow[3];
				int 		score	= *(int*)pRow[4];
				(void)id; (void)name; (void)age; (void)class; (void)score;
				count++;
			}
			xdb_free_result (pRes);
		}
		qps_sum += bench_ts_end (SQL_LKUP_COUNT);
		XDB_CHECK (count == SQL_LKUP_COUNT, bench_print ("OK %d != LKUP %d\n", count, SQL_LKUP_COUNT););
	}
	pResult->query_qps += qps_sum / 5;


	bench_print ("------------ %s UPDATE %d ------------\n", ORDER_STR(bRand), UPD_COUNT);
	bench_ts_beg();
	for (int i = 0; i < UPD_COUNT; ++i) {
		pRes = xdb_bexec (pConn, "UPDATE student SET age=age+? WHERE id=?", 1, STU_ID(i));
		XDB_RESCHK (pRes, bench_print ("Can't update table student id%d\n", STU_ID(i)); return;);
	}
	pResult->update_qps += bench_ts_end (UPD_COUNT);


	bench_print ("------------ %s DELETE %d ------------\n", ORDER_STR(bRand), STU_COUNT);
	bench_ts_beg();
	for (int i = 0; i < STU_COUNT; ++i) {
		pRes = xdb_bexec (pConn, "DELETE FROM student WHERE id=?", STU_ID(i));
		XDB_RESCHK (pRes, bench_print ("Can't delete table student id=%d\n", STU_ID(i)); return;);
	}
	pResult->delete_qps += bench_ts_end (STU_COUNT);
}

void bench_pstmt_test (void *pConn, int STU_COUNT, bool bRand, bench_result_t *pResult)
{
	xdb_res_t*	pRes;
	xdb_row_t	*pRow;
	xdb_stmt_t 	*pStmt = NULL;

	bench_print ("\n[============= Prepared STMT Test =============]\n");


	bench_print ("\n------------ INSERT %d ------------\n", STU_COUNT);
	pStmt = xdb_stmt_prepare (pConn, "INSERT INTO student (id,name,age,class,score) VALUES (?,?,?,?,?)");
	bench_ts_beg();
	for (int i = 0; i < STU_COUNT; ++i) {
		pRes = xdb_stmt_bexec (pStmt, i, STU_NAME(i), STU_AGE(i), STU_CLASS(i), STU_SCORE(i));
		XDB_RESCHK (pRes, bench_print ("Can't insert table student id%d\n", i); goto error;);
	}
	pResult->insert_qps += bench_ts_end (STU_COUNT);
	xdb_stmt_close (pStmt);


	bench_print ("------------ %s LKUP %d ------------\n", ORDER_STR(bRand), LKUP_COUNT);
	pStmt = xdb_stmt_prepare (pConn, "SELECT * FROM student WHERE id=?");
	XDB_CHECK (pStmt != NULL, goto error);
	uint64_t qps_sum = 0;
	for (int t = 0; t < 5; ++t) {
		int count = 0;
		bench_ts_beg();
		for (int i = 0; i < LKUP_COUNT; ++i) {
			pRes = xdb_stmt_bexec (pStmt, STU_ID(i));
			pRow = xdb_fetch_row (pRes);
			if (NULL != pRow) {	
				int 		id		= *(int*)pRow[0];
				const char *name 	=  (char*)pRow[1];
				int 		age 	= *(int*)pRow[2];
				const char *class 	=  (char*)pRow[3];
				int 		score	= *(int*)pRow[4];
				(void)id; (void)name; (void)age; (void)class; (void)score;
				count++;
			}
			xdb_free_result (pRes);
		}
		qps_sum += bench_ts_end (LKUP_COUNT);
		XDB_CHECK (count == LKUP_COUNT, bench_print ("OK %d != LKUP %d\n", count, LKUP_COUNT););
	}
	xdb_stmt_close (pStmt);
	pResult->query_qps += qps_sum / 5;


	bench_print ("------------ %s UPDATE %d ------------\n", ORDER_STR(bRand), UPD_COUNT);
	pStmt = xdb_stmt_prepare (pConn, "UPDATE student SET age=age+? WHERE id=?");
	XDB_CHECK (pStmt != NULL, goto error);
	bench_ts_beg();
	for (int i = 0; i < UPD_COUNT; ++i) {
		pRes = xdb_stmt_bexec (pStmt, 1, STU_ID(i));
		XDB_RESCHK (pRes, bench_print ("Can't update table student id%d\n", STU_ID(i)); goto error;);
	}
	pResult->update_qps = bench_ts_end (UPD_COUNT);
	xdb_stmt_close (pStmt);


	bench_print ("------------ %s DELETE %d ------------\n", ORDER_STR(bRand), STU_COUNT);
	pStmt = xdb_stmt_prepare (pConn, "DELETE FROM student WHERE id=?");
	XDB_CHECK (pStmt != NULL, goto error);
	bench_ts_beg();
	for (int i = 0; i < STU_COUNT; ++i) {
		pRes = xdb_stmt_bexec (pStmt, STU_ID(i));
		XDB_RESCHK (pRes, bench_print ("Can't delete table student id%d\n", STU_ID(i)); goto error;);
	}
	pResult->delete_qps += bench_ts_end (STU_COUNT);

error:	
	xdb_stmt_close (pStmt);
}
