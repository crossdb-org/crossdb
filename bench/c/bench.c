#include <sys/time.h>
#include <crossdb.h>

static uint64_t timestamp_us () 
{ 
	struct timeval tv; gettimeofday(&tv, NULL); 
	return tv.tv_sec * 1000000LL + tv.tv_usec; 
}
static uint64_t s_last_ts;
static void ts_beg() { s_last_ts = timestamp_us (); }
static void ts_end (int count) 
{
	s_last_ts = timestamp_us() - s_last_ts; 
	printf ("Use time %uus, QPS %d\n", (uint32_t)s_last_ts, (int)((uint64_t)count*1000000/s_last_ts)); 
}

int main (int argc, char **argv)
{
	xdb_res_t*	pRes;
	xdb_row_t	*pRow;
	xdb_stmt_t 	*pStmt;

	xdb_conn_t* pConn = xdb_open (":memory:");
	if (NULL == pConn) {
		printf ("Can't open connection:\n");
		return -1;
	}

	int STU_COUNT = argc > 1 ? atoi(argv[1]) : 1000000;
	#define LKUP_COUNT	10000000

	pRes = xdb_exec (pConn, "CREATE TABLE student (id INT PRIMARY KEY, name CHAR(16), age INT, class CHAR(16), score INT)");
	XDB_CHECK(pRes, printf ("Can't create table student\n"); goto error;);

	//xdb_exec (pConn, "SHELL");

	static int id[LKUP_COUNT];
	for (int i = 0; i < LKUP_COUNT; ++i) {
		id[i] = rand()%STU_COUNT;		
	}

	printf ("********************** INSERT %d with SQL **********************\n", STU_COUNT);
	ts_beg();
	for (int i = 0; i < STU_COUNT; ++i) {
		pRes = xdb_pexec (pConn, "INSERT INTO student (id,name,age,class,score) VALUES (%d,'jack',10,'3-1',90)", i);
		XDB_CHECK(pRes, printf ("Can't insert table student id=%d\n", i); goto error;);
	}
	ts_end(STU_COUNT);

	printf ("********************** Random LKUP %d with SQL **********************\n", LKUP_COUNT/2);
	for (int t = 0; t < 4; ++t) {
		int count = 0;
		ts_beg();
		for (int i = 0; i < LKUP_COUNT/2; ++i) {
			pRes = xdb_pexec (pConn, "SELECT * FROM student WHERE id=%d", id[i]);
			pRow = xdb_fetch_row (pRes);
			if (NULL != pRow) {
				count++;
				xdb_free_result (pRes);
			}
		}
		ts_end(LKUP_COUNT/2);
		if (count != LKUP_COUNT/2) {
			printf ("OK %d != LKUP %d\n", count, LKUP_COUNT/2);
		}
	}

	printf ("********************** UPDATE %d with SQL **********************\n", STU_COUNT);
	ts_beg();
	for (int i = 0; i < STU_COUNT; ++i) {
		pRes = xdb_pexec (pConn, "UPDATE student SET age=15 WHERE id=%d", i);
		XDB_CHECK(pRes, printf ("Can't update table student id=%d\n", i); goto error;);
	}
	ts_end(STU_COUNT);

	//xdb_exec (pConn, "SHELL");

	printf ("********************** DELETE %d with SQL **********************\n", STU_COUNT);
	ts_beg();
	for (int i = 0; i < STU_COUNT; ++i) {
		pRes = xdb_pexec (pConn, "DELETE FROM student WHERE id=%d", i);
		XDB_CHECK(pRes, printf ("Can't delete table student id=%d\n", i); goto error;);
	}
	ts_end(STU_COUNT);


	printf ("\n********************** INSERT %d with SQL **********************\n", STU_COUNT);
	ts_beg();
	for (int i = 0; i < STU_COUNT; ++i) {
		pRes = xdb_pexec (pConn, "INSERT INTO student (id,name,age,class,score) VALUES (%d,'jack',10,'3-1',90)", i);
		XDB_CHECK(pRes, printf ("Can't insert table student id=%d\n", i); goto error;);
	}
	ts_end(STU_COUNT);

	printf ("********************** Random LKUP %d with Prepared STMT **********************\n", LKUP_COUNT);
	pStmt = xdb_stmt_prepare (pConn, "SELECT * FROM student WHERE id=?");
	if (NULL == pStmt) {
		goto error;
	}

	for (int t = 0; t < 4; ++t) {
		int count = 0;
		ts_beg();
		for (int i = 0; i < LKUP_COUNT; ++i) {
			xdb_bind_int (pStmt, 1, id[i]);
			pRes = xdb_stmt_exec (pStmt);
			pRow = xdb_fetch_row (pRes);
			if (NULL != pRow) {
				count++;
			}
			xdb_free_result (pRes);
		}
		ts_end(LKUP_COUNT);
		if (count != LKUP_COUNT) {
			printf ("OK %d != LKUP %d\n", count, LKUP_COUNT);
		}
	}
	xdb_stmt_close (pStmt);

	printf ("********************** UPDATE %d with Prepared SMTT **********************\n", STU_COUNT);
	pStmt = xdb_stmt_prepare (pConn, "UPDATE student SET age=15 WHERE id=?");
	if (NULL == pStmt) {
		goto error;
	}
	ts_beg();
	for (int i = 0; i < STU_COUNT; ++i) {
		xdb_bind_int (pStmt, 1, i);
		pRes = xdb_stmt_exec (pStmt);
		XDB_CHECK(pRes, printf ("Can't update table student id=%d\n", i); goto error;);
	}
	ts_end(STU_COUNT);
	xdb_stmt_close (pStmt);

	//xdb_exec (pConn, "SHELL");

	printf ("********************** DELETE %d with Prepared SQL **********************\n", STU_COUNT);
	pStmt = xdb_stmt_prepare (pConn, "DELETE FROM student WHERE id=?");
	if (NULL == pStmt) {
		goto error;
	}
	ts_beg();
	for (int i = 0; i < STU_COUNT; ++i) {
		xdb_bind_int (pStmt, 1, i);
		pRes = xdb_stmt_exec (pStmt);
		XDB_CHECK(pRes, printf ("Can't delete table student id=%d\n", i); goto error;);
	}
	ts_end(STU_COUNT);
	xdb_stmt_close (pStmt);

	//xdb_exec (pConn, "SHELL");

error:
	xdb_close (pConn);
	
	return 0;
}
