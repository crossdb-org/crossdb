#include <crossdb.h>

int main (int argc, char **argv)
{
	xdb_res_t	*pRes;
	xdb_row_t	*pRow;

	xdb_conn_t	*pConn = xdb_open (":memory:");

	// Create Table
	pRes = xdb_exec (pConn, "CREATE TABLE student (id INT PRIMARY KEY, name CHAR(16), age INT, class CHAR(16), score FLOAT, info CHAR(255))");
	XDB_CHECK(pRes, printf ("Can't create table student\n"); goto error;);
	pRes = xdb_exec (pConn, "CREATE TABLE IF NOT EXISTS teacher (id INT PRIMARY KEY, name CHAR(16), age INT, info CHAR(255), INDEX (name))");
	XDB_CHECK(pRes, printf ("Can't create table teacher\n"); goto error;);
	pRes = xdb_exec (pConn, "CREATE TABLE IF NOT EXISTS book (id INT PRIMARY KEY, name CHAR(64), author CHAR(32), count INT, INDEX (name))");
	XDB_CHECK(pRes, printf ("Can't create table book\n"); goto error;);

	// Insert
	pRes = xdb_exec (pConn, "INSERT INTO student (id,name,age,class,score) VALUES (1,'jack',10,'3-1',90),(2,'tom',11,'2-5',91),(3,'jack',11,'1-6',92),(4,'rose',10,'4-2',90),(5,'tim',10,'3-1',95)");
	XDB_CHECK(pRes, printf ("Can't insert table student\n"); goto error;);
	pRes = xdb_pexec (pConn, "INSERT INTO student (id,name,age,class,score,info) VALUES (6,'Tony',10,'3-1',95,'%s')", "He is a boy.\nHe likes playing football.\nWe all like him!");
	XDB_CHECK(pRes, printf ("Can't insert table student\n"); goto error;);
	pRes = xdb_pexec (pConn, "INSERT INTO student (id,name,age,class,score,info) VALUES (7,'Wendy',10,'3-1',95,'%s')", "She is a girl.\nShe likes cooking.\nWe all love her!");
	XDB_CHECK(pRes, printf ("Can't insert table student\n"); goto error;);
	pRes = xdb_exec (pConn, "INSERT INTO teacher (id,name,age) VALUES (1,'Tomas',40),(2,'Steven',50),(3,'Bill',31),(4,'Lucy',29)");
	XDB_CHECK(pRes, printf ("Can't insert table teacher\n"); goto error;);
	pRes = xdb_exec (pConn, "INSERT INTO book (id,name,author,count) VALUES (1,'Romeo and Juliet','Shakespeare',10),(2,'Pride and Prejudice','Austen',5),(3,'Great Expectations','Dickens',8),(4,'Sorrows of Young Werther','Von Goethe',4)");
	XDB_CHECK(pRes, printf ("Can't insert table book\n"); goto error;);

	// Select
	pRes = xdb_exec (pConn, "SELECT * from student");
	printf ("=== Select all %d rows\n", (int)pRes->row_count);
	while (NULL != (pRow = xdb_fetch_row (pRes))) {
		xdb_print_row (pRes->col_meta, pRow, 0);
		printf ("\n");
	}
	xdb_free_result (pRes);

	// Update
	printf ("\n=== Update age = 9 for id = 2\n");
	pRes = xdb_exec (pConn, "UPDATE student set age=9 WHERE id = 2");
	XDB_CHECK(pRes, printf ("Can't update id=%d\n",2); goto error;);

	pRes = xdb_exec (pConn, "SELECT id,name,age,class,score from student WHERE id = 2");
	printf ("select %d rows\n", (int)pRes->row_count);
	while (NULL != (pRow = xdb_fetch_row (pRes))) {
		xdb_print_row (pRes->col_meta, pRow, 0);
		printf ("\n");
		printf ("id=%d name='%s' age=%d class='%s' score=%f\n", 
			xdb_column_int (pRes->col_meta, pRow, 0), 
			xdb_column_str (pRes->col_meta, pRow, 1), 
			xdb_column_int (pRes->col_meta, pRow, 2), 
			xdb_column_str (pRes->col_meta, pRow, 3), 
			xdb_column_float(pRes->col_meta, pRow, 4));
		printf ("id=%d name='%s' age=%d class='%s' score=%f\n", 
			*(int*)pRow[0], 
			(char*)pRow[1], 
			*(int*)pRow[2], 
			(char*)pRow[3], 
			*(float*)pRow[4]);
	}
	xdb_free_result (pRes);

	// Delete
	printf ("\n=== Delete id = 3\n");
	pRes = xdb_exec (pConn, "DELETE FROM student WHERE id = 3");
	XDB_CHECK(pRes, printf ("Can't delete id=%d\n",3); goto error;);

	pRes = xdb_exec (pConn, "SELECT * from student WHERE id = 3");
	printf ("select %d rows\n", (int)pRes->row_count);
	while (NULL != (pRow = xdb_fetch_row (pRes))) {
		xdb_print_row (pRes->col_meta, pRow, 0);
		printf ("\n");
	}
	xdb_free_result (pRes);

	// Aggregation function
	printf ("\n=== AGG COUNT,MIN,MAX,SUM,AVG\n");
	pRes = xdb_exec (pConn, "SELECT COUNT(*),MIN(score),MAX(score),SUM(score),AVG(score) FROM student");
	printf ("=== select %d rows\n", (int)pRes->row_count);
	if (NULL != (pRow = xdb_fetch_row (pRes))) {
		xdb_print_row (pRes->col_meta, pRow, 0);
		printf ("\n");
		printf ("COUNT(*)=%d MIN(score)=%f MAX(score)=%f SUM(score)=%f AVG(score)=%f\n", 
			xdb_column_int (pRes->col_meta, pRow, 0), 
			xdb_column_double(pRes->col_meta, pRow, 1), 
			xdb_column_double(pRes->col_meta, pRow, 2), 
			xdb_column_double(pRes->col_meta, pRow, 3), 
			xdb_column_double(pRes->col_meta, pRow, 4));
		printf ("COUNT(*)=%d MIN(score)=%f MAX(score)=%f SUM(score)=%f AVG(score)=%f\n", 
			(int)*(int64_t*)pRow[0], 
			*(float*)pRow[1], 
			*(float*)pRow[2], 
			*(double*)pRow[3], 
			*(double*)pRow[4]);
	}
	xdb_free_result (pRes);

	// Transaction rollback
	printf ("\n=== Rollback\n");
	xdb_begin (pConn);
	pRes = xdb_exec (pConn, "UPDATE student set age=15 WHERE id = 2");
	pRes = xdb_exec (pConn, "SELECT id,name,age from student WHERE id = 2");
	printf ("select %d rows\n", (int)pRes->row_count);
	if (NULL != (pRow = xdb_fetch_row (pRes))) {
		xdb_print_row (pRes->col_meta, pRow, 0);
		printf ("\n");
	}
	xdb_free_result (pRes);
	printf ("-- rollback\n");
	xdb_rollback (pConn);
	pRes = xdb_exec (pConn, "SELECT id,name,age from student WHERE id = 2");
	printf ("select %d rows\n", (int)pRes->row_count);
	if (NULL != (pRow = xdb_fetch_row (pRes))) {
		xdb_print_row (pRes->col_meta, pRow, 0);
		printf ("\n");
	}
	xdb_free_result (pRes);

	// Transaction commit
	printf ("\n=== Commit\n");
	xdb_begin (pConn);
	pRes = xdb_exec (pConn, "UPDATE student set age=15 WHERE id = 2");
	pRes = xdb_exec (pConn, "SELECT * from student WHERE id = 2");
	printf ("select %d rows\n", (int)pRes->row_count);
	if (NULL != (pRow = xdb_fetch_row (pRes))) {
		xdb_print_row (pRes->col_meta, pRow, 0);
		printf ("\n");
	}
	xdb_free_result (pRes);
	printf ("-- commit\n");
	xdb_commit (pConn);
	pRes = xdb_exec (pConn, "SELECT * from student WHERE id = 2");
	printf ("select %d rows\n", (int)pRes->row_count);
	if (NULL != (pRow = xdb_fetch_row (pRes))) {
		xdb_print_row (pRes->col_meta, pRow, 0);
		printf ("\n");
	}
	xdb_free_result (pRes);

	// Multi-Statements
	printf ("\n=== Muti-Statements\n");
	pRes = xdb_exec (pConn, "SELECT COUNT(*) FROM student; SELECT id,name FROM student WHERE id=2");
	printf ("-- 1st result: ");
	// count(*)	
	if (NULL != (pRow = xdb_fetch_row (pRes))) {
		xdb_print_row (pRes->col_meta, pRow, 0);
		printf ("\n");
	}
	xdb_free_result (pRes);
	// select
	printf ("-- 2nd result: ");
	pRes = xdb_next_result (pConn);
	if (NULL != pRes) {
		if (NULL != (pRow = xdb_fetch_row (pRes))) {
			xdb_print_row (pRes->col_meta, pRow, 0);
			printf ("\n");
		}
		xdb_free_result (pRes);
	}

	// Embedded shell
	xdb_exec (pConn, "SHELL");

error:
	xdb_close (pConn);

	return 0;
}
