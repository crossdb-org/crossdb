#include "utest.h"
#include <crossdb.h>

struct XdbTest {
	xdb_conn_t *pConn;
};

UTEST_I_SETUP(XdbTest)
{
	xdb_conn_t *pConn = xdb_open (":memory:");
	ASSERT_TRUE (pConn!=NULL);
	utest_fixture->pConn = pConn;
	xdb_res_t *pRes = xdb_exec (pConn, "CREATE TABLE student (id INT PRIMARY KEY, name CHAR(16), age INT, class CHAR(16), score FLOAT)");
	ASSERT_EQ (pRes->errcode, XDB_OK);	
}

UTEST_I_TEARDOWN(XdbTest)
{
	xdb_conn_t *pConn = utest_fixture->pConn;
	xdb_res_t *pRes = xdb_exec (pConn, "DROP TABLE student");
	ASSERT_EQ (pRes->errcode, XDB_OK);	
	xdb_close (pConn);
}

#define CHECK_STUDENT(pRow, id, name, age, cls, score)	\
	ASSERT_EQ (*(int*)pRow[0], 1001);	\
	ASSERT_STREQ ((char*)pRow[1], name);	\
	ASSERT_EQ (*(uint16_t*)(pRow[1]-2), strlen(name));	\
	ASSERT_EQ (*(int*)pRow[2], age);	\
	ASSERT_STREQ ((char*)pRow[3], cls);	\
	ASSERT_EQ (*(uint16_t*)(pRow[3]-2), strlen(cls));	\
	ASSERT_EQ (*(float*)pRow[4], score);

#define CHECK_STUDENT_API(pMeta, pRow, id, name, age, cls, score)	\
	ASSERT_EQ (xdb_column_int (pMeta, pRow, 0), 1001);	\
	ASSERT_STREQ (xdb_column_str (pMeta, pRow, 1), name);	\
	ASSERT_STREQ (xdb_column_str2 (pMeta, pRow, 1, &len), name);	\
	ASSERT_EQ (len, strlen(name));	\
	ASSERT_EQ (xdb_column_int (pMeta, pRow, 2), age);	\
	ASSERT_STREQ (xdb_column_str2 (pMeta, pRow, 3, &len), cls);	\
	ASSERT_EQ (len, strlen(cls));	\
	ASSERT_EQ (*(float*)pRow[4], score);

UTEST_I(XdbTest, insert_one, 1)
{
	int len;
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_exec (pConn, "INSERT INTO student (id,name,age,class,score) VALUES (1001,'jack',10,'3-1',90.5)");
	ASSERT_EQ (pRes->errcode, XDB_OK);	
	ASSERT_EQ (pRes->affected_rows, 1);

	pRes = xdb_exec (pConn, "SELECT * from student");
	ASSERT_EQ (pRes->errcode, XDB_OK);	
	ASSERT_EQ (pRes->row_count, 1);
	pRow = xdb_fetch_row (pRes);
	ASSERT_TRUE (pRow != NULL);
	CHECK_STUDENT (pRow, 1001, "jack", 10, "3-1", 90.5);
	CHECK_STUDENT_API (pRes->col_meta, pRow, 1001, "jack", 10, "3-1", 90.5);
	pRow = xdb_fetch_row (pRes);
	ASSERT_TRUE (pRow == NULL);
	xdb_free_result (pRes);
}

UTEST_I(XdbTest, update_one, 1)
{
	int len;
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_exec (pConn, "INSERT INTO student (id,name,age,class,score) VALUES (1001,'jack',10,'3-1',90.5)");
	ASSERT_EQ (pRes->errcode, XDB_OK);	
	ASSERT_EQ (pRes->affected_rows, 1);

	pRes = xdb_exec (pConn, "UPDATE student SET age=age+1 WHERE id=1001");
	ASSERT_EQ (pRes->errcode, XDB_OK);	
	ASSERT_EQ (pRes->affected_rows, 1);	

	pRes = xdb_exec (pConn, "SELECT * from student");
	ASSERT_EQ (pRes->errcode, XDB_OK);	
	ASSERT_EQ (pRes->row_count, 1);
	pRow = xdb_fetch_row (pRes);
	ASSERT_TRUE (pRow != NULL);
	CHECK_STUDENT (pRow, 1001, "jack", 10 + 1, "3-1", 90.5);
	CHECK_STUDENT_API (pRes->col_meta, pRow, 1001, "jack", 10 + 1, "3-1", 90.5);
	pRow = xdb_fetch_row (pRes);
	ASSERT_TRUE (pRow == NULL);
	xdb_free_result (pRes);
}

#if 0
UTEST_I(XdbTest, insert_many, 1) {
	xdb_conn_t *pConn = utest_fixture->pConn;
	xdb_res_t *pRes = xdb_exec (pConn, "INSERT INTO student (id,name,age,class,score) VALUES (1,'jack',10,'3-1',90),(2,'tom',11,'2-5',91),(3,'jack',11,'1-6',92),(4,'rose',10,'4-2',90),(5,'tim',10,'3-1',95)");
	ASSERT_TRUE(pRes->errcode == XDB_OK);	
	ASSERT_TRUE(pRes->affected_rows == 5);
}
#endif

#if 0

UTEST_I(XdbTest, agg_count, 1) {
	xdb_conn_t *pConn = utest_fixture->pConn;
	xdb_res_t *pRes = xdb_exec (pConn, "INSERT INTO student (id,name,age,class,score) VALUES (1,'jack',10,'3-1',90),(2,'tom',11,'2-5',91),(3,'jack',11,'1-6',92),(4,'rose',10,'4-2',90),(5,'tim',10,'3-1',95)");
	ASSERT_TRUE(pRes->errcode == XDB_OK);	
	ASSERT_TRUE(pRes->affected_rows == 5);
}
#endif

UTEST_MAIN()
