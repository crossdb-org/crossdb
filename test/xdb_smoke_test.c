#include "utest.h"
#include <crossdb.h>

typedef struct {
	int 	id;
	char 	*name;
	char 	age;
	float 	height;
	double	 weight;
	char 	*cls;
	short	score;
} student_t;

#define STU_1000	1000, "jack", 11, 1.50, 41.2, "6-1", 93
#define STU_1001	1001, "rose", 11, 1.55, 39.5, "6-4", 92
#define STU_1002	1002, "tom",  12, 1.55, 42.9, "6-1", 95
#define STU_1003	1003, "wendy",11, 1.55, 41.7, "6-3", 94
#define STU_1004	1004, "jack", 10, 1.45, 40.2, "6-2", 92
#define STU_1005	1005, "tom",  11, 1.55, 41.9, "6-1", 94
#define STU_1006	1006, "jack", 11, 1.55, 40.2, "6-2", 92
#define STU_1007	1007, "tom",  12, 1.55, 40.5, "6-1", 93

#define STU2_1000	"1000, 'jack', 11, 1.50, 41.2, '6-1', 93"
#define STU2_1001	"1001, 'rose', 11, 1.55, 39.5, '6-4', 92"
#define STU2_1002	"1002, 'tom',  12, 1.55, 42.9, '6-1', 95"
#define STU2_1003	"1003, 'wendy',11, 1.55, 41.7, '6-3', 94"
#define STU2_1004	"1004, 'jack', 10, 1.45, 40.2, '6-2', 92"
#define STU2_1005	"1005, 'tom',  11, 1.55, 41.9, '6-1', 94"
#define STU2_1006	"1006, 'jack', 11, 1.55, 40.2, '6-2', 92"
#define STU2_1007	"1007, 'tom',  12, 1.55, 40.5, '6-1', 93"

#define STU2_1000_DUP	"1000, 'jack', 12, 1.55, 40.2, '6-2', 92"

student_t stu_info[] = {
	{STU_1000},
	{STU_1001},
	{STU_1002},
	{STU_1003},
	{STU_1004},
	{STU_1005},
	{STU_1006},
	{STU_1007},
};

struct XdbTest {
	xdb_conn_t *pConn;
};

struct XdbTestRows {
	xdb_conn_t *pConn;
};

UTEST_I_SETUP(XdbTest)
{
	xdb_conn_t *pConn = xdb_open (utest_index ? "testdb" : ":memory:");
	ASSERT_TRUE (pConn!=NULL);
	utest_fixture->pConn = pConn;
	xdb_res_t *pRes = xdb_exec (pConn, "CREATE TABLE student (id INT PRIMARY KEY, name VARCHAR(32), age TINYINT, height FLOAT, weight DOUBLE, class CHAR(16), score SMALLINT)");
	ASSERT_EQ_MSG (pRes->errcode, XDB_OK, xdb_errmsg(pRes));
}
UTEST_I_TEARDOWN(XdbTest)
{
	xdb_conn_t *pConn = utest_fixture->pConn;
	xdb_res_t *pRes = xdb_exec (pConn, "DROP TABLE student");
	ASSERT_EQ_MSG (pRes->errcode, XDB_OK, xdb_errmsg(pRes));
	xdb_close (pConn);
}

UTEST_F_SETUP(XdbTest)
{
	xdb_conn_t *pConn = xdb_open (":memory:");
	ASSERT_TRUE (pConn!=NULL);
	utest_fixture->pConn = pConn;
	xdb_res_t *pRes = xdb_exec (pConn, "CREATE TABLE student (id INT PRIMARY KEY, name VARCHAR(32), age TINYINT, height FLOAT, weight DOUBLE, class CHAR(16), score SMALLINT)");
	ASSERT_EQ_MSG (pRes->errcode, XDB_OK, xdb_errmsg(pRes));
}
UTEST_F_TEARDOWN(XdbTest)
{
	xdb_conn_t *pConn = utest_fixture->pConn;
	xdb_res_t *pRes = xdb_exec (pConn, "DROP TABLE student");
	ASSERT_EQ_MSG (pRes->errcode, XDB_OK, xdb_errmsg(pRes));
	xdb_close (pConn);
}

UTEST_I_SETUP(XdbTestRows)
{
	xdb_conn_t *pConn = xdb_open (utest_index ? "testdb" : ":memory:");
	ASSERT_TRUE (pConn!=NULL);
	utest_fixture->pConn = pConn;
	xdb_res_t *pRes = xdb_exec (pConn, "CREATE TABLE student (id INT PRIMARY KEY, name VARCHAR(32), age TINYINT, height FLOAT, weight DOUBLE, class CHAR(16), score SMALLINT)");
	ASSERT_EQ_MSG (pRes->errcode, XDB_OK, xdb_errmsg(pRes));

	pRes = xdb_bexec (pConn, "INSERT INTO student (id,name,age,height,weight,class,score) VALUES (?,?,?,?,?,?,?),(?,?,?,?,?,?,?),(?,?,?,?,?,?,?),(?,?,?,?,?,?,?),(?,?,?,?,?,?,?),(?,?,?,?,?,?,?),(?,?,?,?,?,?,?)",
								STU_1000, STU_1001, STU_1002, STU_1003, STU_1004, STU_1005, STU_1006);
	ASSERT_EQ_MSG (pRes->errcode, XDB_OK, xdb_errmsg(pRes));
	ASSERT_EQ (pRes->affected_rows, 7);
}
UTEST_I_TEARDOWN(XdbTestRows)
{
	xdb_conn_t *pConn = utest_fixture->pConn;
	xdb_res_t *pRes = xdb_exec (pConn, "DROP TABLE student");
	ASSERT_EQ_MSG (pRes->errcode, XDB_OK, xdb_errmsg(pRes));
	xdb_close (pConn);
}

#define STU_ID(pRow)	(*(int*)pRow[0])
#define STU_NAME(pRow)	((char*)pRow[1])
#define STU_AGE(pRow)	(*(char*)pRow[2])

#define CHECK_STUDENT(pRow, stu)	\
	ASSERT_EQ (*(int*)pRow[0], stu.id);	\
	ASSERT_STREQ ((char*)pRow[1], stu.name);	\
	ASSERT_EQ (*(uint16_t*)(pRow[1]-2), strlen(stu.name));	\
	ASSERT_EQ (*(char*)pRow[2], stu.age);	\
	ASSERT_NEAR (*(float*)pRow[3], stu.height, 0.000001);	\
	ASSERT_NEAR (*(double*)pRow[4], stu.weight, 0.000001);	\
	ASSERT_STREQ ((char*)pRow[5], stu.cls);	\
	ASSERT_EQ (*(uint16_t*)(pRow[5]-2), strlen(stu.cls));	\
	ASSERT_EQ (*(short*)pRow[6], stu.score);

#define CHECK_STUDENT_API(pMeta, pRow, stu)	\
	ASSERT_EQ (xdb_column_int (pMeta, pRow, 0), stu.id);	\
	ASSERT_STREQ (xdb_column_str (pMeta, pRow, 1), stu.name);	\
	ASSERT_STREQ (xdb_column_str2 (pMeta, pRow, 1, &len), stu.name);	\
	ASSERT_EQ (len, strlen(stu.name));	\
	ASSERT_EQ (xdb_column_int (pMeta, pRow, 2), stu.age);	\
	ASSERT_NEAR (xdb_column_float (pMeta, pRow, 3), stu.height, 0.000001);	\
	ASSERT_NEAR (xdb_column_double (pMeta, pRow, 4), stu.weight, 0.000001);	\
	ASSERT_STREQ (xdb_column_str2 (pMeta, pRow, 5, &len), stu.cls);	\
	ASSERT_EQ (len, strlen(stu.cls));	\
	ASSERT_EQ (xdb_column_int (pMeta, pRow, 6), stu.score);

#define CHECK_QUERY(pRes, num, action...)	\
do {	\
	int count;	\
	bool id_mark[8] = {0};	\
	ASSERT_EQ_MSG (pRes->errcode, XDB_OK, xdb_errmsg(pRes));	\
	ASSERT_EQ (pRes->row_count, num);	\
	for (count = 0; (pRow = xdb_fetch_row (pRes)); count++) {	\
		int len;	\
		ASSERT_EQ(id_mark[STU_ID(pRow) - 1000], false);	\
		id_mark[STU_ID(pRow) - 1000] = true;	\
		student_t stu = stu_info[STU_ID(pRow) - 1000];	\
		action;	\
		CHECK_STUDENT (pRow, stu);	\
		CHECK_STUDENT_API (pRes->col_meta, pRow, stu);	\
	}	\
	ASSERT_EQ (pRes->row_count, count);	\
	xdb_free_result (pRes);	\
} while (0)

#define CHECK_EXP(pRes, num, action...)	\
	do {	\
		int count;	\
		ASSERT_EQ_MSG (pRes->errcode, XDB_OK, xdb_errmsg(pRes));	\
		ASSERT_EQ (pRes->row_count, num);	\
		for (count = 0; (pRow = xdb_fetch_row (pRes)); count++) {	\
			action; \
		}	\
		ASSERT_EQ (pRes->row_count, count); \
		xdb_free_result (pRes); \
	} while (0)

#define CHECK_QUERY_ONE(pRes, stu, action...)	\
do {	\
	int count;	\
	ASSERT_EQ_MSG (pRes->errcode, XDB_OK, xdb_errmsg(pRes));	\
	ASSERT_EQ (pRes->row_count, 1);	\
	for (count = 0; (pRow = xdb_fetch_row (pRes)); count++) {	\
		int len;	\
		action;	\
		CHECK_STUDENT (pRow, stu);	\
		CHECK_STUDENT_API (pRes->col_meta, pRow, stu);	\
	}	\
	ASSERT_EQ (pRes->row_count, count);	\
	xdb_free_result (pRes);	\
} while (0)

#define CHECK_AFFECT(pRes, num, action...)	\
	ASSERT_EQ_MSG (pRes->errcode, XDB_OK, xdb_errmsg(pRes));	\
	ASSERT_EQ (pRes->affected_rows, num);	\
	action;

#include "xdb_smoke_ddl.c"
#include "xdb_smoke_dml.c"
#include "xdb_smoke_func.c"
#include "xdb_smoke_trans.c"
#include "xdb_smoke_semantic.c"

UTEST_I(XdbTestRows, sysdb_check, 2)
{
}

UTEST_MAIN()
