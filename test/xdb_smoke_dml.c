UTEST_I(XdbTest, insert_one, 2)
{
	xdb_conn_t *pConn = utest_fixture->pConn;
	xdb_res_t *pRes = xdb_bexec (pConn, "INSERT INTO student (id,name,age,height,weight,class,score) VALUES ("STU2_1000")");
	CHECK_AFFECT (pRes, 1);
}

UTEST_I(XdbTestRows, insert_dup, 2)
{
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_bexec (pConn, "INSERT INTO student (id,name,age,height,weight,class,score) VALUES ("STU2_1000_DUP")");
	CHECK_AFFECT (pRes, 0);

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE id=1000");
	CHECK_QUERY (pRes, 1, ASSERT_EQ(STU_ID(pRow),1000));	
}

UTEST_I(XdbTest, insert_many, 2)
{
	xdb_conn_t *pConn = utest_fixture->pConn;
	xdb_res_t *pRes = xdb_bexec (pConn, "INSERT INTO student (id,name,age,height,weight,class,score) VALUES ("STU2_1000"),("STU2_1001"),("STU2_1002"),("STU2_1003"),("STU2_1004"),("STU2_1005"),("STU2_1006")");
	CHECK_AFFECT (pRes, 7);
}

UTEST_I(XdbTestRows, query_one, 2)
{
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE id=1000");
	CHECK_QUERY (pRes, 1, ASSERT_EQ(STU_ID(pRow),1000));
}

UTEST_I(XdbTestRows, query_all, 2)
{
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_exec (pConn, "SELECT * FROM student");
	CHECK_QUERY (pRes, 7);

	pRes = xdb_exec (pConn, "SELECT id,name,age,height,weight,class,score FROM student");
	CHECK_QUERY (pRes, 7);
}

UTEST_I(XdbTestRows, query_each, 2)
{
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	for (int i = 0; i < 7; ++i) {
		pRes = xdb_bexec (pConn, "SELECT * FROM student WHERE id=?", 1000 + i);
		CHECK_QUERY (pRes, 1, ASSERT_EQ(STU_ID(pRow),1000+i));
	}
}

UTEST_I(XdbTestRows, query_exp, 2)
{
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_exec (pConn, "SELECT age+5,age FROM student");
	CHECK_EXP (pRes, 7, ASSERT_EQ(xdb_column_int(pRes, pRow, 0), xdb_column_int(pRes, pRow, 1) + 5));

	pRes = xdb_exec (pConn, "SELECT age+score,age,score FROM student");
	CHECK_EXP (pRes, 7, ASSERT_EQ(xdb_column_int(pRes, pRow, 0), xdb_column_int(pRes, pRow, 1) + xdb_column_int(pRes, pRow, 2)));
}

UTEST_I(XdbTestRows, query_many_id, 2)
{
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE id>1004");
	CHECK_QUERY (pRes, 2, ASSERT_GT(STU_ID(pRow),1004));

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE id>=1004");
	CHECK_QUERY (pRes, 3, ASSERT_GE(STU_ID(pRow),1004));

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE id<1004");
	CHECK_QUERY (pRes, 4, ASSERT_LT(STU_ID(pRow),1004));

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE id<=1004");
	CHECK_QUERY (pRes, 5, ASSERT_LE(STU_ID(pRow),1004));

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE id!=1004");
	CHECK_QUERY (pRes, 6, ASSERT_NE(STU_ID(pRow),1004));

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE id<>1004");
	CHECK_QUERY (pRes, 6, ASSERT_NE(STU_ID(pRow),1004));
}

UTEST_I(XdbTestRows, query_or, 2)
{
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE id>1004 OR name='jack'");
	CHECK_QUERY (pRes, 4, ASSERT_TRUE ((STU_ID(pRow)>1004) || !strcmp(STU_NAME(pRow),"jack")));
}

UTEST_I(XdbTestRows, query_limit, 2)
{
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_exec (pConn, "SELECT * FROM student LIMIT 2");
	CHECK_QUERY (pRes, 2);

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE id>1003 LIMIT 2");
	CHECK_QUERY (pRes, 2, ASSERT_GT(STU_ID(pRow),1003));
}

UTEST_I(XdbTestRows, query_order, 2)
{
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	int age = 0;
	pRes = xdb_exec (pConn, "SELECT * FROM student ORDER BY age");
	CHECK_QUERY (pRes, 7, ASSERT_GE(STU_AGE(pRow),age); age=STU_AGE(pRow));

	age = 0;
	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE id>1003 ORDER BY age");
	CHECK_QUERY (pRes, 3, ASSERT_GT(STU_ID(pRow),1003); ASSERT_GE(STU_AGE(pRow),age); age=STU_AGE(pRow));

	age = 0;
	pRes = xdb_exec (pConn, "SELECT * FROM student ORDER BY age ASC");
	CHECK_QUERY (pRes, 7, ASSERT_GE(STU_AGE(pRow),age); age=STU_AGE(pRow));

	age = 0;
	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE id>1003 ORDER BY age ASC");
	CHECK_QUERY (pRes, 3, ASSERT_GT(STU_ID(pRow),1003); ASSERT_GE(STU_AGE(pRow),age); age=STU_AGE(pRow));

	age = 100;
	pRes = xdb_exec (pConn, "SELECT * FROM student ORDER BY age DESC");
	CHECK_QUERY (pRes, 7, ASSERT_LE(STU_AGE(pRow),age); age=STU_AGE(pRow));

	age = 100;
	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE id>1003 ORDER BY age DESC");
	CHECK_QUERY (pRes, 3, ASSERT_GT(STU_ID(pRow),1003); ASSERT_LE(STU_AGE(pRow),age); age=STU_AGE(pRow));
}

UTEST_I(XdbTestRows, query_order_limit, 2)
{
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	int age = 0;
	pRes = xdb_exec (pConn, "SELECT * FROM student ORDER BY age LIMIT 2");
	CHECK_QUERY (pRes, 2, ASSERT_GE(STU_AGE(pRow),age); age=STU_AGE(pRow));

	age = 0;
	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE id>1003 ORDER BY age LIMIT 2");
	CHECK_QUERY (pRes, 2, ASSERT_GT(STU_ID(pRow),1003); ASSERT_GE(STU_AGE(pRow),age); age=STU_AGE(pRow));

	age = 0;
	pRes = xdb_exec (pConn, "SELECT * FROM student ORDER BY age ASC LIMIT 2");
	CHECK_QUERY (pRes, 2, ASSERT_GE(STU_AGE(pRow),age); age=STU_AGE(pRow));

	age = 0;
	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE id>1003 ORDER BY age ASC LIMIT 2");
	CHECK_QUERY (pRes, 2, ASSERT_GT(STU_ID(pRow),1003); ASSERT_GE(STU_AGE(pRow),age); age=STU_AGE(pRow));

	age = 100;
	pRes = xdb_exec (pConn, "SELECT * FROM student ORDER BY age DESC LIMIT 2");
	CHECK_QUERY (pRes, 2, ASSERT_LE(STU_AGE(pRow),age); age=STU_AGE(pRow));

	age = 100;
	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE id>1003 ORDER BY age DESC LIMIT 2");
	CHECK_QUERY (pRes, 2, ASSERT_GT(STU_ID(pRow),1003); ASSERT_LE(STU_AGE(pRow),age); age=STU_AGE(pRow));
}

UTEST_I(XdbTestRows, query_page, 2)
{
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	int id = 1000;
	pRes = xdb_exec (pConn, "SELECT * FROM student LIMIT 2");
	CHECK_QUERY (pRes, 2, ASSERT_EQ(STU_ID(pRow),id++));

	pRes = xdb_exec (pConn, "SELECT * FROM student LIMIT 2 OFFSET 2");
	CHECK_QUERY (pRes, 2, ASSERT_EQ(STU_ID(pRow),id++));

	pRes = xdb_exec (pConn, "SELECT * FROM student LIMIT 4,2");
	CHECK_QUERY (pRes, 2, ASSERT_EQ(STU_ID(pRow),id++));

	pRes = xdb_exec (pConn, "SELECT * FROM student LIMIT 2 OFFSET 6");
	CHECK_QUERY (pRes, 1, ASSERT_EQ(STU_ID(pRow),id++));
}

UTEST_I(XdbTestRows, query_where_page, 2)
{
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	int id = 1002;
	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE id > 1001 LIMIT 2");
	CHECK_QUERY (pRes, 2, ASSERT_EQ(STU_ID(pRow),id++));

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE id > 1001 LIMIT 2 OFFSET 2");
	CHECK_QUERY (pRes, 2, ASSERT_EQ(STU_ID(pRow),id++));

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE id > 1001 LIMIT 4,2");
	CHECK_QUERY (pRes, 1, ASSERT_EQ(STU_ID(pRow),id++));
}

UTEST_I(XdbTestRows, query_where_order_page, 2)
{
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	int id = 1006;
	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE id > 1001 ORDER BY id DESC LIMIT 2");
	CHECK_QUERY (pRes, 2, ASSERT_EQ(STU_ID(pRow),id--));

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE id > 1001 ORDER BY id DESC LIMIT 2 OFFSET 2");
	CHECK_QUERY (pRes, 2, ASSERT_EQ(STU_ID(pRow),id--));

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE id > 1001 ORDER BY id DESC LIMIT 4,2");
	CHECK_QUERY (pRes, 1, ASSERT_EQ(STU_ID(pRow),id--));
}

UTEST_I(XdbTestRows, query_order_page, 2)
{
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	int id = 1006;
	pRes = xdb_exec (pConn, "SELECT * FROM student ORDER BY id DESC LIMIT 2");
	CHECK_QUERY (pRes, 2, ASSERT_EQ(STU_ID(pRow),id--));

	pRes = xdb_exec (pConn, "SELECT * FROM student ORDER BY id DESC LIMIT 2 OFFSET 2");
	CHECK_QUERY (pRes, 2, ASSERT_EQ(STU_ID(pRow),id--));

	pRes = xdb_exec (pConn, "SELECT * FROM student ORDER BY id DESC LIMIT 4,2");
	CHECK_QUERY (pRes, 2, ASSERT_EQ(STU_ID(pRow),id--));

	pRes = xdb_exec (pConn, "SELECT * FROM student ORDER BY id DESC LIMIT 2 OFFSET 6");
	CHECK_QUERY (pRes, 1, ASSERT_EQ(STU_ID(pRow),id--));
}

UTEST_I(XdbTestRows, query_many_name, 2)
{
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE name='jack'");
	CHECK_QUERY (pRes, 3, ASSERT_STREQ(STU_NAME(pRow),"jack"));

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE name!='jack'");
	CHECK_QUERY (pRes, 4, ASSERT_STRNE(STU_NAME(pRow),"jack"));

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE name<>'jack'");
	CHECK_QUERY (pRes, 4, ASSERT_STRNE(STU_NAME(pRow),"jack"));
}

UTEST_I(XdbTestRows, query_many_composite, 2)
{
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE name='jack' AND age>10");
	CHECK_QUERY (pRes, 2, ASSERT_STREQ(STU_NAME(pRow),"jack"); ASSERT_GT(STU_AGE(pRow),10));
}

UTEST_I(XdbTestRows, query_like, 2)
{
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE name LIKE 'jack'");
	CHECK_QUERY (pRes, 3, ASSERT_STREQ(STU_NAME(pRow),"jack"));

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE name LIKE '%ck'");
	CHECK_QUERY (pRes, 3, ASSERT_STREQ(STU_NAME(pRow),"jack"));

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE name LIKE 'j_ck'");
	CHECK_QUERY (pRes, 3, ASSERT_STREQ(STU_NAME(pRow),"jack"));

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE name LIKE 'ja%'");
	CHECK_QUERY (pRes, 3, ASSERT_STREQ(STU_NAME(pRow),"jack"));

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE name LIKE 'w%d_'");
	CHECK_QUERY (pRes, 1, ASSERT_STREQ(STU_NAME(pRow),"wendy"));

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE name LIKE 'abc'");
	CHECK_QUERY (pRes, 0);

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE class LIKE '6-_'");
	CHECK_QUERY (pRes, 7);

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE class LIKE '_-2'");
	CHECK_QUERY (pRes, 2);
}

UTEST_I(XdbTestRows, query_miss, 2)
{
	int len;
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_bexec (pConn, "SELECT * FROM student WHERE id=?", 1100);
	CHECK_QUERY (pRes, 0);

	pRes = xdb_pexec (pConn, "SELECT * FROM student WHERE id=%d", 1100);
	CHECK_QUERY (pRes, 0);
}

UTEST_I(XdbTestRows, update_one, 2)
{
	int len;
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_exec (pConn, "UPDATE student SET age=age+1 WHERE id=1001");
	CHECK_AFFECT (pRes, 1);

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE id=1001");
	CHECK_QUERY(pRes, 1, stu.age += 1);

	// other rows are not affected
	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE id!=1001");
	CHECK_QUERY(pRes, 6);
}

UTEST_I(XdbTestRows, update_exp, 2)
{
	int len;
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_exec (pConn, "UPDATE student SET score=score+age WHERE id=1001");
	CHECK_AFFECT (pRes, 1);

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE id=1001");
	CHECK_QUERY(pRes, 1, stu.score += stu.age);

	// other rows are not affected
	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE id!=1001");
	CHECK_QUERY(pRes, 6);
}

UTEST_I(XdbTestRows, update_one_pstmt, 2)
{
	int len;
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_bexec (pConn, "UPDATE student SET age=age+? WHERE id=?", 1, 1001);
	CHECK_AFFECT (pRes, 1);

	pRes = xdb_bexec (pConn, "SELECT * FROM student WHERE id=?", 1001);
	CHECK_QUERY(pRes, 1, stu.age += 1);

	pRes = xdb_bexec (pConn, "SELECT * FROM student WHERE id!=?", 1001);
	CHECK_QUERY(pRes, 6);
}

UTEST_I(XdbTestRows, update_one_fmt, 2)
{
	int len;
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_pexec (pConn, "UPDATE student SET age=age+%d WHERE id=%d", 1, 1001);
	CHECK_AFFECT (pRes, 1);

	pRes = xdb_pexec (pConn, "SELECT * FROM student WHERE id=%d", 1001);
	CHECK_QUERY(pRes, 1, stu.age += 1);

	pRes = xdb_pexec (pConn, "SELECT * FROM student WHERE id!=%d", 1001);
	CHECK_QUERY(pRes, 6);
}

UTEST_I(XdbTestRows, update_char, 2)
{
	int len;
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_pexec (pConn, "UPDATE student SET name='%s', class='%s' WHERE name='%s'", "jackson", "6-7", "jack");
	CHECK_AFFECT (pRes, 3);

	pRes = xdb_pexec (pConn, "SELECT * FROM student WHERE name='%s'", "jackson");
	CHECK_QUERY(pRes, 3, stu.name="jackson"; stu.cls="6-7";);

	pRes = xdb_bexec (pConn, "UPDATE student SET name=?, class=? WHERE name=?", "jack", "6-2", "jackson");
	CHECK_AFFECT (pRes, 3);

	pRes = xdb_bexec (pConn, "SELECT * FROM student WHERE name=?", "jack");
	CHECK_QUERY(pRes, 3, stu.cls="6-2";);
}

UTEST_I(XdbTestRows, update_pk, 2)
{
	int len;
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_exec (pConn, "UPDATE student SET id=1101 WHERE id=1001");
	CHECK_AFFECT (pRes, 1);

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE id=1001");
	CHECK_QUERY (pRes, 0);	

	student_t stu = stu_info[1];
	stu.id = 1101;
	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE id=1101");
	CHECK_QUERY_ONE(pRes, stu);

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE id!=1101");
	CHECK_QUERY (pRes, 6);	
}

UTEST_I(XdbTestRows, update_pk_dup, 2)
{
	int len;
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_exec (pConn, "UPDATE student SET id=1002 WHERE id=1001");
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_exec (pConn, "SELECT * FROM student");
	CHECK_QUERY (pRes, 7);	
}

UTEST_I(XdbTestRows, update_all, 2)
{
	int len;
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_exec (pConn, "UPDATE student SET age=age+1");
	CHECK_AFFECT (pRes, 7);

	pRes = xdb_exec (pConn, "SELECT * FROM student");
	CHECK_QUERY(pRes, 7, stu.age += 1);
}

UTEST_I(XdbTestRows, update_many, 2)
{
	int len;
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_exec (pConn, "UPDATE student SET age=age+1 WHERE name='jack'");
	CHECK_AFFECT (pRes, 3);

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE name='jack'");
	CHECK_QUERY(pRes, 3, stu.age += 1);
	// others are not affected
	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE name!='jack'");
	CHECK_QUERY(pRes, 4);
}

UTEST_I(XdbTestRows, update_miss, 2)
{
	int len;
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_exec (pConn, "UPDATE student SET age=age+1 WHERE id=1101");
	CHECK_AFFECT (pRes, 0);

	pRes = xdb_exec (pConn, "SELECT * FROM student");
	CHECK_QUERY (pRes, 7);	
}

UTEST_I(XdbTestRows, delete_one, 2)
{
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_exec (pConn, "DELETE FROM student WHERE id=1001");
	CHECK_AFFECT (pRes, 1);

	pRes = xdb_exec (pConn, "SELECT * FROM student");
	CHECK_QUERY(pRes, 6);
}

UTEST_I(XdbTestRows, delete_many, 2)
{
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_bexec (pConn, "DELETE FROM student WHERE name=?", "jack");
	CHECK_AFFECT (pRes, 3);

	pRes = xdb_exec (pConn, "SELECT * FROM student");
	CHECK_QUERY(pRes, 4);
}

UTEST_I(XdbTestRows, delete_all, 2)
{
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_exec (pConn, "DELETE FROM student");
	CHECK_AFFECT (pRes, 7);

	pRes = xdb_exec (pConn, "SELECT * FROM student");
	CHECK_QUERY(pRes, 0);
}

UTEST_I(XdbTestRows, delete_miss, 2)
{
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_pexec (pConn, "DELETE FROM student WHERE id=%d", 1101);
	CHECK_AFFECT (pRes, 0);

	pRes = xdb_exec (pConn, "SELECT * FROM student");
	CHECK_QUERY(pRes, 7);
}

UTEST_I(XdbTestRows, idx_query, 2)
{
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE name='jack'");
	CHECK_QUERY (pRes, 3, ASSERT_STREQ(stu.name, "jack"));

	pRes = xdb_exec (pConn, "CREATE INDEX idx_name ON student (name)");
	ASSERT_EQ (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE name='jack'");
	CHECK_QUERY (pRes, 3, ASSERT_STREQ(stu.name, "jack"));

	pRes = xdb_exec (pConn, "DROP INDEX idx_name ON student");
	ASSERT_EQ (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE name='jack'");
	CHECK_QUERY (pRes, 3, ASSERT_STREQ(stu.name, "jack"));
}

UTEST_I(XdbTestRows, idx_query_update, 2)
{
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE name='jack'");
	CHECK_QUERY (pRes, 3, ASSERT_STREQ(stu.name, "jack"));

	pRes = xdb_exec (pConn, "CREATE INDEX idx_name ON student (name)");
	ASSERT_EQ (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE name='jack'");
	CHECK_QUERY (pRes, 3, ASSERT_STREQ(stu.name, "jack"));

	pRes = xdb_exec (pConn, "UPDATE student SET name='jackson' WHERE name='jack'");
	CHECK_AFFECT (pRes, 3);

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE name='jack'");
	CHECK_QUERY (pRes, 0);

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE name='jackson'");
	CHECK_QUERY (pRes, 3, stu.name="jackson");

	pRes = xdb_exec (pConn, "DELETE FROM student WHERE name='jackson' LIMIT 1");
	CHECK_AFFECT (pRes, 1);

	pRes = xdb_exec (pConn, "DROP INDEX idx_name ON student");
	ASSERT_EQ (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE name='jackson'");
	CHECK_QUERY (pRes, 2, stu.name="jackson");
}

UTEST_I(XdbTestRows, idx_composite_query, 2)
{
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE name='jack' AND age=11");
	CHECK_QUERY (pRes, 2, ASSERT_STREQ(stu.name, "jack"); ASSERT_EQ(stu.age, 11));

	pRes = xdb_exec (pConn, "CREATE INDEX idx_nameage ON student (name,age)");
	ASSERT_EQ (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE name='jack' AND age=11");
	CHECK_QUERY (pRes, 2, ASSERT_STREQ(stu.name, "jack"); ASSERT_EQ(stu.age, 11));

	pRes = xdb_exec (pConn, "DROP INDEX idx_nameage ON student");
	ASSERT_EQ (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE name='jack' AND age=11");
	CHECK_QUERY (pRes, 2, ASSERT_STREQ(stu.name, "jack"); ASSERT_EQ(stu.age, 11));
}
