UTEST_I(XdbTest, trans_commit_insert, 2)
{
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	xdb_ret rc = xdb_begin (pConn);
	ASSERT_EQ (rc, XDB_OK);

	pRes = xdb_bexec (pConn, "INSERT INTO student (id,name,age,height,weight,class,score) VALUES ("STU2_1000"),("STU2_1001"),("STU2_1002"),("STU2_1003"),("STU2_1004"),("STU2_1005"),("STU2_1006")");
	CHECK_AFFECT (pRes, 7);

	rc = xdb_commit (pConn);
	ASSERT_EQ (rc, XDB_OK);

	pRes = xdb_bexec (pConn, "SELECT * FROM student");
	CHECK_QUERY(pRes, 7);		
}

UTEST_I(XdbTest, trans_rollback_insert, 2)
{
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	xdb_ret rc = xdb_begin (pConn);
	ASSERT_EQ (rc, XDB_OK);

	pRes = xdb_bexec (pConn, "INSERT INTO student (id,name,age,height,weight,class,score) VALUES ("STU2_1000"),("STU2_1001"),("STU2_1002"),("STU2_1003"),("STU2_1004"),("STU2_1005"),("STU2_1006")");
	CHECK_AFFECT (pRes, 7);

	rc = xdb_rollback (pConn);
	ASSERT_EQ (rc, XDB_OK);

	pRes = xdb_bexec (pConn, "SELECT * FROM student");
	CHECK_QUERY(pRes, 0);		
}

UTEST_I(XdbTestRows, trans_commit_update, 2)
{
	int len;
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	xdb_ret rc = xdb_begin (pConn);
	ASSERT_EQ (rc, XDB_OK);

	pRes = xdb_exec (pConn, "UPDATE student SET age=age+1 WHERE name='jack'");
	CHECK_AFFECT (pRes, 3);

	rc = xdb_commit (pConn);
	ASSERT_EQ (rc, XDB_OK);

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE name='jack'");
	CHECK_QUERY(pRes, 3, stu.age += 1);
	// others are not affected
	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE name!='jack'");
	CHECK_QUERY(pRes, 4);
}

UTEST_I(XdbTestRows, trans_rollback_update, 2)
{
	int len;
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	xdb_ret rc = xdb_begin (pConn);
	ASSERT_EQ (rc, XDB_OK);

	pRes = xdb_exec (pConn, "UPDATE student SET age=age+1 WHERE name='jack'");
	CHECK_AFFECT (pRes, 3);

	rc = xdb_rollback (pConn);
	ASSERT_EQ (rc, XDB_OK);

	pRes = xdb_exec (pConn, "SELECT * FROM student");
	CHECK_QUERY(pRes, 7);
}

UTEST_I(XdbTestRows, trans_commit_delete, 2)
{
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	xdb_ret rc = xdb_begin (pConn);
	ASSERT_EQ (rc, XDB_OK);

	pRes = xdb_bexec (pConn, "DELETE FROM student WHERE name=?", "jack");
	CHECK_AFFECT (pRes, 3);

	rc = xdb_commit (pConn);
	ASSERT_EQ (rc, XDB_OK);

	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE name='jack'");
	CHECK_QUERY(pRes, 0);

	pRes = xdb_exec (pConn, "SELECT * FROM student");
	CHECK_QUERY(pRes, 4);
}

UTEST_I(XdbTestRows, trans_rollback_delete, 2)
{
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	xdb_ret rc = xdb_begin (pConn);
	ASSERT_EQ (rc, XDB_OK);

	pRes = xdb_bexec (pConn, "DELETE FROM student WHERE name=?", "jack");
	CHECK_AFFECT (pRes, 3);

	rc = xdb_rollback (pConn);
	ASSERT_EQ (rc, XDB_OK);

	pRes = xdb_exec (pConn, "SELECT * FROM student");
	CHECK_QUERY(pRes, 7);
}

UTEST_I(XdbTestRows, trans_commit, 2)
{
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_exec (pConn, "BEGIN");
	ASSERT_EQ (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_bexec (pConn, "INSERT INTO student (id,name,age,height,weight,class,score) VALUES ("STU2_1007")");
	CHECK_AFFECT (pRes, 1);
	pRes = xdb_bexec (pConn, "UPDATE student SET age=age+? WHERE id=?", 1, 1001);
	CHECK_AFFECT (pRes, 1);
	pRes = xdb_pexec (pConn, "DELETE FROM student WHERE id=%d", 1003);
	CHECK_AFFECT (pRes, 1);

	pRes = xdb_exec (pConn, "COMMIT");
	ASSERT_EQ (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_bexec (pConn, "SELECT * FROM student WHERE id=1001");
	CHECK_QUERY(pRes, 1, stu.age+=1);		
	pRes = xdb_bexec (pConn, "SELECT * FROM student WHERE id=1003");
	CHECK_QUERY(pRes, 0);		
	pRes = xdb_bexec (pConn, "SELECT * FROM student WHERE id=1007");
	CHECK_QUERY(pRes, 1);		
	pRes = xdb_bexec (pConn, "SELECT * FROM student WHERE id!=1001");
	CHECK_QUERY(pRes, 6);	
}

UTEST_I(XdbTestRows, trans_rollback, 2)
{
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_exec (pConn, "BEGIN");
	ASSERT_EQ (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_bexec (pConn, "INSERT INTO student (id,name,age,height,weight,class,score) VALUES ("STU2_1007")");
	CHECK_AFFECT (pRes, 1);
	pRes = xdb_bexec (pConn, "UPDATE student SET age=age+? WHERE id=?", 1, 1001);
	CHECK_AFFECT (pRes, 1);
	pRes = xdb_pexec (pConn, "DELETE FROM student WHERE id=%d", 1003);
	CHECK_AFFECT (pRes, 1);

	pRes = xdb_exec (pConn, "ROLLBACK");
	ASSERT_EQ (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_bexec (pConn, "SELECT * FROM student WHERE id=1007");
	CHECK_QUERY(pRes, 0);		
	pRes = xdb_bexec (pConn, "SELECT * FROM student");
	CHECK_QUERY(pRes, 7);
}

UTEST_I(XdbTestRows, trans_commit_dup, 2)
{
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_exec (pConn, "BEGIN");
	ASSERT_EQ (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_bexec (pConn, "INSERT INTO student (id,name,age,height,weight,class,score) VALUES ("STU2_1007")");

	CHECK_AFFECT (pRes, 1);
	pRes = xdb_bexec (pConn, "INSERT INTO student (id,name,age,height,weight,class,score) VALUES ("STU2_1007")");
	CHECK_AFFECT (pRes, 0);

	pRes = xdb_exec (pConn, "UPDATE student SET age=age+1 WHERE id=1007");
	CHECK_AFFECT (pRes, 1);
	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE id=1007");
	CHECK_QUERY (pRes, 1, stu.age+=1);	

	pRes = xdb_exec (pConn, "UPDATE student SET id=1001 WHERE id=1007");
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_exec (pConn, "DELETE FROM student WHERE id=1007");
	CHECK_AFFECT (pRes, 1);

	pRes = xdb_exec (pConn, "COMMIT");
	ASSERT_EQ (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_bexec (pConn, "SELECT * FROM student");
	CHECK_QUERY(pRes, 7);
}

UTEST_I(XdbTestRows, trans_rollback_dup, 2)
{
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_exec (pConn, "BEGIN");
	ASSERT_EQ (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_bexec (pConn, "INSERT INTO student (id,name,age,height,weight,class,score) VALUES ("STU2_1007")");

	CHECK_AFFECT (pRes, 1);
	pRes = xdb_bexec (pConn, "INSERT INTO student (id,name,age,height,weight,class,score) VALUES ("STU2_1007")");
	CHECK_AFFECT (pRes, 0);

	pRes = xdb_exec (pConn, "UPDATE student SET age=age+1 WHERE id=1007");
	CHECK_AFFECT (pRes, 1);
	pRes = xdb_exec (pConn, "SELECT * FROM student WHERE id=1007");
	CHECK_QUERY (pRes, 1, stu.age+=1);	

	pRes = xdb_exec (pConn, "UPDATE student SET id=1001 WHERE id=1007");
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_exec (pConn, "DELETE FROM student WHERE id=1007");
	CHECK_AFFECT (pRes, 1);

	pRes = xdb_exec (pConn, "ROLLBACK");
	ASSERT_EQ (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_bexec (pConn, "SELECT * FROM student");
	CHECK_QUERY(pRes, 7);
}
