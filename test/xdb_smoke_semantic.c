UTEST_F(XdbTest, create_db_wrong_semantic)
{
	xdb_res_t *pRes;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_bexec (pConn, "CREATE2 DATABASE");
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);
	pRes = xdb_bexec (pConn, "CREATE2 DATABASE xdb");
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);
	pRes = xdb_bexec (pConn, "CREATE DATABASE2 xdb");
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);
	pRes = xdb_bexec (pConn, "CREATE DATABASE IF2 NOT EXISTS xdb");
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);
	pRes = xdb_bexec (pConn, "CREATE DATABASE IF NOT2 EXISTS xdb");
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);
	pRes = xdb_bexec (pConn, "CREATE DATABASE IF NOT2 EXISTS2 xdb");
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);
}

UTEST_F(XdbTest, drop_db_wrong_semantic)
{
	xdb_res_t *pRes;
	xdb_conn_t *pConn = utest_fixture->pConn;
	pRes = xdb_bexec (pConn, "DROP DATABASE xdb");
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);
}

UTEST_F(XdbTest, create_table_wrong_semantic)
{
}

UTEST_F(XdbTest, drop_table_wrong_semantic)
{
}

UTEST_F(XdbTest, create_idx_semantic)
{
}

UTEST_F(XdbTest, drop_idx_semantic)
{
}

UTEST_F(XdbTest, insert_wrong_semantic)
{
	int len;
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_bexec (pConn, "INSERT2 INTO student (id,name,age,height,weight,class,score) VALUES ("STU2_1000")");
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_bexec (pConn, "INSERT INTO2 student (id,name,age,height,weight,class,score) VALUES ("STU2_1000")");
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_bexec (pConn, "INSERT2 INTO student2 (id,name,age,height,weight,class,score) VALUES ("STU2_1000")");
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_bexec (pConn, "INSERT2 INTO student (id2,name,age,height,weight,class,score) VALUES ("STU2_1000")");
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_bexec (pConn, "INSERT2 INTO student (id,name,age,height,weight,class,score) VALUES ("STU2_1000")");
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_bexec (pConn, "INSERT2 INTO student (id,name,age,height,weight,class,score) VALUES2 ("STU2_1000")");
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);
}

UTEST_F(XdbTest, query_wrong_semantic)
{
	int len;
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_pexec (pConn, "SELECT2 * FROM student WHERE id=%d", 1100);
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_pexec (pConn, "SELECT name,age2 FROM student WHERE id=%d", 1100);
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_pexec (pConn, "SELECT age2+5 FROM student WHERE id=%d", 1100);
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_pexec (pConn, "SELECT COUNT2(*) FROM student WHERE id=%d", 1100);
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_pexec (pConn, "SELECT MAX(*) FROM student WHERE id=%d", 1100);
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_pexec (pConn, "SELECT MAX2(id) FROM student WHERE id=%d", 1100);
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_pexec (pConn, "SELECT * FROM2 student WHERE id=%d", 1100);
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_bexec (pConn, "SELECT * FROM student2 WHERE id=?", 1100);
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_bexec (pConn, "SELECT * FROM student WHERE2 id=?", 1100);
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_bexec (pConn, "SELECT * FROM student WHERE id2=?", 1100);
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);
}

UTEST_F(XdbTest, update_wrong_semantic)
{
	int len;
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_exec (pConn, "UPDATE2 student SET age=age+1 WHERE id=1101");
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_exec (pConn, "UPDATE student2 SET age=age+1 WHERE id=1101");
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_exec (pConn, "UPDATE student2 SET2 age=age+1 WHERE id=1101");
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_exec (pConn, "UPDATE student SET age2=age+1 WHERE id=1101");
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_exec (pConn, "UPDATE student SET age=age2+1 WHERE id=1101");
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_exec (pConn, "UPDATE student SET age=age+1 WHERE2 id=1101");
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_exec (pConn, "UPDATE student SET age=age+1 WHERE2 id2=1101");
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);
}

UTEST_F(XdbTest, delete_wrong_semantic)
{
	int len;
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_pexec (pConn, "DELETE2 FROM student WHERE id=%d", 1101);
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_pexec (pConn, "DELETE FROM2 student WHERE id=%d", 1101);
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_pexec (pConn, "DELETE FROM student2 WHERE id=%d", 1101);
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_pexec (pConn, "DELETE FROM student WHERE2 id=%d", 1101);
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_pexec (pConn, "DELETE FROM student WHERE id2=%d", 1101);
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);
}

UTEST_F(XdbTest, agg_wrong_semantic)
{
	int len;
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_pexec (pConn, "SELECT COUNT2(*) FROM student WHERE id=%d", 1100);
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_pexec (pConn, "SELECT COUNT(id2) FROM student WHERE id=%d", 1100);
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_pexec (pConn, "SELECT MIN2(*) FROM student WHERE id=%d", 1100);
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_pexec (pConn, "SELECT MIN(id2) FROM student WHERE id=%d", 1100);
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_pexec (pConn, "SELECT MAX2(*) FROM student WHERE id=%d", 1100);
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_pexec (pConn, "SELECT MAX(id2) FROM student WHERE id=%d", 1100);
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_pexec (pConn, "SELECT AVG2(*) FROM student WHERE id=%d", 1100);
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);

	pRes = xdb_pexec (pConn, "SELECT AVG(id2) FROM student WHERE id=%d", 1100);
	ASSERT_NE (xdb_errcode(pRes), XDB_OK);
}
