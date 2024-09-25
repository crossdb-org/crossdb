UTEST_I(XdbTest, create_db, 2)
{
	xdb_res_t *pRes;
	xdb_conn_t *pConn = utest_fixture->pConn;
	pRes = xdb_bexec (pConn, "CREATE DATABASE xdb");
	ASSERT_EQ (pRes->errcode, XDB_OK);
	pRes = xdb_bexec (pConn, "CREATE DATABASE xdb");
	ASSERT_NE (pRes->errcode, XDB_OK);
	pRes = xdb_bexec (pConn, "CREATE DATABASE IF NOT EXISTS xdb");
	ASSERT_EQ (pRes->errcode, XDB_OK);
	pRes = xdb_bexec (pConn, "DROP DATABASE xdb");
	ASSERT_EQ (pRes->errcode, XDB_OK);
}

UTEST_I(XdbTest, drop_db, 2)
{
	xdb_res_t *pRes;
	xdb_conn_t *pConn = utest_fixture->pConn;
	pRes = xdb_bexec (pConn, "DROP DATABASE xdb");
	ASSERT_NE (pRes->errcode, XDB_OK);
	pRes = xdb_bexec (pConn, "DROP DATABASE IF EXISTS xdb");
	ASSERT_EQ (pRes->errcode, XDB_OK);
	pRes = xdb_bexec (pConn, "CREATE DATABASE xdb");
	ASSERT_EQ (pRes->errcode, XDB_OK);
	pRes = xdb_bexec (pConn, "DROP DATABASE xdb");
	ASSERT_EQ (pRes->errcode, XDB_OK);
}

UTEST_I(XdbTest, create_table, 2)
{
	xdb_res_t *pRes;
	xdb_conn_t *pConn = utest_fixture->pConn;
	pRes = xdb_bexec (pConn, "CREATE TABLE teacher (id INT PRIMARY KEY, name CHAR(16), age TINYINT)");
	ASSERT_EQ (pRes->errcode, XDB_OK);
	pRes = xdb_bexec (pConn, "CREATE TABLE teacher (id INT PRIMARY KEY, name CHAR(16), age TINYINT)");
	ASSERT_NE (pRes->errcode, XDB_OK);
	pRes = xdb_bexec (pConn, "CREATE TABLE IF NOT EXISTS teacher (id INT PRIMARY KEY, name CHAR(16), age TINYINT)");
	ASSERT_EQ (pRes->errcode, XDB_OK);
	pRes = xdb_bexec (pConn, "DROP TABLE teacher");
	ASSERT_EQ (pRes->errcode, XDB_OK);
}

UTEST_I(XdbTest, drop_table, 2)
{
	xdb_res_t *pRes;
	xdb_conn_t *pConn = utest_fixture->pConn;
	pRes = xdb_bexec (pConn, "DROP TABLE teacher");
	ASSERT_NE (pRes->errcode, XDB_OK);
	pRes = xdb_bexec (pConn, "DROP TABLE IF EXISTS teacher");
	ASSERT_EQ (pRes->errcode, XDB_OK);
	pRes = xdb_bexec (pConn, "CREATE TABLE teacher (id INT PRIMARY KEY, name CHAR(16), age TINYINT)");
	ASSERT_EQ (pRes->errcode, XDB_OK);
	pRes = xdb_bexec (pConn, "DROP TABLE teacher");
	ASSERT_EQ (pRes->errcode, XDB_OK);
}

UTEST_I(XdbTestRows, create_idx, 2)
{
}

UTEST_I(XdbTestRows, drop_idx, 2)
{
}

