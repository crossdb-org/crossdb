UTEST_I(XdbTestRows, agg_count, 2)
{
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_exec (pConn, "SELECT COUNT(*) FROM student");
	CHECK_EXP(pRes, 1, ASSERT_EQ(xdb_column_int(pRes, pRow, 0), 7));

	pRes = xdb_exec (pConn, "SELECT COUNT(id) FROM student");
	CHECK_EXP(pRes, 1, ASSERT_EQ(xdb_column_int(pRes, pRow, 0), 7));

	pRes = xdb_exec (pConn, "SELECT COUNT(*) FROM student WHERE id > 1003");
	CHECK_EXP(pRes, 1, ASSERT_EQ(xdb_column_int(pRes, pRow, 0), 3));

	pRes = xdb_exec (pConn, "SELECT COUNT(id) FROM student WHERE id > 1003");
	CHECK_EXP(pRes, 1, ASSERT_EQ(xdb_column_int(pRes, pRow, 0), 3));
}

UTEST_I(XdbTestRows, agg_minmax, 2)
{
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_exec (pConn, "SELECT MIN(id) FROM student");
	CHECK_EXP(pRes, 1, ASSERT_EQ(xdb_column_int(pRes, pRow, 0), 1000));

	pRes = xdb_exec (pConn, "SELECT MAX(id) FROM student");
	CHECK_EXP(pRes, 1, ASSERT_EQ(xdb_column_int(pRes, pRow, 0), 1006));

	pRes = xdb_exec (pConn, "SELECT MIN(id),MAX(id) FROM student");
	CHECK_EXP(pRes, 1, ASSERT_EQ(xdb_column_int(pRes, pRow, 0), 1000); ASSERT_EQ(xdb_column_int(pRes, pRow, 1), 1006));

	pRes = xdb_exec (pConn, "SELECT MIN(id),MAX(id) FROM student WHERE id > 1001");
	CHECK_EXP(pRes, 1, ASSERT_EQ(xdb_column_int(pRes, pRow, 0), 1002); ASSERT_EQ(xdb_column_int(pRes, pRow, 1), 1006));
}

UTEST_I(XdbTestRows, agg_sumavg, 2)
{
	xdb_res_t *pRes;
	xdb_row_t *pRow;
	xdb_conn_t *pConn = utest_fixture->pConn;

	pRes = xdb_exec (pConn, "SELECT SUM(id) FROM student");
	int sum = 1000+1001+1002+1003+1004+1005+1006;
	CHECK_EXP(pRes, 1, ASSERT_EQ(xdb_column_int(pRes, pRow, 0), sum));

	pRes = xdb_exec (pConn, "SELECT AVG(id) FROM student");
	CHECK_EXP(pRes, 1, ASSERT_NEAR(xdb_column_double(pRes, pRow, 0), (double)sum/7, 0.000001));

	pRes = xdb_exec (pConn, "SELECT SUM(id),AVG(id) FROM student");
	CHECK_EXP(pRes, 1, ASSERT_EQ(xdb_column_int(pRes, pRow, 0), sum); ASSERT_NEAR(xdb_column_double(pRes, pRow, 1), (double)sum/7, 0.000001));

	pRes = xdb_exec (pConn, "SELECT SUM(id),AVG(id) FROM student WHERE id > 1001");
	sum -= (1000 + 1001);
	CHECK_EXP(pRes, 1, ASSERT_EQ(xdb_column_int(pRes, pRow, 0), sum); ASSERT_NEAR(xdb_column_double(pRes, pRow, 1), (double)sum/5, 0.000001));
}

