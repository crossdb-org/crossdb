#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>
#include <sched.h>
#include <pthread.h>

#define SQL_LKUP_COUNT	LKUP_COUNT/5
#define UPD_COUNT		LKUP_COUNT/10

#define BENCH_CHECK(expr, action...)	if (!(expr)) { action; }

#ifndef TEST_NAME
#define TEST_NAME(i) i?"STMT":"SQL"
#endif

static uint64_t s_last_ts;

bool s_quiet = false;

static uint64_t timestamp_us () 
{ 
	struct timeval tv; gettimeofday(&tv, NULL); 
	return tv.tv_sec * 1000000LL + tv.tv_usec; 
}

static void bench_ts_beg() 
{
	s_last_ts = timestamp_us ();
}

#define ORDER_STR(bRand)	bRand?"Random":"Sequential"

#define bench_print(fmt...)	\
	if (!s_quiet) {	\
		printf (fmt);	\
	}

const char* qps2str (uint32_t qps)
{
	static 	uint32_t id;
	static char buf[8][16];
	char	*pBuf = buf[id++%8];
	if (qps >= 1000000) {
		snprintf (pBuf, sizeof(buf[0]), "%2d,%03d,%03d", qps/1000000, (qps/1000)%1000, qps%1000);
	} else {
		snprintf (pBuf, sizeof(buf[0]), "%3d,%03d", qps/1000, qps%1000);
	}
	return pBuf;
}

static uint32_t bench_ts_end (int count) 
{
	s_last_ts = timestamp_us() - s_last_ts; 
	uint32_t qps = (int)((uint64_t)count*1000000/s_last_ts);
	bench_print ("Use time %7uus, QPS %s\n", (uint32_t)s_last_ts, qps2str(qps)); 
	return qps;
}

static int bind_cpu (int cpuid)
{
#ifdef __linux__
	cpu_set_t  mask;
	CPU_ZERO(&mask);
	CPU_SET(cpuid, &mask);
	if (sched_setaffinity(0, sizeof(mask), &mask) < 0) {
		printf ("Invalid cpuid %d\n", cpuid);
		perror ("sched_setaffinity");
		return -1;
	}
	if (pthread_setaffinity_np(pthread_self(), sizeof(mask), &mask) < 0) {
		perror("pthread_setaffinity_np");
		return -1;
	}
	return 0;
#else
	printf ("This platform doesn't support cpu bind!\n");
	return -1;
#endif
}

#ifndef __cplusplus
const char *s_name[] = {"Jack", "Tom", "Rose", "Tina", "Wendy", "David", "Adam", "Mose", "Paul", "Joseph"};
const char *s_class[] = {"5-2", "5-6", "5-3", "5-8", "5-7", "5-4", "5-1", "5-9", "5-10", "5-5"};
#else
string s_name[] = {"Jack", "Tom", "Rose", "Tina", "Wendy", "David", "Adam", "Mose", "Paul", "Joseph"};
string s_class[] = {"5-2", "5-6", "5-3", "5-8", "5-7", "5-4", "5-1", "5-9", "5-10", "5-5"};
#endif

#define STU_BASEID		10000000
#define STU_ID(id)		((bRand?s_rand_id[id]:id%STU_COUNT)+STU_BASEID)
#define STU_NAME(id)	s_name[id%10]
#define STU_CLASS(id)	s_class[id%10]
#define STU_AGE(id)		(10+id%3)
#define STU_SCORE(id)	(90+id%10)

static int s_rand_id[100000000];

typedef struct {
	uint32_t	insert_qps;
	uint32_t	query_qps;
	uint32_t	update_qps;
	uint32_t	delete_qps;
} bench_result_t;

#ifndef __cplusplus

typedef void (*stu_callback) (void *pArg, int id, const char *name, int age, const char *cls, int score);
void stu_count_cb (void *pArg, int id, const char *name, int age, const char *cls, int score)
{
	int *pCount = (int*)pArg;
	(*pCount)++;
	(void)id, (void)name, (void)age, (void)cls, (void)score;
}

#else

typedef void (*stu_callback) (void *pArg, int id, string &name, int age, string &cls, int score);
void stu_count_cb (void *pArg, int id, string &name, int age, string &cls, int score)
{
	int *pCount = (int*)pArg;
	(*pCount)++;
	(void)id, (void)name, (void)age, (void)cls, (void)score;
}

#endif

#define BENCH_SQL_CREATE		"CREATE TABLE student (id INT PRIMARY KEY, name CHAR(16), age INT, class CHAR(16), score INT)"
#define BENCH_SQL_DROP			"DROP TABLE IF EXISTS student"
#define BENCH_SQL_INSERT		"INSERT INTO student (id,name,age,class,score) VALUES (?,?,?,?,?)"
#define BENCH_SQL_GET_BYID		"SELECT * FROM student WHERE id=?"
#define BENCH_SQL_UDPAGE_BYID	"UPDATE student SET age=? WHERE id=?"
#define BENCH_SQL_DEL_BYID		"DELETE FROM student WHERE id=?"


void* bench_open (const char *db);
void bench_close (void *pConn);
bool bench_sql (void *pConn, const char *sql);
#ifndef __cplusplus
bool bench_sql_insert (void *pConn, const char *sql, int id, const char *name, int age, const char *cls, int score);
#else
bool bench_sql_insert (void *pConn, const char *sql, int id, string &name, int age, string &cls, int score);
#endif
bool bench_sql_get_byid (void *pConn, const char *sql, int id, stu_callback callback, void *pArg);
bool bench_sql_updAge_byid (void *pConn, const char *sql, int id, int age);
bool bench_sql_del_byid (void *pConn, const char *sql, int id);

void* bench_stmt_prepare (void *pConn, const char *sql);
void bench_stmt_close (void *pStmt);
#ifndef __cplusplus
bool bench_stmt_insert (void *pStmt, int id, const char *name, int age, const char *cls, int score);
#else
bool bench_stmt_insert (void *pStmt, int id, string &name, int age, string &cls, int score);
#endif
bool bench_stmt_updAge_byid (void *pStmt, int id, int age);
bool bench_stmt_del_byid (void *pStmt, int id);
bool bench_stmt_get_byid (void *pStmt, int id, stu_callback callback, void *pArg);

void bench_sql_test (void *pConn, int STU_COUNT, bool bRand, bench_result_t *pResult)
{
	bool ok;

	bench_sql (pConn, BENCH_SQL_DROP);
	bench_sql (pConn, BENCH_SQL_CREATE);

	bench_print ("\n[============= %s Test =============]\n\n", TEST_NAME(0));

	bench_print ("------------ INSERT %s ------------\n", qps2str(STU_COUNT));
	bench_ts_beg();
	for (int i = 0; i < STU_COUNT; ++i) {
		ok = bench_sql_insert (pConn, BENCH_SQL_INSERT, STU_BASEID+i, STU_NAME(i), STU_AGE(i), STU_CLASS(i), STU_SCORE(i));
		BENCH_CHECK (ok, bench_print ("Can't insert student id=%d\n", STU_BASEID+i); return;);
	}
	pResult->insert_qps += bench_ts_end (STU_COUNT);

	bench_print ("------------ %s LKUP %s ------------\n", ORDER_STR(bRand), qps2str(SQL_LKUP_COUNT));
	uint64_t qps_sum = 0;
	for (int t = 0; t < 5; ++t) {
		int count = 0;
		bench_ts_beg();
		for (int i = 0; i < SQL_LKUP_COUNT; ++i) {
			ok = bench_sql_get_byid (pConn, BENCH_SQL_GET_BYID, STU_ID(i), stu_count_cb, &count);
			BENCH_CHECK (ok, bench_print ("Can't get student id=%d\n", STU_ID(i)); return;);
		}
		qps_sum += bench_ts_end (SQL_LKUP_COUNT);
		BENCH_CHECK (count == SQL_LKUP_COUNT, bench_print ("OK %d != LKUP %d\n", count, SQL_LKUP_COUNT););
	}
	pResult->query_qps += qps_sum / 5;

	bench_print ("------------ %s UPDATE %s ------------\n", ORDER_STR(bRand), qps2str(UPD_COUNT));
	bench_ts_beg();
	for (int i = 0; i < UPD_COUNT; ++i) {
		ok = bench_sql_updAge_byid (pConn, BENCH_SQL_UDPAGE_BYID, STU_ID(i), 10+i%20);
		BENCH_CHECK (ok, bench_print ("Can't update student id=%d\n", STU_ID(i)); return;);
	}
	pResult->update_qps += bench_ts_end (UPD_COUNT);

	bench_print ("------------ %s DELETE %s ------------\n", ORDER_STR(bRand), qps2str(STU_COUNT));
	bench_ts_beg();
	for (int i = 0; i < STU_COUNT; ++i) {
		ok = bench_sql_del_byid (pConn, BENCH_SQL_DEL_BYID, STU_ID(i));
		BENCH_CHECK (ok, bench_print ("Can't delete student id=%d\n", STU_ID(i)); return;);
	}
	pResult->delete_qps += bench_ts_end (STU_COUNT);
}

void bench_stmt_test (void *pConn, int STU_COUNT, bool bRand, bench_result_t *pResult)
{
	bool	ok;
	void 	*pStmt = NULL;
	uint64_t qps_sum = 0;

	bench_sql (pConn, BENCH_SQL_DROP);
	bench_sql (pConn, BENCH_SQL_CREATE);

	bench_print ("\n[============= %s Test =============]\n", TEST_NAME(1));

	bench_print ("\n------------ INSERT %s ------------\n", qps2str(STU_COUNT));
	pStmt = bench_stmt_prepare (pConn, BENCH_SQL_INSERT);
	BENCH_CHECK (pStmt != NULL, bench_print ("Can't prepare '%s'\n", BENCH_SQL_INSERT); goto error);
	bench_ts_beg();
	for (int i = 0; i < STU_COUNT; ++i) {
		ok = bench_stmt_insert (pStmt, STU_BASEID+i, STU_NAME(i), STU_AGE(i), STU_CLASS(i), STU_SCORE(i));
		BENCH_CHECK (ok, bench_print ("Can't insert student id=%d\n", STU_BASEID+i); return;);
	}
	pResult->insert_qps += bench_ts_end (STU_COUNT);
	bench_stmt_close (pStmt);

	bench_print ("------------ %s LKUP %s ------------\n", ORDER_STR(bRand), qps2str(LKUP_COUNT));
	pStmt = bench_stmt_prepare (pConn, BENCH_SQL_GET_BYID);
	BENCH_CHECK (pStmt != NULL, bench_print ("Can't prepare '%s'\n", BENCH_SQL_INSERT); goto error);
	for (int t = 0; t < 5; ++t) {
		int count = 0;
		bench_ts_beg();
		for (int i = 0; i < LKUP_COUNT; ++i) {
			ok = bench_stmt_get_byid (pStmt, STU_ID(i), stu_count_cb, &count);
			BENCH_CHECK (ok, bench_print ("Can't get student id=%d\n", STU_ID(i)););
		}
		qps_sum += bench_ts_end (LKUP_COUNT);
		BENCH_CHECK (count == LKUP_COUNT, bench_print ("OK %d != LKUP %d\n", count, LKUP_COUNT););
	}
	bench_stmt_close (pStmt);
	pResult->query_qps += qps_sum / 5;
	
	bench_print ("------------ %s UPDATE %s ------------\n", ORDER_STR(bRand), qps2str(UPD_COUNT));
	pStmt = bench_stmt_prepare (pConn, BENCH_SQL_UDPAGE_BYID);
	BENCH_CHECK (pStmt != NULL, bench_print ("Can't prepare '%s'\n", BENCH_SQL_INSERT); goto error);
	bench_ts_beg();
	for (int i = 0; i < UPD_COUNT; ++i) {
		ok = bench_stmt_updAge_byid (pStmt, STU_ID(i), 10+i%20);
		BENCH_CHECK (ok, bench_print ("Can't update student id=%d\n", STU_ID(i)); goto error;);
	}
	pResult->update_qps += bench_ts_end (UPD_COUNT);
	bench_stmt_close (pStmt);

	bench_print ("------------ %s DELETE %s ------------\n", ORDER_STR(bRand), qps2str(STU_COUNT));
	pStmt = bench_stmt_prepare (pConn, BENCH_SQL_DEL_BYID);
	BENCH_CHECK (pStmt != NULL, bench_print ("Can't prepare '%s'\n", BENCH_SQL_INSERT); goto error);
	bench_ts_beg();
	for (int i = 0; i < STU_COUNT; ++i) {
		ok = bench_stmt_del_byid (pStmt, STU_ID(i));
		BENCH_CHECK (ok, bench_print ("Can't delete student id=%d\n", STU_ID(i)); goto error;);
	}
	pResult->delete_qps += bench_ts_end (STU_COUNT);

error:	
	bench_stmt_close (pStmt);
}

static void print_result (uint32_t rows, const char *type, bench_result_t *pResult, bool bCharts)
{
	printf ("####################### %s Rows %s Test Result ###############################\n", qps2str(rows), type);

	printf (" %8s | %8s | %10s | %10s | %10s | %10s\n", "DB", "Access", "INSERT QPS", "QUERY QPS", "UPDATE QPS", "DELETE QPS");
	for (int i = 0; i < 2; ++i) {
		printf (" %8s | %8s | %10s | %10s | %10s | %10s\n", BENCH_DBNAME, TEST_NAME(i), 
			qps2str(pResult[i].insert_qps), qps2str(pResult[i].query_qps), 
			qps2str(pResult[i].update_qps), qps2str(pResult[i].delete_qps));
	}
	if (bCharts) {
		for (int i = 0; i < 2; ++i) {
			printf ("        {label: '%s %s', data:[%d, %d, %d, %d], borderWidth: 1, borderRadius: 10},\n", BENCH_DBNAME, TEST_NAME(i),
				pResult[i].insert_qps, pResult[i].query_qps, 
				pResult[i].update_qps, pResult[i].delete_qps);
		}
	}
}

int main (int argc, char **argv)
{
	int 	STU_COUNT = (argc == 2) ? atoi(argv[1]) : 1000000;
	int		ch, round = 1, cpuid = -1;
	bool 	bCharts = false;
	const	char *db = ":memory:";

	if (argc >= 2) {
		while ((ch = getopt(argc, argv, "n:r:c:d:qjh")) != -1) {
			switch (ch) {
			case 'h':
				printf ("Usage:\n");
	            printf ("  -h                        show this help\n");
	            printf ("  -n <row count>            default 1000000\n");
	            printf ("  -r <round count>          test round, default 1\n");
				printf ("  -c <cpu core>             bind cpu core\n");
	            printf ("  -q                        quite mode\n");
				return -1;
			case 'n':
				STU_COUNT = atoi (optarg);
				break;
			case 'r':
				round = atoi (optarg);
				if (0 == round) {
					round = 1;
				}
				break;
			case 'c':
				cpuid = atoi (optarg);
				break;
			case 'd':
				db = optarg;
				break;
			case 'j':
				bCharts = true;
				break;
			case 'q':
				s_quiet = true;
				break;
			}
		}
	}

	if (cpuid >= 0) {
		if (0 == bind_cpu (cpuid)) {
			bench_print ("\nRun test on CPU %d\n", cpuid);
		}
	}

	if (0 == STU_COUNT) {
		STU_COUNT = 1000000;
	}

	for (int i = 0; i < STU_COUNT; ++i) {
		s_rand_id[i] = i;
	}
	for (int i = 0; i < STU_COUNT; ++i) {
		int a = rand()%STU_COUNT;
		int b = rand()%STU_COUNT;
		if (a != b) {
			int tmp = s_rand_id[a];
			s_rand_id[a] = s_rand_id[b];
			s_rand_id[b] = tmp;
		}
	}
	for (int i = STU_COUNT; i < LKUP_COUNT; ++i) {
		s_rand_id[i] = rand()%STU_COUNT;
	}

	void *pConn = bench_open (db);
	if (NULL == pConn) {
		goto error;
	}

	bench_result_t result[4];
	memset (&result, 0, sizeof(result));

	for (int i = 0; i < round; ++i) {
		bench_print ("$$$$$$$$$$$$$$$$$$$$$$$$$$$ Round %d $$$$$$$$$$$$$$$$$$$$$$$$$$$\n", i+1);

		bench_print ("\n******************** %10s Test *********************\n", "Sequential");

		bench_sql_test   (pConn, STU_COUNT, false, &result[0]);
		bench_stmt_test (pConn, STU_COUNT, false, &result[1]);

		bench_print ("\n\n********************* %10s Test *********************\n", "Random");

		bench_sql_test   (pConn, STU_COUNT, true, &result[2]);
		bench_stmt_test (pConn, STU_COUNT, true, &result[3]);
	}
	
	for (int i = 0; i < 4; ++i) {
		result[i].insert_qps /= round;
		result[i].query_qps /= round;
		result[i].update_qps /= round;
		result[i].delete_qps /= round;
	}

	print_result (STU_COUNT, "Sequential", &result[0], bCharts);

	print_result (STU_COUNT, "Random", &result[2], bCharts);

error:
	bench_close (pConn);

	return 0;
}
