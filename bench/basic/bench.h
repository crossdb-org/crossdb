#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>

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
	if (qps > 1000000) {
		sprintf (pBuf, "%2d,%03d,%03d", qps/1000000, (qps/1000)%1000, qps%1000);
	} else {
		sprintf (pBuf, "%03d,%03d", qps/1000, qps%1000);
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

static int s_rand_id[100000000];

typedef struct {
	uint32_t	insert_qps;
	uint32_t	query_qps;
	uint32_t	update_qps;
	uint32_t	delete_qps;
} bench_result_t;

void* bench_create ();
void bench_close (void *pConn);
void bench_sql_test (void *pConn, int STU_COUNT, bool bRand, bench_result_t *pResult);
void bench_pstmt_test (void *pConn, int STU_COUNT, bool bRand, bench_result_t *pResult);

const char *s_name[] = {"Jack", "Tom", "Rose", "Tina", "Wendy", "David", "Adam", "Mose", "Paul", "Joseph"};
const char *s_class[] = {"5-2", "5-6", "5-3", "5-8", "5-7", "5-4", "5-1", "5-9", "5-10", "5-5"};

#define STU_ID(id)		bRand?s_rand_id[i]:i%STU_COUNT
#define STU_NAME(id)	s_name[id%10]
#define STU_CLASS(id)	s_class[id%10]
#define STU_AGE(id)		(10+id%3)
#define STU_SCORE(id)	(90+id%10)


static void print_result (uint32_t rows, const char *type, bench_result_t *pResult, bool bCharts)
{
	printf ("\n####################### %d Rows %s Test Result ###############################\n", rows, type);

	printf ("\n %8s | %8s | %10s | %10s | %10s | %10s\n", "DB", "Access", "INSERT QPS", "QUERY QPS", "UPDATE QPS", "DELETE QPS");
	for (int i = 0; i < 2; ++i) {
		printf (" %8s | %8s | %10s | %10s | %10s | %10s\n", BENCH_DBNAME, i?"PSTMT":"SQL", 
			qps2str(pResult[i].insert_qps), qps2str(pResult[i].query_qps), 
			qps2str(pResult[i].update_qps), qps2str(pResult[i].delete_qps));
	}
	if (bCharts) {
		for (int i = 0; i < 2; ++i) {
			printf ("label: '%s %s', data:[ %10d,%10d,%10d,%10d],\n", BENCH_DBNAME, i?"PSTMT":"SQL",
				pResult[i].insert_qps, pResult[i].query_qps, 
				pResult[i].update_qps, pResult[i].delete_qps);
		}
	}
}

int main (int argc, char **argv)
{
	int 	STU_COUNT = (argc == 2) ? atoi(argv[1]) : 1000000;
	int		ch, round = 1;
	bool 	bCharts = false;

	if (argc >= 2) {
		while ((ch = getopt(argc, argv, "n:r:qjh")) != -1) {
			switch (ch) {
			case 'h':
				printf ("Usage:\n");
	            printf ("  -h                        show this help\n");
	            printf ("  -n <row count>            default 1000000\n");
	            printf ("  -r <round count>          test round, default 1\n");
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
			case 'j':
				bCharts = true;
				break;
			case 'q':
				s_quiet = true;
				break;
			}
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

	void *pConn = bench_create ();
	if (NULL == pConn) {
		goto error;
	}

	bench_result_t result[4] = {0};

	for (int i = 0; i < round; ++i) {
		printf ("\n$$$$$$$$$$$$$$$$$$$$$$$$$$$ Round %d $$$$$$$$$$$$$$$$$$$$$$$$$$$\n", i+1);

		bench_print ("\n******************** %10s Test *********************\n", "Sequential");

		bench_sql_test   (pConn, STU_COUNT, false, &result[0]);
		bench_pstmt_test (pConn, STU_COUNT, false, &result[1]);

		bench_print ("\n\n********************* %10s Test *********************\n", "Random");

		bench_sql_test   (pConn, STU_COUNT, true, &result[2]);
		bench_pstmt_test (pConn, STU_COUNT, true, &result[3]);
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
