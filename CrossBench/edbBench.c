#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <inttypes.h>
#include <ctype.h>
#include <unistd.h>
#include <pthread.h>

typedef enum {
	EDBM_ASYNC,
	EDBM_SYNC,
	EDBM_NOSYNC,
} edbmSync_e;

typedef struct {
	const char *name;
	int dbInit (int keyMaxlen, int valMaxLen);
	void* dbOpenCtx ();
	void dbCloseCtx (void *ctx);
	void* dbBegin (bool bWrite);
	int dbCommit (bool bWrite, void *pPriv);
	int dbPut (uint8_t *pKey, int klen, int val);
	int dbGet (uint8_t *pKey, int klen, int *pVal);
	int dbSet (uint8_t *pKey, int klen, int val);
	int dbDel (uint8_t *pKey, int klen);
} edbmDrvOps_t;

typedef struct {
	bool 		bInMem;
	bool		bExclusive;
	edbmSync_e	syncMode;
	bool		bTransation;
	bool		bWalOn;
	bool		bAutoLock;
	int			threadNum;

	bool		bKeyInt;	// is key int
	int			rowCount;
	int			batchRows;
	int			updateRatio;
	int			execTime;
} edbmCfg_t ;

static pthread_rwlock_t s_edbmRwlock = PTHREAD_RWLOCK_INITIALIZER;

static inline void edbm_rdlock () {
	pthread_rwlock_rdlock (&s_edbmRwlock);
}
static inline void edbm_rdunlock () {
	pthread_rwlock_unlock (&s_edbmRwlock);
}
static inline void edbm_wrlock () {
	pthread_rwlock_wrlock (&s_edbmRwlock);
}
static inline void edbm_wrunlock () {
	pthread_rwlock_unlock (&s_edbmRwlock);
}	

#define EDBM_MAX_THREAD		256
#define EDBM_MAX_DB			10

#ifdef EDBM_LMDB
#include "lmdb_drv.c"
#endif
#ifdef EDBM_SQLITE3
#include "sqlite3_drv.c"
#endif
#ifdef EDBM_GLIBC
#include "glibc_drv.c"
#endif
#ifdef EDBM_CPPSTL
#include "cppstl_drv.c"
#endif

static int 	*idSetKey;
static int 	*idSetVal;

static edbmDrvOps_t *s_edbmDrv_list[EDBM_MAX_DB] = {
#ifdef EDBM_LMDB
	lmdb_EDBMDrv,
#endif
#ifdef EDBM_SQLITE3
	sqlite3_EDBMDrv,
#endif
#ifdef EDBM_GLIBC
	glibc_EDBMDrv,
#endif
#ifdef EDBM_CPPSTL
	cppstl_EDBMDrv,
#endif
} edbmResult_t;


edbmCfg_t s_edbmCfg = {
	.dbType 	= EDBM_TMPFS,
	.bExclusive	= true,
	.syncMode	= EDBM_ASYNC,

	.bTransation= false,
	.bWalOn 	= true,

	.keyType 	= EDBM_KEYINT,

	.rowCount	= 1000000,

	.batchRows 	= 1,
	.execTime	= 30,
	.updateRatio= 0,

	.bAutoLock 	= true,
	.threadNum	= 1
};

static volatile int s_EDBMThreadStop = 0;

static bool s_EDBMUpdate[100] = {false};

typedef struct {
	int				id;
	edbmDrvOps_t	*pDbDrv;
	edbmCfg_t		*pCfg;
	void			*pDbEnv;
	void			*pDbArg;
	uint64_t		counter;
	uint64_t		rsvd[2];
} edbmThread_env;

static inline uint32_t edbm_intGet (int id)
{
	return (id^0x38761d64) | (id&3)<<30;
}

static inline char* edbm_strGet (int id, char *out)
{
	static const char *type[] = {"macaddr","ipv4addr","ipv6addr","vlanid"};
	sprintf (out, "%s-%d", type[id&3], id);
	return out;
}


uint64_t edbm_dumpSpeed (uint64_t lastCounter, uint64_t lastTS)
{
	static uint64_t counter;
	uint64_t TS;
	if (0 == lastCounter) {
		lastCounter = counter;
		lastTS = TS;
	}

	counter = 0;
	for (int i = 0; i < )


	TS = edbm_timestamp ();
	
}

static void *edbm_thread(void *arg) 
{
	uint32_t	id = 0; 
	EDBMThread_env	*pEnv = (EDBMThread_env*)arg;

	while (s_EDBMThreadStop >= 0) {
		while (s_EDBMThreadStop) {
			usleep (100);
		}
		while (!s_EDBMThreadStop) {
			if (!s_EDBMUpdate[(id++)%100]) {
				// get
				pEnv->pDbDrv->dbTranBegin (false);
				for (int i = 0; i < s_edbmCfg.batchRow; ++i) {
					int val;
					pEnv->pDbDrv->dbGet (idSetKey[(id+i)%s_edbmCfg.rowCount], &val);
					pEnv->counter++;
				}
				pEnv->pDbDrv->dbTransEnd (false);
			} else {
				// update		
				pEnv->pDbDrv->dbTranBegin (true);
				for (int i = 0; i < s_edbmCfg.batchRow; ++i) {
					pEnv->pDbDrv->dbSet (idSetKey[(id+i)%s_edbmCfg.rowCount], (int)pEnv->pCounter);
					pEnv->pCounter++;
				}
				pEnv->pDbDrv->dbTransEnd (true);
			}
		}
	}
}

uint64_t edbm_timestamp ()
{
    struct timeval time;
    gettimeofday(&time,NULL);
	return (uint64_t)time.tv_sec * 1000000 + time.tv_usec;
}

void edbm_dumpCfg ()
{
	printf ("  DB Type		  : %s\n", EDBM_TMPFS==s_edbmCfg.dbType ? "TMPFS" : (EDBM_INMEM==s_edbmCfg.dbType ? "INMEM" : "ONDISK"));
	printf ("  Access Mode	  : %s\n", s_edbmCfg.bExclusive?"Exclusive":"Shared");
	printf ("  Sync Mode	  : %s\n", EDBM_ASYNC==s_edbmCfg.syncMode? "ASYNC" : (EDBM_SYNC==s_edbmCfg.syncMode ? "SYNC" : "EDBM_NOSYNC"));
	printf ("  \n");
	printf ("  Transaction	  : %s\n", s_edbmCfg.bTransation?"Yes":"No");
	printf ("  WAL			  : %s\n", s_edbmCfg.bWalOn?"On":"Off");
	printf ("  \n");
	printf ("  Key Type 	  : %s\n", EDBM_KEYINT==s_edbmCfg.keyType ? "INT" : (EDBM_KEYCHAR==s_edbmCfg.keyType ? "CHAR" : "BYTE"));
	printf ("  Index Type	  : %s\n", EDBM_IDXDFT==s_edbmCfg.idxType ? "Default" : (EDBM_IDXHASH==s_edbmCfg.idxType ? "Hash" : "Tree"));
	printf ("  Row Number	  : %d\n", s_edbmCfg.rowCount);
	printf ("  \n");
	printf ("  Access Order   : %s\n", s_edbmCfg.bRandom ? "Random" : "Sequential");
	printf ("  Batch Rows	  : %d\n", s_edbmCfg.batchRows);
	printf ("  Execution Time : %ds\n", s_edbmCfg.execTime);
	printf ("  Update Ratio   : %d%\n", s_edbmCfg.updateRatio);
	printf ("  \n");
	printf ("  AutoLock 	  : %s\n", s_edbmCfg.bAutoLock?"Yes":"No");
	printf ("  Thread Core: %d\n", s_edbmCfg.threadNum);
}

int main (int argc, char **argv)
{
	int		i;
	char	ch;
	char	*dbList = NULL;
	
	int 	dbList[EDBM_MAX_DB];
	int		dbCount = 0;
	
	edbmDrvOps_t *pDbDrv = NULL;

	if (argc < 2) {
        printf ("Embedded Database Benchmark Parameters (first option is default)\n");

        printf ("  -D <dbName,dbName,...>      \n");
        printf ("    Supported DB:");
		for (i = 0; s_edbmDrv_list[i]!=NULL; ++i) {
			printf (" %s", s_edbmDrv_list[i].name);
		}
        printf ("\n");
        printf ("  -d <db type>            tmpfs | inmem | ondisk\n");
        printf ("  -m <access mode>        exclusive | shared\n");
        printf ("  -s <sync mode>          async | sync | nosync\n");
        printf ("  \n");
        printf ("  -T <transaction>        notrans | trans\n");
        printf ("  -w <walmode>            walon | waloff\n");
        printf ("  \n");
        printf ("  -k <key type>           int | string | byte\n");
        printf ("  -i <index type>         default | hash | tree\n");
        printf ("  -n <row number>         1M (unit: K M B)\n");
        printf ("  \n");
        printf ("  -o <access order>       random | sequential\n");
        printf ("  -b <batch rows>         1\n");
        printf ("  -e <execution time>     30\n");
        printf ("  -u <update ratio>       0    (0~100)\n");
        printf ("  \n");
        printf ("  -a <autolock>           autolock | nolock\n");
        printf ("  -t <thread count>       1    (1~256)\n");
		return 0;
	}

	while ((ch = getopt(argc, argv, "D:d:m:s:T:w:k:i:n:o:b:e:u:a:t:")) != -1) {
		switch (ch) {
		case 'D':
			dbList = optarg;
			char *token = strtok(str, ", ");
			while (token != NULL) {
				for (i = 0; s_edbmDrv_list[i]!=NULL; ++i) {
					if (!strcmp (token, s_edbmDrv_list[i].name) {
						break;
					}
				}
				token = strtok(NULL, ", ");
			}
			if (NULL == s_edbmDrv_list[i]) {
				printf ("Unkown database %d\n", token);
				return -1;
			}
			dbList[dbCount++] = i;
			break;
		case 'd':
			if (!strcmp (optarg, "inmem")) { 
				s_edbmCfg.dbType = EDBM_INMEM;
			} else if (!strcmp ("optarg", "ondisk")) { 
				s_edbmCfg.dbType = EDBM_ONDISK; 
			}
			break;
		case 'm':
			if (!strcmp (optarg, "shared")) { 
				s_edbmCfg.bExclusive = false; 
			}
			break;
		case 's':
			if (!strcmp (optarg, "sync")) { 
				s_edbmCfg.dbType = EDBM_SYNC;
			} else if (!strcmp (optarg, "nosync")) { 
				s_edbmCfg.dbType = EDBM_NOSYNC; 
			}
			break;
		case 'T':
			if (!strcmp (optarg, "trans")) { 
				s_edbmCfg.bTransation = true; 
			}
			break;
		case 'w':
			if (!strcmp (optarg, "waloff")) { 
				s_edbmCfg.bWalOn = false; 
			}
			break;
		case 'k':
			if (!strcmp (optarg, "string")) { 
				s_edbmCfg.keyType = EDBM_KEYCHAR;
			} else if (!strcmp (optarg, "byte")) { 
				s_edbmCfg.dbType = EDBM_KEYBYTE; 
			}
			break;
		case 'i':
			if (!strcmp (optarg, "hash")) { 
				s_edbmCfg.idxType = EDBM_IDXHASH;
			} else if (!strcmp (optarg, "tree")) { 
				s_edbmCfg.idxType = EDBM_IDXHASH; 
			}
			break;
		case 'n':
			s_edbmCfg.rowCount = atoi (optarg);
			switch (toupper(optarg[strlen(optarg)-1])) {
			case 'K':
				s_edbmCfg.rowCount *= 1000;
				break;
			case 'M':
				s_edbmCfg.rowCount *= 1000000;
				break;
			case 'B':
				s_edbmCfg.rowCount *= 1000000000;
				break;
			default:
				printf ("Vaild row count example: 100K 8M 40M\n");
				return -1;
			}
			break;
		case 'o':
			if (!strcmp (optarg, "sequential")) { 
				s_edbmCfg.bRandom = false; 
			}
			break;
		case 'b':
			s_edbmCfg.batchRows = atoi (optarg);
			if (s_edbmCfg.batchRows<=0 ) { 
				printf ("Invalid batch rows: %d\n", s_edbmCfg.batchRows); 
				return -1; 
			}
			break;
		case 'e':
			s_edbmCfg.execTime = atoi (optarg);
			if (s_edbmCfg.execTime<=0 ) { printf ("Invalid execution time: %d\n", s_edbmCfg.execTime); return -1; }
			break;
		case 'u':
			s_edbmCfg.updateRatio = atoi (optarg);
			if (s_edbmCfg.updateRatio<=0 || s_edbmCfg.updateRatio>100) { 
				printf ("Invalid update ratio: %d\n", s_edbmCfg.updateRatio); return -1; 
			}
			break;
		case 'a':
			if (!strcmp (optarg, "nolock")) { s_edbmCfg.bAutoLock = false; }
			break;
		case 't':
			s_edbmCfg.threadNum = atoi (optarg);
			if (s_edbmCfg.threadNum<=0 || s_edbmCfg.threadNum>1024) { printf ("Invalid cpu core: %d\n", s_edbmCfg.threadNum); return -1; }
			break;
		default:
			printf ("Unknown option: -%s\n", ch);
			return -1;
		}
	}
	if (dbCount <= 0) {
		printf ("No Test DB!\n");
		return -1;
	}

	srand (time(NULL));

	edbmResult_t edbmRes[];
	if (NULL == pDbRes) {
		printf ("No memory!\n");
		return -1;
	}
	idSetKey = calloc (s_edbmCfg.rowCount, sizeof(int));
	if (NULL == idSetKey) {
		printf ("No memory!\n");
		return -1;
	}
	idSetVal = calloc (s_edbmCfg.rowCount, sizeof(int));
	if (NULL == idSetVal) {
		printf ("No memory!\n");
		return -1;
	}
	for (i = 0; i < s_edbmCfg.rowCount; ++i) {
		idSetVal[i] = rand ();
	}
	for (i = 0; i < s_edbmCfg.updateRatio; ++i) {
		s_EDBMUpdate[i] = true;
	}

	printf ("Embedded Database Benchmark Parameters\n");

    printf ("  DB Name        :", );
	for (i = 0; i < dbCount; ++i) {
		printf (" %s", s_edbmDrv_list[dbList[i]].name);
	}

	edbm_dumpCfg ();

	edbmThread_env	threadEnv[EDBM_MAX_THREAD];
	edbmDrvOps_t	*pDbDrv = NULL;

	idSetKey[i];

	// create thread
	for (i = 0; i< s_edbmCfg.threadNum; ++i) {
		pthread_t thread;
		threadEnv[i].id = i;
		pthread_create(&thread, NULL, ia_doer_thread, doer);
	}

	for (i = 0; i < dbCount; ++i) {
		pDbDrv = s_edbmDrv_list[dbList[i]];

		for (i = 0; i< s_edbmCfg.threadNum; ++i) {
			threadEnv[i].counter = 0;
			threadEnv[i].pDbDrv = pDbDrv;
		}

		printf ("###############################################################################\n");
		printf ("# Test %s\n", s_pDbDrv->name);
		printf ("###############################################################################\n");

		printf ("Init DB\n");
		pDbDrv->dbInit ();

		printf ("Load Data %d\n", s_edbmDrv_list[dbList[i]].name);
		uint64_t	ts_beg = edbm_timestamp ();
		for (int r = 0; r < s_edbmCfg.rowCount; r++) {
			pDbDrv->dbInsert (r, idSetVal[r]);
		}
		uint64_t	ts_beg = edbm_timestamp ();

		// set pointer
		
		// start run 10s
		sleep (10);
		
		// collect 1st counter
		while () {
			sleep (5);
			// print onece
		}
		// save last result

		// stop running

		printf ("UnLoad Data %d\n", s_edbmDrv_list[dbList[i]].name);
	}

	edbm_dumpCfg ();
	
	// Print Result
	for (i = 0; i < dbCount; ++i) {
		printf ("%15s", );
	}
	for (i = 0; i < dbCount; ++i) {
		printf ("============== ");
	}
	for (i = 0; i < dbCount; ++i) {
		printf ("%15u", );
	}

	// Sum up (scan)

	return 0;
}
