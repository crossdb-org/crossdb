#include <iostream>
#include <string>
#include <map>
#include <unordered_map>

#define BENCH_DBNAME	"STL"
#define TEST_NAME(i) 	i?"HashMap":"Map"
int LKUP_COUNT = 10000000;

using namespace std;

#include "bench.h"

class student {
public:
	int		id;
	string	name;
	int		age;
	string	cls;
	int		score;

	student () {
	}
	student (int id, string &name, int age, string &cls, int score) {
		this->id = id;
		this->name = name;
		this->age = age;
		this->cls = cls;
		this->score = score;
	}	
};

// std::shared_mutex is not used because, in a single-threaded context, the compiler optimizes the code and omits the lock.
static pthread_rwlock_t stu_tbl_lock = PTHREAD_RWLOCK_INITIALIZER;


/************************************************
		Map
 ************************************************/

static map<int, student> 			stu_tbl_map;

void* bench_open (const char *db)
{
	return &stu_tbl_map;
}

void bench_close (void *pDb)
{
}

bool bench_sql (void *pDb, const char *sql)
{
	return true;
}

student* stl_map_get_byid (int id)
{
    auto iter = stu_tbl_map.find (id);
	if (iter != stu_tbl_map.end()) {
		return &iter->second;
	} else {
		return NULL;
	}
}

bool bench_sql_insert (void *pDb, const char *sql, int id, string &name, int age, string &cls, int score)
{
	pthread_rwlock_wrlock(&stu_tbl_lock);
	auto ok = stu_tbl_map.try_emplace (id, id, name, age, cls, score);
	pthread_rwlock_unlock(&stu_tbl_lock);
	return ok.second;
}

bool bench_sql_get_byid (void *pDb, const char *sql, int id, stu_callback callback, void *pArg)
{
	student *pStu, stu;

	pthread_rwlock_rdlock(&stu_tbl_lock);	
	if (NULL != (pStu = stl_map_get_byid (id))) {
		stu = *pStu;
		pStu = &stu;
	}
	pthread_rwlock_unlock(&stu_tbl_lock);	

	if (pStu) {
		// handle row out of the lock to reduce lock time
		callback (pArg, pStu->id, pStu->name, pStu->age, pStu->cls, pStu->score);
	}
	return pStu != NULL;
}

bool bench_sql_updAge_byid (void *pDb, const char *sql, int id, int age)
{
	student *pStu;
	
	pthread_rwlock_wrlock(&stu_tbl_lock);
	if (NULL != (pStu = stl_map_get_byid (id))) {
		pStu->age = age;
	}
	pthread_rwlock_unlock(&stu_tbl_lock);	
	return pStu != NULL;
}

bool bench_sql_del_byid (void *pDb, const char *sql, int id)
{
	pthread_rwlock_wrlock(&stu_tbl_lock);
	bool ok = stu_tbl_map.erase(id);;
	pthread_rwlock_unlock(&stu_tbl_lock);
	return ok;
}


/************************************************
		HashMap (unordered_map)
 ************************************************/
 
static unordered_map<int, student> 	stu_tbl_hmap;

void* bench_stmt_prepare (void *pDb, const char *sql)
{
	return pDb;
}

void bench_stmt_close (void *pStmt)
{
}

student* stl_hmap_get_byid (int id)
{
    auto iter = stu_tbl_hmap.find (id);
	if (iter != stu_tbl_hmap.end()) {
		return &iter->second;
	} else {
		return NULL;
	}
}

bool bench_stmt_insert (void *pStmt, int id, string &name, int age, string &cls, int score)
{
	pthread_rwlock_wrlock(&stu_tbl_lock);
	auto ok = stu_tbl_hmap.try_emplace (id, id, name, age, cls, score);
	pthread_rwlock_unlock(&stu_tbl_lock);
	return ok.second;
}

bool bench_stmt_get_byid (void *pStmt, int id, stu_callback callback, void *pArg)
{
	student *pStu, stu;

	pthread_rwlock_rdlock(&stu_tbl_lock);	
	if (NULL != (pStu = stl_hmap_get_byid (id))) {
		stu = *pStu;
		pStu = &stu;
	}
	pthread_rwlock_unlock(&stu_tbl_lock);	

	if (pStu) {
		// handle row out of the lock to reduce lock time
		callback (pArg, pStu->id, pStu->name, pStu->age, pStu->cls, pStu->score);
	}
	return pStu != NULL;
}

bool bench_stmt_updAge_byid (void *pStmt, int id, int age)
{
	student *pStu;

	pthread_rwlock_wrlock(&stu_tbl_lock);
	if (NULL != (pStu = stl_hmap_get_byid (id))) {
		pStu->age = age;
	}
	pthread_rwlock_unlock(&stu_tbl_lock);	
	return pStu != NULL;
}

bool bench_stmt_del_byid (void *pStmt, int id)
{
	pthread_rwlock_wrlock(&stu_tbl_lock);
	bool ok = stu_tbl_hmap.erase(id);
	pthread_rwlock_unlock(&stu_tbl_lock);
	return ok;
}
