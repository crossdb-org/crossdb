#include <iostream>
#include <string>
#include <map>
#include <unordered_map>

#define BENCH_DBNAME	"STL"
#define LKUP_COUNT		10000000
#define TEST_NAME(i) i?"HashMap":"Map"

#include "bench.h"

using namespace std;

class student {
public:
	int		id;
#ifdef USE_STRING
	string	name;
	string	cls;
#else
	char	name[17];
	char	cls[17];
#endif
	int		age;
	int		score;

	student () {
	}
	student (int id, const char *name, int age, const char *cls, int score) {
		this->id = id;
#ifdef USE_STRING
		this->name = name;
		this->cls = cls;
#else
		strncpy (this->name, name, 17);
		this->name[16] = '\0';
		strncpy (this->cls, cls, 17);
		this->cls[16] = '\0';
#endif
		this->age = age;
		this->score = score;
	}	
};

static map<int, student> 			stu_tbl_map;
static unordered_map<int, student> 	stu_tbl_hmap;
// std::shared_mutex is not used because, in a single-threaded context, the compiler optimizes the code and omits the lock.
pthread_rwlock_t stu_tbl_lock = PTHREAD_RWLOCK_INITIALIZER;

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

bool bench_sql_insert (void *pDb, const char *sql, int id, const char *name, int age, const char *cls, int score)
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
	if (NULL == (pStu = stl_map_get_byid (id))) {
		pthread_rwlock_unlock(&stu_tbl_lock);
		return false;
	}
	stu = *pStu;
	pthread_rwlock_unlock(&stu_tbl_lock);	
	// handle row out of the lock to reduce lock time
#ifdef USE_STRING
	callback (pArg, stu.id, stu.name.c_str(), stu.age, stu.cls.c_str(), stu.score);
#else
	callback (pArg, stu.id, stu.name, stu.age, stu.cls, stu.score);
#endif
	return true;
}

bool bench_sql_updAge_byid (void *pDb, const char *sql, int id, int age)
{
	student *pStu;

	pthread_rwlock_wrlock(&stu_tbl_lock);
	if (NULL == (pStu = stl_map_get_byid (id))) {
		pthread_rwlock_unlock(&stu_tbl_lock);	
		return false;
	}
	pStu->age = age;
	pthread_rwlock_unlock(&stu_tbl_lock);	
	return true;
}

bool bench_sql_del_byid (void *pDb, const char *sql, int id)
{
	pthread_rwlock_wrlock(&stu_tbl_lock);
	bool ok = stu_tbl_map.erase(id);;
	pthread_rwlock_unlock(&stu_tbl_lock);
	return ok;
}

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

bool bench_stmt_insert (void *pStmt, int id, const char *name, int age, const char *cls, int score)
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
	if (NULL == (pStu = stl_hmap_get_byid (id))) {
		pthread_rwlock_unlock(&stu_tbl_lock);	
		return false;
	}
	stu = *pStu;
	pthread_rwlock_unlock(&stu_tbl_lock);
	// handle row out of the lock to reduce lock time
#ifdef USE_STRING
	callback (pArg, stu.id, stu.name.c_str(), stu.age, stu.cls.c_str(), stu.score);
#else
	callback (pArg, stu.id, stu.name, stu.age, stu.cls, stu.score);
#endif
	return true;
}

bool bench_stmt_updAge_byid (void *pStmt, int id, int age)
{
	student *pStu;

	pthread_rwlock_wrlock(&stu_tbl_lock);
	if (NULL == (pStu = stl_hmap_get_byid (id))) {
		pthread_rwlock_unlock(&stu_tbl_lock);	
		return false;
	}
	pStu->age = age;
	pthread_rwlock_unlock(&stu_tbl_lock);	
	return true;
}

bool bench_stmt_del_byid (void *pStmt, int id)
{
	pthread_rwlock_wrlock(&stu_tbl_lock);
	bool ok = stu_tbl_hmap.erase(id);
	pthread_rwlock_unlock(&stu_tbl_lock);
	return ok;
}
