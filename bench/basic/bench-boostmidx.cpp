#include <iostream>
#include <string>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/composite_key.hpp>

#define BENCH_DBNAME	"Boost"
#define TEST_NAME(i) 	i?"Hash":"Order"
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
		Ordered Index
 ************************************************/

struct _id {};

using student_table_map =
boost::multi_index::multi_index_container<
	student,
	boost::multi_index::indexed_by<
		boost::multi_index::ordered_unique<boost::multi_index::tag<_id>, BOOST_MULTI_INDEX_MEMBER(student, int, id)>
	>
>;

student_table_map stu_tbl_map;

student_table_map::index<_id>::type& find_id_map = stu_tbl_map.get<_id>();

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

bool bench_sql_insert (void *pDb, const char *sql, int id, string &name, int age, string &cls, int score)
{
	pthread_rwlock_wrlock(&stu_tbl_lock);
	auto ok = stu_tbl_map.emplace (id, name, age, cls, score);
	pthread_rwlock_unlock(&stu_tbl_lock);
	return ok.second;
}

bool bench_sql_get_byid (void *pDb, const char *sql, int id, stu_callback callback, void *pArg)
{
	student stu, *pStu = NULL;

	pthread_rwlock_rdlock(&stu_tbl_lock);	
	student_table_map::index<_id>::type::iterator iter_id = find_id_map.find(id);
	if (iter_id != find_id_map.end()) {
		stu = *iter_id;
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
	bool ok = false;
	student stu;

	pthread_rwlock_wrlock(&stu_tbl_lock);
	student_table_map::index<_id>::type::iterator iter_id = find_id_map.find(id);
	if (iter_id != find_id_map.end()) {
		stu = *iter_id;
		stu.age += age;
		ok = find_id_map.replace(iter_id, stu);
	}
	pthread_rwlock_unlock(&stu_tbl_lock);	
	return ok;
}

bool bench_sql_del_byid (void *pDb, const char *sql, int id)
{
	pthread_rwlock_wrlock(&stu_tbl_lock);
	bool ok = find_id_map.erase(id);
	pthread_rwlock_unlock(&stu_tbl_lock);	
	return ok;
}


/************************************************
		Hashed Index
 ************************************************/

using student_table_hmap =
boost::multi_index::multi_index_container<
	student,
	boost::multi_index::indexed_by<
		boost::multi_index::hashed_unique<boost::multi_index::tag<_id>, BOOST_MULTI_INDEX_MEMBER(student, int, id)>
	>
>;

student_table_hmap stu_tbl_hmap;

student_table_hmap::index<_id>::type& find_id_hmap = stu_tbl_hmap.get<_id>();

void* bench_stmt_prepare (void *pDb, const char *sql)
{
	return pDb;
}

void bench_stmt_close (void *pStmt)
{
}

bool bench_stmt_insert (void *pStmt, int id, string &name, int age, string &cls, int score)
{
	pthread_rwlock_wrlock(&stu_tbl_lock);
	auto ok = stu_tbl_hmap.emplace (id, name, age, cls, score);
	pthread_rwlock_unlock(&stu_tbl_lock);
	return ok.second;
}

bool bench_stmt_get_byid (void *pStmt, int id, stu_callback callback, void *pArg)
{
	student stu, *pStu = NULL;

	pthread_rwlock_rdlock(&stu_tbl_lock);	
	student_table_hmap::index<_id>::type::iterator iter_id = find_id_hmap.find(id);
	if (iter_id != find_id_hmap.end()) {
		stu = *iter_id;
		pStu = &stu;
	}
	pthread_rwlock_unlock(&stu_tbl_lock);	

	if (pStu) {
		// handle row out of the lock to reduce lock time
		callback (pArg, stu.id, stu.name, stu.age, stu.cls, stu.score);
	}
	return pStu != NULL;
}

bool bench_stmt_updAge_byid (void *pStmt, int id, int age)
{
	bool ok = false;
	student stu;

	pthread_rwlock_wrlock(&stu_tbl_lock);

	student_table_hmap::index<_id>::type::iterator iter_id = find_id_hmap.find(id);
	if (iter_id != find_id_hmap.end()) {
		stu = *iter_id;
		stu.age += age;
		ok = find_id_hmap.replace(iter_id, stu);
	}

	pthread_rwlock_unlock(&stu_tbl_lock);	
	return ok;
}

bool bench_stmt_del_byid (void *pStmt, int id)
{
	pthread_rwlock_wrlock(&stu_tbl_lock);
	bool ok = find_id_hmap.erase(id);
	pthread_rwlock_unlock(&stu_tbl_lock);
	return ok;
}
