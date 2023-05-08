#include "lmdb.h"

int lmdb_dbInit ()
{
	return 0;
}

int lmdb_dbInsert (int id, int val)
{
	return 0;
}

int lmdb_dbGet (int id)
{
	return 0;
}

int lmdb_dbSet (int id, int val)
{
	return 0;
}

int lmdb_dbDel (int id)
{
	return 0;
}

int lmdb_dbTransBegin ()
{
    rc = mdb_txn_begin(self->env, NULL, 0, &ctx->txn);
	
	return 0;
}

edbDrvOps_t lmdb_edbDrv = {
	.name   = "lmdb",
	.dbInit = lmdb_dbInit,
	.dbInsert	= lmdb_dbInsert,
	.dbGet  = lmdb_dbGet,
	.dbSet  = lmdb_dbSet,
	.dbDel	= lmdb_dbDel
};

