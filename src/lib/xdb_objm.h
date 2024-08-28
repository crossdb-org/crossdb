/******************************************************************************
* Copyright (c) 2024-present JC Wang. All rights reserved
*
*   https://crossdb.org
*   https://github.com/crossdb-org/crossdb
*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at https://mozilla.org/MPL/2.0/.
******************************************************************************/

#ifndef __XDB_OBJM_H__
#define __XDB_OBJM_H__

typedef struct xdb_obj_t {
	struct xdb_obj_t	*pNxtObj;
	int 				xoid;
	uint32_t			hashcode;
	uint8_t				nm_len;
	char 				obj_name[XDB_NAME_LEN+1];
	uint8_t				obj_type;
} xdb_obj_t;

#define XDB_OBJ_ID(pObj)	((pObj)->obj.xoid)
#define XDB_OBJ_NAME(pObj)	((pObj)->obj.obj_name)
#define XDB_OBJ_NMLEN(pObj)	((pObj)->obj.nm_len)

#define XDB_OBJM_COUNT(objm)	((objm).obj_count)
#define XDB_OBJM_MAX(objm)		((objm).obj_max)
#define XDB_OBJM_GET(objm,id)	(void*)((objm).ppObjPool[id])

typedef struct xdb_objm_t {
	xdb_obj_t **ppObjPool;
	int			obj_cap;
	int			obj_max;
	int			obj_count;
	xdb_obj_t	**ppObjHash;
	uint32_t	hash_mask;
} xdb_objm_t;

static inline void* 
xdb_objm_getbyid (xdb_objm_t *pObjm, int xoid)
{
	return xoid < pObjm->obj_max ? pObjm->ppObjPool[xoid] : NULL;
}

XDB_STATIC void* 
xdb_objm_get2 (xdb_objm_t *pObjm, const char *name, int len);

static inline void* 
xdb_objm_get (xdb_objm_t *pObjm, const char *name)
{
	return xdb_objm_get2 (pObjm, name, 0);
}

XDB_STATIC int 
xdb_objm_add (xdb_objm_t *pObjm, void *pObj);

XDB_STATIC int 
xdb_objm_del (xdb_objm_t *pObjm, void *pObj);

XDB_STATIC void 
xdb_objm_free (xdb_objm_t *pObjm);

#endif // __XDB_OBJM_H__
