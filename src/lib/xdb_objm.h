
/******************************************************************************
* Copyright (c) 2024-present JC Wang. All rights reserved
*
*   https://crossdb.org
*   https://github.com/crossdb-org/crossdb
*
* This program is free software: you can redistribute it and/or modify it under
* the terms of the GNU General Public License, version 3 or later, as published 
* by the Free Software Foundation.
*
* This program is distributed in the hope that it will be useful, but WITHOUT
* ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS 
* FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License along with 
* this program. If not, see <http://www.gnu.org/licenses/>.
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
	return pObjm->ppObjPool[xoid];
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