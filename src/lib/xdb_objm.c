/******************************************************************************
* Copyright (c) 2024-present JC Wang. All rights reserved
*
*   https://crossdb.org
*   https://github.com/crossdb-org/crossdb
*
* This program is free software: you can use, redistribute, and/or modify it under
* the terms of the GNU Lesser General Public License, version 3 or later ("LGPL"),
* as published by the Free Software Foundation.
*
* This program is distributed in the hope that it will be useful, but WITHOUT
* ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS 
* FOR A PARTICULAR PURPOSE. See the GNU published General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public License along with 
* this program. If not, see <http://www.gnu.org/licenses/>.
******************************************************************************/

#include "xdb_objm.h"

XDB_STATIC void* 
xdb_objm_get2 (xdb_objm_t *pObjm, const char *name, int len)
{
	if (xdb_unlikely (0 == len)) {
		len = strlen (name);
	}
	if (xdb_unlikely (NULL == pObjm->ppObjHash)) {
		return NULL;
	}
	
	uint32_t hashcode = xdb_strcasehash (name, len);
	uint32_t slot = hashcode & pObjm->hash_mask;

	xdb_obj_t *pObj;
	for (pObj = pObjm->ppObjHash[slot]; pObj != NULL; pObj=pObj->pNxtObj) {
		if ((pObj->hashcode == hashcode) && !strcasecmp (pObj->obj_name, name))	{
			return pObj;
		}
	}
	return NULL;
}

XDB_STATIC int 
xdb_objm_hash_add (xdb_objm_t *pObjm, xdb_obj_t *pNewObj)
{
	pNewObj->hashcode = xdb_strcasehash (pNewObj->obj_name, pNewObj->nm_len);
	
	uint32_t slot = pNewObj->hashcode & pObjm->hash_mask;

	xdb_obj_t *pObj, *pPrevObj = NULL;
	for (pObj = pObjm->ppObjHash[slot]; pObj != NULL; pObj=pObj->pNxtObj) {
		if ((pObj->hashcode == pNewObj->hashcode) && !strcasecmp (pObj->obj_name, pNewObj->obj_name))	{
			return -XDB_E_EXISTS;
		}
		pPrevObj = pObj;
	}
	if (NULL == pPrevObj) {
		pNewObj->pNxtObj = NULL;
		pObjm->ppObjHash[slot] = pNewObj;
	} else {
		pNewObj->pNxtObj = pPrevObj->pNxtObj;
		pPrevObj->pNxtObj = pNewObj;
	}
	return XDB_OK;
}

XDB_STATIC int 
xdb_objm_add (xdb_objm_t *pObjm, void *obj)
{
	xdb_obj_t *pObj = obj;

	// xdb_dbglog ("add %s\n", pObj->obj_name);
	pObj->nm_len = strlen (pObj->obj_name);
	if (pObjm->obj_count == pObjm->obj_cap || (pObj->xoid > pObjm->obj_cap)) {
		int newcap = 1;
		if (pObjm->obj_cap > 0) {
			newcap = pObjm->obj_cap << 1;
		}
		while (pObj->xoid >= newcap) {
			newcap <<= 1;
		}
		xdb_obj_t **ppObjPool = xdb_realloc (pObjm->ppObjPool, newcap*3 * sizeof (void*));
		if (NULL == ppObjPool) {
			return -XDB_E_MEMORY;
		}
		pObjm->ppObjPool = ppObjPool;
		memset (ppObjPool + pObjm->obj_cap, 0, (newcap*3 - pObjm->obj_cap) * sizeof (void*));

		pObjm->ppObjHash = &ppObjPool[newcap];
		pObjm->hash_mask = ((newcap<<1) - 1);
		for (int i = 0 ; i < pObjm->obj_cap; ++i) {
			if (NULL != ppObjPool[i]) {
				xdb_objm_hash_add (pObjm, ppObjPool[i]);
			}
		}
		pObjm->obj_cap = newcap;
	}

	if (pObj->xoid < 0) {
		for (int i = 0; i < pObjm->obj_cap; ++i) {
			if (NULL == pObjm->ppObjPool[i]) {
				pObj->xoid = i;
				break;
			}
		}
	}

	pObjm->ppObjPool[pObj->xoid] = pObj;
	xdb_objm_hash_add (pObjm, pObj);
	pObjm->obj_count++;
	if (pObj->xoid >= pObjm->obj_max) {
		pObjm->obj_max = pObj->xoid + 1;
	}

	return -XDB_ERROR;
}

XDB_STATIC int 
xdb_objm_del (xdb_objm_t *pObjm, void *obj)
{
	xdb_obj_t *pDelObj = obj;
	//uint64_t hash = xdb_wyhash (pDelObj->obj_name, pDelObj->nm_len);
	int slot = pDelObj->hashcode & pObjm->hash_mask;

	// remove from hash
	xdb_obj_t *pObj, *pPrevObj = NULL;
	for (pObj = pObjm->ppObjHash[slot]; pObj != NULL; pObj=pObj->pNxtObj) {
		if (pObj == pDelObj)	{
			if (NULL == pPrevObj) {
				pObjm->ppObjHash[slot] = pDelObj->pNxtObj;
			} else {
				pPrevObj->pNxtObj = pDelObj->pNxtObj;
			}
			break;
		}
		pPrevObj = pObj;
	}

	pObjm->ppObjPool[pDelObj->xoid] = NULL;
	pObjm->obj_count--;

	if (pDelObj->xoid + 1 == pObjm->obj_max) {
		int id;
		for (id = pDelObj->xoid - 1; (id >= 0) && (NULL == pObjm->ppObjPool[id]); --id)
			;
		pObjm->obj_max = id + 1;
	}

	return XDB_OK;
}

XDB_STATIC void 
xdb_objm_free (xdb_objm_t *pObjm)
{
	xdb_free (pObjm->ppObjPool);
	memset (pObjm, 0, sizeof (*pObjm));
}
