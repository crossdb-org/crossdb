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

#include "xdb_bmp.h"

#define XDB_LVBMP_FREE(lvbmp)	\
	if ((lvbmp)->bMalloc) {	\
		xdb_free (lvbmp);	\
	}

static inline xdb_lv1bmp_t* 
xdb_bmp_lv1alloc (xdb_bmp_t *pBmp)
{
	xdb_lv1bmp_t *pLv1Bmp;
	if (pBmp->lv1_alloc < 1) {
		pLv1Bmp = &pBmp->lv1_bmp[pBmp->lv1_alloc++];
		pLv1Bmp->bMalloc = false;
	} else {
		pLv1Bmp = xdb_malloc (sizeof(*pLv1Bmp));
		if (NULL == pLv1Bmp) {
			return NULL;
		}
		pLv1Bmp->bMalloc = true;
	}
	pLv1Bmp->bmp2 = 0;
	memset (pLv1Bmp->bmp3, 0, sizeof (pLv1Bmp->bmp3));
	//xdb_print ("alloc v1bmp %p\n", pLv1Bmp);
	return pLv1Bmp;
}

XDB_STATIC void 
xdb_bmp_init (xdb_bmp_t *pBmp)
{
	memset (pBmp->bmp1, 0, sizeof (pBmp->bmp1));
	pBmp->lv1_alloc = 0;
	pBmp->lv2_alloc = 0;
}

static inline void 
xdb_lv1bmp_free (xdb_lv1bmp_t *pLv1Bmp)
{
	#ifndef XDB_BIG_ENDIAN
	uint64_t bits2 = pLv1Bmp->bmp2;
	#else
	uint64_t bits2 = xdb_bswap64(pLv1Bmp->bmp2);
	#endif
	uint8_t *bmp2 = (uint8_t*)&bits2;

	for (int b2 = 0; b2 < 8; ++b2) {
		if (bmp2[b2]) {
			for (int i2 = 0; i2 < 8; ++i2) {
				if (bmp2[b2] & (1<<i2)) {
					uint32_t id2 = (b2<<3)|i2;
					#ifndef XDB_BIG_ENDIAN
					uint64_t bits3 = pLv1Bmp->bmp3[id2];
					#else
					uint64_t bits3 = xdb_bswap64(pLv1Bmp->bmp3[id2]);
					#endif
					uint8_t *bmp3 = (uint8_t*)&bits3;
					for (int b3 = 0; b3 < 8; ++b3) {
						if (bmp3[b3]) {
							for (int i3 = 0; i3 < 8; ++i3) {
								if (bmp3[b3] & (1<<i3)) {
									int id3 = (b3<<3)|i3;
									xdb_lv2bmp_t *pLv2Bmp = pLv1Bmp->pLv2Bmp[id2][id3];
									XDB_LVBMP_FREE (pLv2Bmp);
								}
							}
						}
					}
				}
			}
		}
	}
}

XDB_STATIC void 
xdb_bmp_free (xdb_bmp_t *pBmp)
{
	for (int id0 = 0; id0 < XDB_ARY_LEN(pBmp->bmp1); ++id0) {
	#ifndef XDB_BIG_ENDIAN
		uint64_t bits1 = pBmp->bmp1[id0];
	#else
		uint64_t bits1 = xdb_bswap64(pBmp->bmp1[id0]);
	#endif
		if (bits1) {
			uint8_t *bmp1 = (uint8_t*)&bits1;
			for (int b1 = 0; b1 < 8; ++b1) {
				if (bmp1[b1]) {
					for (int i1 = 0; i1 < 8; ++i1) {
						if (bmp1[b1] & (1<<i1)) {
							int id1 = (b1<<3)|i1;
							xdb_lv1bmp_t *pLv1Bmp = pBmp->pLv1Bmp[id0][id1];
							xdb_lv1bmp_free (pLv1Bmp);
							XDB_LVBMP_FREE (pLv1Bmp);
						}
					}
				}
			}
		}
	}
}

static inline void 
__xdb_lv2bmp_iterate (xdb_lv2bmp_t *pLv2Bmp, uint32_t bid, xdb_bmp_cb pCb, void *pArg)
{
	#ifndef XDB_BIG_ENDIAN
	uint64_t bits4 = pLv2Bmp->bmp4;
	#else
	uint64_t bits4 = xdb_bswap64(pLv2Bmp->bmp4);
	#endif
	uint8_t *bmp4 = (uint8_t*)&bits4;

	for (int b4 = 0; b4 < 8; ++b4) {
		if (bmp4[b4]) {
			for (int i4 = 0; i4 < 8; ++i4) {
				if (bmp4[b4] & (1<<i4)) {
					uint32_t id4 = (b4<<3)|i4, id4base = id4<<6;
					#ifndef XDB_BIG_ENDIAN
					uint64_t bits5 = pLv2Bmp->bmp5[id4];
					#else
					uint64_t bits5 = xdb_bswap64(pLv2Bmp->bmp5[id4]);
					#endif
					uint8_t *bmp5 = (uint8_t*)&bits5;
					for (int b5 = 0; b5 < 8; ++b5) {
						if (bmp5[b5]) {
							for (int i5 = 0; i5 < 8; ++i5) {
								if (bmp5[b5] & (1<<i5)) {
									int id5 = (b5<<3)|i5;
									if (NULL != pCb) {
										pCb (bid + id4base + id5, pArg);
									} else {
										xdb_print ("%d ", bid + id4base + id5);
									}
								}
							}
						}
					}
				}
			}
		}
	}
}

XDB_STATIC void 
xdb_lv2bmp_iterate (xdb_lv2bmp_t *pLv2Bmp, xdb_bmp_cb pCb, void *pArg)
{
	return __xdb_lv2bmp_iterate (pLv2Bmp, 0, pCb, pArg);
}

XDB_STATIC void 
xdb_lv2bmp_init (xdb_lv2bmp_t *pLv2Bmp)
{
	pLv2Bmp->bmp4  = 0;
	pLv2Bmp->count = 0;
}

static inline xdb_lv2bmp_t* 
xdb_bmp_lv2alloc (xdb_bmp_t *pBmp)
{
	xdb_lv2bmp_t *pLv2Bmp;
	if (pBmp->lv2_alloc < XDB_ARY_LEN(pBmp->lv2_bmp)) {
		pLv2Bmp = &pBmp->lv2_bmp[pBmp->lv2_alloc++];
		pLv2Bmp->bMalloc = false;
	} else {
		pLv2Bmp = xdb_malloc (sizeof(*pLv2Bmp));
		if (NULL == pLv2Bmp) {
			return NULL;
		}
		pLv2Bmp->bMalloc = true;
	}
	pLv2Bmp->bmp4 = 0;
	memset (pLv2Bmp->bmp5, 0, sizeof (pLv2Bmp->bmp5));
	//xdb_print ("alloc v2bmp %p\n", pLv2Bmp);
	return pLv2Bmp;
}

XDB_STATIC void 
xdb_lv2bmp_set (xdb_lv2bmp_t *pLv2Bmp, uint32_t bit)
{
	int id4 = (bit>>6)&0x3f, id5 = bit&0x3f;
	if (pLv2Bmp->bmp4 & (1LL << id4)) {
		if (!(pLv2Bmp->bmp5[id4] & (1LL << id5))) {
			pLv2Bmp->bmp5[id4] |= (1LL << id5);
			pLv2Bmp->count++;
		}
	} else {
		pLv2Bmp->bmp4 |= (1LL << id4);
		pLv2Bmp->bmp5[id4] = (1LL << id5);
		pLv2Bmp->count++;
	}
}

XDB_STATIC void 
xdb_lv2bmp_clr (xdb_lv2bmp_t *pLv2Bmp, uint32_t bit)
{
	int id4 = (bit>>6)&0x3f;

	if (pLv2Bmp->bmp4 & (1LL << id4)) {
		int id5 = bit&0x3f;
		if (pLv2Bmp->bmp5[id4] & (1LL << id5)) {
			pLv2Bmp->count--;
			pLv2Bmp->bmp5[id4] &= ~(1LL << id5);	
			if (0 == pLv2Bmp->bmp5[id4]) {
				pLv2Bmp->bmp4 &= ~(1LL << id4);
			}
		}
	}
}

XDB_STATIC bool 
xdb_lv2bmp_get (xdb_lv2bmp_t *pLv2Bmp, uint32_t bit)
{
	int id4 = (bit>>6)&0x3f;
	if (pLv2Bmp->bmp4 & (1LL << id4)) {
		int id5 = bit&0x3f;
		return (pLv2Bmp->bmp5[id4] & (1LL << id5)) > 0;
	}
	return false;
}

static inline void 
__xdb_lv1bmp_iterate (xdb_lv1bmp_t *pLv1Bmp, uint32_t bid, xdb_bmp_cb pCb, void *pArg)
{
	#ifndef XDB_BIG_ENDIAN
	uint64_t bits2 = pLv1Bmp->bmp2;
	#else
	uint64_t bits2 = xdb_bswap64(pLv1Bmp->bmp2);
	#endif
	uint8_t *bmp2 = (uint8_t*)&bits2;

	for (int b2 = 0; b2 < 8; ++b2) {
		if (bmp2[b2]) {
			for (int i2 = 0; i2 < 8; ++i2) {
				if (bmp2[b2] & (1<<i2)) {
					uint32_t id2 = (b2<<3)|i2, id2base = id2<<18;
					#ifndef XDB_BIG_ENDIAN
					uint64_t bits3 = pLv1Bmp->bmp3[id2];
					#else
					uint64_t bits3 = xdb_bswap64(pLv1Bmp->bmp3[id2]);
					#endif
					uint8_t *bmp3 = (uint8_t*)&bits3;
					for (int b3 = 0; b3 < 8; ++b3) {
						if (bmp3[b3]) {
							for (int i3 = 0; i3 < 8; ++i3) {
								if (bmp3[b3] & (1<<i3)) {
									int id3 = (b3<<3)|i3;
									xdb_lv2bmp_t *pLv2Bmp = pLv1Bmp->pLv2Bmp[id2][id3];
									__xdb_lv2bmp_iterate (pLv2Bmp, bid + id2base + (id3<<12), pCb, pArg);
								}
							}
						}
					}
				}
			}
		}
	}
}

XDB_STATIC void 
xdb_bmp_iterate (xdb_bmp_t *pBmp, xdb_bmp_cb pCb, void *pArg)
{
	for (int id0 = 0; id0 < XDB_ARY_LEN(pBmp->bmp1); ++id0) {
		#ifndef XDB_BIG_ENDIAN
		uint64_t bits1 = pBmp->bmp1[id0];
		#else
		uint64_t bits1 = xdb_bswap64(pBmp->bmp1[id0]);
		#endif
		if (bits1) {
			uint32_t id0base = id0 << 30;
			uint8_t *bmp1 = (uint8_t*)&bits1;
			for (int b1 = 0; b1 < 8; ++b1) {
				if (bmp1[b1]) {
					for (int i1 = 0; i1 < 8; ++i1) {
						if (bmp1[b1] & (1<<i1)) {
							int id1 = (b1<<3)|i1;
							xdb_lv1bmp_t *pLv1Bmp = pBmp->pLv1Bmp[id0][id1];
							__xdb_lv1bmp_iterate (pLv1Bmp, id0base + (id1<<24), pCb, pArg);
						}
					}
				}
			}
		}
	}
}

XDB_STATIC int 
xdb_bmp_set (xdb_bmp_t *pBmp, uint32_t bit)
{
	int id0 = bit>>30, id1 = (bit>>24)&0x3f;

	xdb_lv1bmp_t **ppLv1Bmp = &pBmp->pLv1Bmp[id0][id1];
	if (0 == (pBmp->bmp1[id0] & (1LL << id1))) {
		pBmp->bmp1[id0] 		|= (1LL << id1);
		*ppLv1Bmp = xdb_bmp_lv1alloc (pBmp);
		if (NULL == *ppLv1Bmp) {
			return -XDB_E_MEMORY;
		}
	}
	xdb_lv1bmp_t *pLv1Bmp = *ppLv1Bmp;
	int id2 = (bit>>18)&0x3f, id3 = (bit>>12)&0x3f;
	pLv1Bmp->bmp2 |= (1LL << id2);
	xdb_lv2bmp_t **ppLv2Bmp = &pLv1Bmp->pLv2Bmp[id2][id3];
	if (0 == (pLv1Bmp->bmp3[id2] & (1LL << id3))) {
		pLv1Bmp->bmp3[id2] |= (1LL << id3);
		*ppLv2Bmp = xdb_bmp_lv2alloc (pBmp);
		if (NULL == *ppLv2Bmp) {
			return -XDB_E_MEMORY;
		}
	}
	xdb_lv2bmp_t *pLv2Bmp = *ppLv2Bmp;
	int id4 = (bit>>6)&0x3f, id5 = bit&0x3f;
	pLv2Bmp->bmp4 |= (1LL << id4);
	pLv2Bmp->bmp5[id4] |= (1LL << id5);

	return 0;
}

XDB_STATIC void 
xdb_bmp_clr (xdb_bmp_t *pBmp, uint32_t bit)
{
	int id0 = bit>>30, id1 = (bit>>24)&0x3f;

	if (0 == (pBmp->bmp1[id0] & (1LL << id1))) {
		return;
	}
	xdb_lv1bmp_t *pLv1Bmp = pBmp->pLv1Bmp[id0][id1];
	
	int id2 = (bit>>18)&0x3f, id3 = (bit>>12)&0x3f;
	if (0 == (pLv1Bmp->bmp3[id2] & (1LL << id3))) {
		return;
	}
	xdb_lv2bmp_t *pLv2Bmp = pLv1Bmp->pLv2Bmp[id2][id3];

	int id4 = (bit>>6)&0x3f, id5 = bit&0x3f;
	pLv2Bmp->bmp5[id4] &= ~(1LL << id5);
	
	if (0 == pLv2Bmp->bmp5[id4]) {
		pLv2Bmp->bmp4 &= ~(1LL << id4);
		if (0 == pLv2Bmp->bmp4) {
			pLv1Bmp->pLv2Bmp[id2][id3] = NULL;
			XDB_LVBMP_FREE (pLv2Bmp);
			pLv1Bmp->bmp3[id2] &= ~(1LL << id3);
			if (0 == pLv1Bmp->bmp3[id2]) {
				pLv1Bmp->bmp2 &= ~(1LL << id2);
				if (0 == pLv1Bmp->bmp2) {
					pBmp->pLv1Bmp[id0][id1] = NULL;
					XDB_LVBMP_FREE (pLv1Bmp);
					pBmp->bmp1[id0] 		&= ~(1LL << id1);					
				}
			}
		}
	}
}

XDB_STATIC bool 
xdb_bmp_get (xdb_bmp_t *pBmp, uint32_t bit)
{
	int id0 = bit>>30, id1 = (bit>>24)&0x3f;

	if (0 == (pBmp->bmp1[id0] & (1LL << id1))) {
		return false;
	}
	xdb_lv1bmp_t *pLv1Bmp = pBmp->pLv1Bmp[id0][id1];

	int id2 = (bit>>18)&0x3f, id3 = (bit>>12)&0x3f;
	if (0 == (pLv1Bmp->bmp3[id2] & (1LL << id3))) {
			return false;
	}
	xdb_lv2bmp_t *pLv2Bmp = pLv1Bmp->pLv2Bmp[id2][id3];

	int id4 = (bit>>6)&0x3f, id5 = bit&0x3f;
	//xdb_print ("%d %d %d %d %d %d %lld", id0, id1, id2, id3, id4, id5, pLv2Bmp->bmp5[id4]);
	return (pLv2Bmp->bmp5[id4] & (1LL << id5)) > 0;
}


#ifdef XDB_MOD_TEST
int bmp_dump (uint32_t bit, void *pArg)
{
	printf ("%u:", bit);
	return 0;
}

int main (int argc, char *argv[])
{
	xdb_bmp_t bmp;

	xdb_bmp_init (&bmp);

	char cmd;
	bool bSet;
	uint32_t id, beg, end, step;
	while (1) {
		// s c d g
		scanf ("%c %u", &cmd, &id);
		switch (cmd) {
		case 'd':
			printf ("  dump\n");
			xdb_bmp_iterate (&bmp, bmp_dump, NULL);
			printf ("\n");
			break;
		case 's':
			printf ("  set %u\n", id);
			xdb_bmp_set (&bmp, id);
			break;
		case 'c':
			printf ("  clr %u\n", id);
			xdb_bmp_clr (&bmp, id);
			break;
		case 'g':
			bSet = xdb_bmp_get (&bmp, id);
			printf ("  get %u %d\n", id, bSet);
			break;
		case 't':
			scanf ("%u %u %u", &beg, &end, &step);
			printf ("  %s from %d to %d step %d\n", id==1?"set":"clr", beg, end, step);
			for (uint32_t i = beg; i < end; i+=step) {
				if (id) {
					xdb_bmp_set (&bmp, i);
				} else {
					xdb_bmp_clr (&bmp, i);
				}
			}
			break;
		case 'e':
			xdb_bmp_free (&bmp);
			exit(0);
		}
	}

	return 0;
}
#endif