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

#if XDB_LOG_FLAGS & XDB_LOG_RBTREE
#define xdb_rbtlog(...)	xdb_print(__VA_ARGS__)
#else
#define xdb_rbtlog(...)
#endif

#define XDB_RB_RED					XDB_ROWID_MSB	// red color
#define XDB_RB_COLOR				XDB_ROWID_MSB	// red color
#define XDB_RB_SIB_NODE				XDB_ROWID_MSB
#define XDB_RB_NULL					0

#define XDB_RB_NODE(row_id)			(&pT->rb_node[row_id])
#define rb_prev						rb_left
#define rb_next						rb_right


/*
 *             X        left_rotate(X)--->       Y
 *           /   \                             /   \
 *          A     Y                           X     C
 *              /   \                        /   \
 *             B     C                      A     B
 */
XDB_STATIC void 
xdb_rb_left_rotate (xdb_rbtree_t *pT, xdb_rowid X)
{
	xdb_rowid		Y;
	xdb_rbnode_t	*pX = XDB_RB_NODE(X), *pY, *pB, *pXP;

    // Prowonditions: (x->right != T.nil);

	// 1: y = x.right
	Y = pX->rb_right;
	pY = XDB_RB_NODE(Y);

    // 2: x.right = y.left
    pX->rb_right = pY->rb_left; // Turn Y's left subtree B into X's right subtree

    // 3: if y.left != T.nil
    if (pY->rb_left != XDB_RB_NULL) {
    	// 4: y.left.p = x
		pB = XDB_RB_NODE(pY->rb_left); 	// set B's parent to X
		pB->rb_parent = X;
    }

    // 5: y.p = x.p
	pY->rb_parent = pX->rb_parent; // Link X's parent to Y

	// 6: if x.p == T.nil
    if (pX->rb_parent == XDB_RB_NULL) { // X is root
		// 7: T.root = y
        pT->rb_root = Y;
		xdb_rbtlog ("  Set %d as root\n", Y);
    } else {
        pXP = XDB_RB_NODE(pX->rb_parent);
    	// 8: elseif x == x.p.left
        if (X == pXP->rb_left) {
			// 9: x.p.left = y
            pXP->rb_left = Y;
        } else {
		// 10: else x.p.right = y
            pXP->rb_right = Y;
        }
    }

   	// 11: y.left = x
    pY->rb_left = X; // Put X on Y's left

	// 12: x.p = Y
    pX->rb_parent = Y;
}

/*
 *               Y       right_rotate(Y)-->       X       
 *             /   \                            /   \     
 *            X     C                          A     Y    
 *          /   \                                  /  \   
 *         A     B                                B    C 
 */
XDB_STATIC void 
xdb_rb_right_rotate (xdb_rbtree_t *pT, xdb_rowid Y)
{
	xdb_rowid		X;
	xdb_rbnode_t	*pY = XDB_RB_NODE(Y), *pX, *pB, *pYP;

    // Prowonditions: (y->left != T.nil);

	// 1: x = y.left
	X = pY->rb_left;
	pX = XDB_RB_NODE(X);

    // 2: y.left = x.right
    pY->rb_left = pX->rb_right; // Turn X's right subtree B into Y's left subtree

    // 3: if x.right != T.nil
    if (pX->rb_right != XDB_RB_NULL) {
    	// 4: x.right.p = y
		pB = XDB_RB_NODE(pX->rb_right); 	// set B's parent to Y
		pB->rb_parent = Y;
    }

    // 5: x.p = y.p
	pX->rb_parent = pY->rb_parent; // Link Y's parent to X

	// 6: if y.p == T.nil
    if (pY->rb_parent == XDB_RB_NULL) { // Y is root
		// 7: T.root = x
        pT->rb_root = X;
		xdb_rbtlog ("  Set %d as root\n", X);
    } else {
        pYP = XDB_RB_NODE(pY->rb_parent);
    	// 8: elseif y == y.p.left
        if (Y == pYP->rb_left) {
			// 9: y.p.left = x
            pYP->rb_left = X;
        } else {
		// 10: else y.p.right = x
            pYP->rb_right = X;
        }
    }

   	// 11: x.right = y
    pX->rb_right = Y; // Put Y on X's right

	// 12: y.p = X
    pY->rb_parent = X;
}

XDB_STATIC void 
xdb_rb_insert_fixup (xdb_rbtree_t *pT, xdb_rowid Z)
{
	xdb_rowid 		Y, ZP, ZPP;
	xdb_rbnode_t	*pY, *pZ = XDB_RB_NODE(Z), *pZP, *pZPP;

	ZP = pZ->rb_parent;
	pZP = XDB_RB_NODE(ZP);
	// 1: while z.p.color == RED
    while (pZP->color_sibling & XDB_RB_RED) {
		ZPP = pZP->rb_parent;
		pZPP = XDB_RB_NODE(ZPP);
		// 2: if z.p == z.p.p.left
        if (ZP == pZPP->rb_left) {
			// 3: y = z.p.p.right
			Y = pZPP->rb_right;
			pY = XDB_RB_NODE(Y);
			// 4: if y.color == RED	: case 1
            if (pY->color_sibling & XDB_RB_RED) {
				// 5: z.p.color = BLACK
				pZP->color_sibling &= (~XDB_RB_RED);
				// 6: y.color = BLACK
                pY->color_sibling &= (~XDB_RB_RED);
                // 7: z.p.p.color = RED
                pZPP->color_sibling |= XDB_RB_RED;
                // 8: z = z.p.p
                Z = ZPP;
            } else {
				// 9: elseif z == z.p.right : case 2
            	if (Z == pZP->rb_right) {
	            	// 10: z = z.p
	            	Z = ZP;
	            	// 11 : LEFT-ROTATE(T,z)
	                xdb_rb_left_rotate (pT, Z);

					pZ = XDB_RB_NODE(Z);
					ZP = pZ->rb_parent;
					pZP = XDB_RB_NODE(ZP);
					ZPP = pZP->rb_parent;
					pZPP = XDB_RB_NODE(ZPP);
	            }

				// case 3
	            // 12: z.p.color = BLACK
	            pZP->color_sibling &= (~XDB_RB_RED);
	            // 13: z.p.p.color = RED
	            pZPP->color_sibling |= XDB_RB_RED;
	            // 14: RIGHT-ROTATE(T, z.p.p)
	            xdb_rb_right_rotate (pT, ZPP);
			}
        }
        else {
            // 15 : else (same as then clause with right and left exchanged
			// 3: y = z.p.p.left
			Y = pZPP->rb_left;
			pY = XDB_RB_NODE(Y);
			// 4: if y.color == RED	: case 1
            if (pY->color_sibling & XDB_RB_RED) {
				// 5: z.p.color = BLACK
				pZP->color_sibling &= (~XDB_RB_RED);
				// 6: y.color = BLACK
                pY->color_sibling &= (~XDB_RB_RED);
                // 7: z.p.p.color = RED
                pZPP->color_sibling |= XDB_RB_RED;
                // 8: z = z.p.p
                Z = ZPP;
            } else {
				// 9: elseif z == z.p.left : case 2
            	if (Z == pZP->rb_left) {
	            	// 10: z = z.p
	            	Z = ZP;
	            	// 11 : RIGHT-ROTATE(T,z)
	                xdb_rb_right_rotate (pT, Z);

					pZ = XDB_RB_NODE(Z);
					ZP = pZ->rb_parent;
					pZP = XDB_RB_NODE(ZP);
					ZPP = pZP->rb_parent;
					pZPP = XDB_RB_NODE(ZPP);
	            }

				// case 3
	            // 12: z.p.color = BLACK
	            pZP->color_sibling &= (~XDB_RB_RED);
	            // 13: z.p.p.color = RED
	            pZPP->color_sibling |= XDB_RB_RED;
	            // 14: LEFT-ROTATE(T, z.p.p)
	            xdb_rb_left_rotate (pT, ZPP);
			}
        }

		pZ = XDB_RB_NODE(Z);
		ZP = pZ->rb_parent;
		pZP = XDB_RB_NODE(ZP);
    }

	// 16: T.root.color = BLACK
	XDB_RB_NODE(pT->rb_root)->color_sibling &= (~XDB_RB_RED);
}

XDB_STATIC int 
xdb_rbtree_add (xdb_conn_t *pConn, xdb_idxm_t* pIdxm, xdb_rowid Z, void *pZRow)
{
	xdb_stgmgr_t	*pStgMgr	= &pIdxm->pTblm->stg_mgr;
	int					cmp;
	xdb_rbtree_t 		*pT		= pIdxm->pRbtrHdr;
	xdb_rowid 			 X, Y, S;
	xdb_rbnode_t 		*pX, *pY, *pZ = XDB_RB_NODE(Z), *pS;
	void				*pXRow;

	if (Z > pIdxm->node_cap) {
		xdb_stg_truncate (&pIdxm->stg_mgr, pIdxm->node_cap<<1);
		pIdxm->node_cap = XDB_STG_CAP(&pIdxm->stg_mgr);
		pIdxm->pRbtrHdr  = (xdb_rbtree_t*)pIdxm->stg_mgr.pStgHdr;
	}

	xdb_rbtlog ("rbtree add rid %d\n", Z);

	// 1: y = t.nil
	Y = XDB_RB_NULL;
	// 2: x = T.root
	X = pT->rb_root;
	pX = XDB_RB_NODE(X);

    // 3: while x != T.nil
    while (X != XDB_RB_NULL) {
		// 4: y = x
		Y = X;

		pXRow = XDB_IDPTR(pStgMgr, X);
		cmp = xdb_row_cmp3 (pZRow, pXRow, pIdxm->pFields, pIdxm->fld_count);
		if (0 == cmp) {
			if (pIdxm->bUnique && pConn && xdb_row_valid (pConn, pIdxm->pTblm, pXRow, X)) {
				xdb_rbtlog ("  Duplicate insert %d to unique index %s\n", Z, XDB_OBJ_NAME(pIdxm));
				// for remove to work corrowtly
				memset (pZ, 0 ,sizeof (*pZ));
				//XDB_SETERR(XDB_E_EXISTS, "Duplicate insert for index '%s'", XDB_OBJ_NAME(pIdxm));
				return XDB_E_EXISTS;
			} else {
				break;
			}
		}
		// 5: if z.key < x.key
		// 6:   x = x.left
		// 7: else x = x.right
		X = (cmp < 0) ? pX->rb_left : pX->rb_right;
		pX = XDB_RB_NODE(X);
    }

    if (X != XDB_RB_NULL) {
		// same roword add to sibling
		xdb_rbtlog ("  Duplicate insert %d to roword %d as 1st sibling\n", Z, X);
		S = pX->color_sibling & XDB_ROWID_MASK;
		pX->color_sibling &= XDB_RB_COLOR;
		pX->color_sibling |= Z;
		pZ->rb_next = S;
		pZ->rb_prev = XDB_RB_NULL;
		pZ->rb_parent = X | XDB_RB_SIB_NODE;
		pZ->color_sibling = XDB_RB_NULL;
		if (XDB_RB_NULL != S) {
			xdb_rbtlog ("  1st sibling %d replace previous 1st silbing %d\n", Z, S);
			pS = XDB_RB_NODE(S);
			pS->rb_prev = Z;
			pS->rb_parent = XDB_RB_NULL | XDB_RB_SIB_NODE;
		}
    } else {
		// insert Z under Y
		xdb_rbtlog ("  Insert %d under %d\n", Z, Y);

		// 8: z.p = y
		pZ->rb_parent = Y;
		// 9: if y == T.nil
	    if (Y == XDB_RB_NULL) {
			// 10: t.root = Z
	        pT->rb_root = Z;
			xdb_rbtlog ("  Set %d as root\n", Z);
	    } else {
			pY = XDB_RB_NODE(Y);
			// 11: elseif z.key < y.key
	        if (cmp < 0) {
				// 12: y.left = z
	            pY->rb_left = Z;
	        }
	        else {
				// 13: else y.right = z
	            pY->rb_right = Z;
	        }
	    }

		// 14: z.left = T.nil
		// 15: z.right = T.nil 
	    pZ->rb_right = pZ->rb_left = XDB_RB_NULL;

	    // 16: z.color = RED
	    pZ->color_sibling = XDB_RB_RED;

		// 17: RB-INSERT-FIXUP(T, z)
		xdb_rb_insert_fixup (pT, Z);

		pT->node_count++;
	}

	pT->row_count++;

	return XDB_OK;
}

static inline void 
xdb_rb_transplant (xdb_rbtree_t *pT, xdb_rowid U, xdb_rowid V)
{
	xdb_rowid			UP;
	xdb_rbnode_t 		*pU = XDB_RB_NODE(U), *pV = XDB_RB_NODE(V), *pUP;

	UP = pU->rb_parent;
	pUP = XDB_RB_NODE(UP);

	// 1: if u.p == T.nil
	if (UP == XDB_RB_NULL) {
		// 2: T.root = v
		pT->rb_root = V;
	} 
	// 3: elseif u == u.p.left
	else if (U == pUP->rb_left) {
		// 4: u.p.left = v
		pUP->rb_left = V;
	} else {
		// 5: else u.p.rigth = v
		pUP->rb_right = V;		
	}
	// 6: v.p = u.p
	pV->rb_parent = UP;
}

static inline xdb_rowid 
xdb_rb_minimum (xdb_rbtree_t *pT, xdb_rowid X)
{
	xdb_rbnode_t 		*pX = XDB_RB_NODE(X);
	// 1: while x.left != NIL
	while (pX->rb_left != XDB_RB_NULL) {
		// 2: x = x.left
		X = pX->rb_left;
		pX = XDB_RB_NODE(X);
	}
	return X;
}

static inline xdb_rowid 
xdb_rb_maximum (xdb_rbtree_t *pT, xdb_rowid X)
{
	xdb_rbnode_t 		*pX = XDB_RB_NODE(X);
	// 1: while x.right != NIL
	while (pX->rb_right != XDB_RB_NULL) {
		// 2: x = x.right
		X = pX->rb_right;
		pX = XDB_RB_NODE(X);
	}
	return X;
}

XDB_STATIC void 
xdb_rb_delete_fixup (xdb_rbtree_t *pT, xdb_rowid X)
{
	xdb_rowid			W, XP;
	xdb_rbnode_t 		*pX = XDB_RB_NODE(X), *pXP, *pW, *pWL, *pWR;

	// 1: while x != T.root and x.color == BLACK
	while ((X != pT->rb_root) && !(pX->color_sibling&XDB_RB_RED)) 
	{
		XP = pX->rb_parent;
		pXP = XDB_RB_NODE(XP);
		xdb_rbtlog ("rb_delete_fixup %d, parent %d\n", X, XP);
		// 2: if x == x.p.left
		if (X == pXP->rb_left) 
		{
			// 3: w = x.p.right
			W = pXP->rb_right;
			pW = XDB_RB_NODE(W);
			// 4: if w.color == RED		case 1:
			if (pW->color_sibling&XDB_RB_RED) {
				// 5: w.color = BLACK
				pW->color_sibling &= (~XDB_RB_RED);
				// 6: x.p.color = RED
				pXP->color_sibling |= XDB_RB_RED;
				// 7: LEFT-ROTATE(T, x.p)
				xdb_rb_left_rotate (pT, XP);
				// 8: w = x.p.right
				XP = pX->rb_parent;
				pXP = XDB_RB_NODE(XP);
				W = pXP->rb_right;
				pW = XDB_RB_NODE(W);
			}

			// 9: if w.left.color == BLACK and w.right.color = BLACK		case 2:
			pWL = XDB_RB_NODE(pW->rb_left);
			pWR = XDB_RB_NODE(pW->rb_right);
			if (!(pWL->color_sibling&XDB_RB_RED) && !(pWR->color_sibling&XDB_RB_RED)) {
				// 10: w.color = RED
				pW->color_sibling |= XDB_RB_RED;
				// 11: x = x.p
				X = pX->rb_parent;
			} else {
			// 12: else if w.right.color = BLACK		case 3:
				if (!(pWR->color_sibling&XDB_RB_RED)) {
					// 13: w.left.color = BLACK
					pWL->color_sibling &= (~XDB_RB_RED);
					// 14: w.color = RED
					pW->color_sibling |= XDB_RB_RED;
					// 15: RIGHT-ROTATE(T, w)
					xdb_rb_right_rotate (pT, W);
					// 16: w = x.p.right
					XP = pX->rb_parent;
					pXP = XDB_RB_NODE(XP);
					W = pXP->rb_right;
					pW = XDB_RB_NODE(W);
					pWR = XDB_RB_NODE(pW->rb_right);
				}
				// case 4:
				// 17: w.color = x.p.color
				pW->color_sibling &= (~XDB_RB_RED);
				pW->color_sibling |= (pXP->color_sibling&XDB_RB_RED);
				// 18: x.p.color = BLACK
				pXP->color_sibling &= (~XDB_RB_RED);
				// 19: w.right.color = BLACK
				pWR->color_sibling &= (~XDB_RB_RED);
				// 20: LEFT-ROTATE(T, x.p)
				xdb_rb_left_rotate (pT, XP);
				// 21: x = T.root
				X = pT->rb_root;
			}
		}

		// 22: else (same as then clauses with right and left exchanged)
		else {
			// 3: w = x.p.left
			W = pXP->rb_left;
			pW = XDB_RB_NODE(W);
			// 4: if w.color == RED		case 1:
			if (pW->color_sibling&XDB_RB_RED) {
				// 5: w.color = BLACK
				pW->color_sibling &= (~XDB_RB_RED);
				// 6: x.p.color = RED
				pXP->color_sibling |= XDB_RB_RED;
				// 7: RIGHT-ROTATE(T, x.p)
				xdb_rb_right_rotate (pT, XP);
				// 8: w = x.p.left
				XP = pX->rb_parent;
				pXP = XDB_RB_NODE(XP);
				W = pXP->rb_left;
				pW = XDB_RB_NODE(W);
			}

			// 9: if w.left.color == BLACK and w.right.color = BLACK		case 2:
			pWL = XDB_RB_NODE(pW->rb_left);
			pWR = XDB_RB_NODE(pW->rb_right);
			if (!(pWL->color_sibling&XDB_RB_RED) && !(pWR->color_sibling&XDB_RB_RED)) {
				// 10: w.color = RED
				pW->color_sibling |= XDB_RB_RED;
				// 11: x = x.p
				X = pX->rb_parent;
			} else {
			// 12: else if w.left.color = BLACK		case 3:
				if (!(pWL->color_sibling&XDB_RB_RED)) {
					// 13: w.right.color = BLACK
					pWR->color_sibling &= (~XDB_RB_RED);
					// 14: w.color = RED
					pW->color_sibling |= XDB_RB_RED;
					// 15: LEFT-ROTATE(T, w)
					xdb_rb_left_rotate (pT, W);
					// 16: w = x.p.left
					XP = pX->rb_parent;
					pXP = XDB_RB_NODE(XP);
					W = pXP->rb_left;
					pW = XDB_RB_NODE(W);
					pWL = XDB_RB_NODE(pW->rb_left);
				}
				// case 4:
				// 17: w.color = x.p.color
				pW->color_sibling &= (~XDB_RB_RED);
				pW->color_sibling |= (pXP->color_sibling&XDB_RB_RED);
				// 18: x.p.color = BLACK
				pXP->color_sibling &= (~XDB_RB_RED);
				// 19: w.left.color = BLACK
				pWL->color_sibling &= (~XDB_RB_RED);
				// 20: RIGHT-ROTATE(T, x.p)
				xdb_rb_right_rotate (pT, XP);
				// 21: x = T.root
				X = pT->rb_root;
			}
		}

		pX = XDB_RB_NODE(X);
	}

	// 23: x.color = BLACK
	pX->color_sibling &= (~XDB_RB_RED);	
}

XDB_STATIC int 
xdb_rbtree_rem (xdb_idxm_t* pIdxm, xdb_rowid Z, void *pRow)
{
	xdb_rbtree_t 		*pT		= pIdxm->pRbtrHdr;
	xdb_rowid			P, S, X, Y, y_org_color;
	xdb_rbnode_t 		*pP, *pS, *pX, *pY, *pZ = XDB_RB_NODE(Z);

	if (! (pZ->rb_left + pZ->rb_right + pZ->rb_parent + pZ->color_sibling) && (pT->rb_root != Z)) {
		// not in index
		return XDB_OK;
	}

	S = pZ->color_sibling & XDB_ROWID_MASK;
	if (pZ->rb_parent & XDB_RB_SIB_NODE) {
		/* 
		 * It's sibling
		 */
		if (XDB_RB_NULL == pZ->rb_prev) {
			// first sibling
			P = pZ->rb_parent & XDB_ROWID_MASK;
			pP = XDB_RB_NODE(P);
			pP->color_sibling &= XDB_RB_COLOR;
			Y = pZ->rb_next;
			if (XDB_RB_NULL != Y) {
				// Prompt next sibling to first sibling
				pP->color_sibling |= Y;
				pY = XDB_RB_NODE(Y);
				pY->rb_parent |= P;
				pY->rb_prev = XDB_RB_NULL;
			}
		} else {
			// other sibling
			X = pZ->rb_prev;
			pX = XDB_RB_NODE(X);
			Y = pZ->rb_next;
			pX->rb_next = Y;
			if (XDB_RB_NULL != Y) {
				pY = XDB_RB_NODE(Y);
				pY->rb_prev = X;
			}
		}
	} else if (XDB_RB_NULL != S) {
		/* 
		 * It has sibling
		 */
		pS = XDB_RB_NODE(S);
		// Prompt next sibling to first sibling
		Y = pS->rb_next;
		pS->color_sibling = (pZ->color_sibling&XDB_RB_COLOR) | Y;
		if (XDB_RB_NULL != Y) {
			pY = XDB_RB_NODE(Y);
			pY->rb_parent |= S; // has MSB set already
			pY->rb_prev = XDB_RB_NULL;
		}
		// Prompt first sibling to tree and replace Z 
		X = pZ->rb_left;
		pS->rb_left = X;
		if (XDB_RB_NULL != X) {
			pX = XDB_RB_NODE(X);
			pX->rb_parent = S;
		}
		Y = pZ->rb_right;
		pS->rb_right = Y;
		if (XDB_RB_NULL != Y) {
			pY = XDB_RB_NODE(Y);
			pY->rb_parent = S;
		}
		P = pZ->rb_parent;
		pS->rb_parent = P;
		if (XDB_RB_NULL != P) {
			pP = XDB_RB_NODE(P);
			if (pP->rb_left == Z) {
				pP->rb_left = S;
			} else {
				pP->rb_right = S;
			}
		} else {
			pT->rb_root = S;
		}

	} else {

		/* 
		 * Remove from tree
		 */

		// 1: y = z
		Y = Z;
		pY = XDB_RB_NODE(Y);
		// 2: y-orignal-color = y.color
		y_org_color = pY->color_sibling&XDB_RB_COLOR;
		// 3: if z.left == T.nul
		if (pZ->rb_left == XDB_RB_NULL) {
			// 4: x = z.right
			X = pZ->rb_right;
			// 5: RB-TRANSLPANT(T, z, z.right)
			xdb_rb_transplant (pT, Z, X);
		}
		// 6: elseif z.right == T.nul
		else if (pZ->rb_right == XDB_RB_NULL) {
			// 7: x = z.left
			X = pZ->rb_left;		
			// 8: RB-TRANSLPANT(T, z, z.left)
			xdb_rb_transplant (pT, Z, X);
		} else {
			// 9: else y = TREE-MINIMUM(z.right)
			Y = xdb_rb_minimum (pT, pZ->rb_right);
			pY = XDB_RB_NODE(Y);
			// 10: y-orignal-color = y.color
			y_org_color = pY->color_sibling&XDB_RB_COLOR;
			// 11: x = y.right
			X = pY->rb_right;
			// 12: if y.p == z
			if (pY->rb_parent == Z) {
				// 13: x.p = y
				pX = XDB_RB_NODE(X);
				// X may be nil, so need to set parent for fixup to work
				pX->rb_parent = Y;
			} else {
				// 14: else RB-TRANSLPANT(T, y, y.right)
				xdb_rb_transplant (pT, Y, X);
				// 15: y.right = z.right
				pY->rb_right = pZ->rb_right;
				// 16: y.right.p = y
				XDB_RB_NODE(pY->rb_right)->rb_parent = Y;
			}
			// 17: RB-TRANSLPANT(T, z, y)
			xdb_rb_transplant (pT, Z, Y);
			// 18: y.left = z.left
			pY->rb_left = pZ->rb_left;
			// 19: y.left.p = y
			XDB_RB_NODE(pY->rb_left)->rb_parent = Y;
			// 20: y.color = z.color
			pY->color_sibling &= (~XDB_RB_RED);
			pY->color_sibling |= (pZ->color_sibling&XDB_RB_RED);
		}

		// 21: if y-orignal-color == BLACK
		if (!(y_org_color & XDB_RB_RED)) {
			// 22: RB-DELETE-FIXUP(T, x)
			xdb_rb_delete_fixup (pT, X);
		}

		pT->node_count--;
	}

	// reset curnode
	memset (pZ, 0, sizeof (*pZ));

	pT->row_count--;

	return XDB_OK;
}

static inline xdb_rowid 
xdb_rb_successor (xdb_rbtree_t *pT, xdb_rowid X)
{
	xdb_rowid 			P;
	xdb_rbnode_t 		*pX = XDB_RB_NODE(X), *pP;

	if (XDB_RB_NULL != pX->rb_right) {
		P = xdb_rb_minimum (pT, pX->rb_right);
	} else {
		P = pX->rb_parent;
		pP = XDB_RB_NODE(P);
		while ((XDB_RB_NULL != P) && (X == pP->rb_right)) {
			X = P;
			P = pP->rb_parent;
			pP = XDB_RB_NODE(P);
		} 
	}
	
	return P;
}

static inline xdb_rowid 
xdb_rb_precessor (xdb_rbtree_t *pT, xdb_rowid X)
{
	xdb_rowid 			P;
	xdb_rbnode_t 		*pX = XDB_RB_NODE(X), *pP;

	if (XDB_RB_NULL != pX->rb_left) {
		P = xdb_rb_maximum (pT, pX->rb_left);
	} else {
		P = pX->rb_parent;
		pP = XDB_RB_NODE(P);
		while ((XDB_RB_NULL != P) && (X == pP->rb_left)) {
			X = P;
			P = pP->rb_parent;
			pP = XDB_RB_NODE(P);
		} 
	}
	
	return P;
}

static inline xdb_rowid 
xdb_rb_find (xdb_rbtree_t *pT, xdb_idxfilter_t *pIdxFilter, xdb_rowid *pL, int *pCmp)
{
	int cmp = 0;
	xdb_rowid 			 X;
	xdb_rbnode_t 		*pX;
	xdb_idxm_t			*pIdxm = pIdxFilter->pIdxm;
	xdb_tblm_t 			*pTblm	= pIdxm->pTblm;
	xdb_stgmgr_t		*pStgMgr	= &pTblm->stg_mgr;

	// Find the 1st eq node
	for (X = pT->rb_root; XDB_RB_NULL != X; ) {
		void *pRow = XDB_IDPTR(pStgMgr, X);
		xdb_prefetch (pRow);
		pX = XDB_RB_NODE(X);
		xdb_prefetch (pX);
		*pL = X;
		cmp = xdb_row_cmp (pTblm, pRow, pIdxm->pFields, pIdxFilter->pIdxVals, pIdxFilter->match_cnt);
		if (xdb_likely (cmp < 0)) {
			X = pX->rb_left;
		} else if (xdb_likely (cmp > 0)) {
			X = pX->rb_right;
		} else {
			break;
		}
	}
	*pCmp = cmp;
	return X;
}

static inline xdb_rowid 
xdb_rb_find_minimun (xdb_rbtree_t *pT, xdb_idxfilter_t *pIdxFilter, xdb_rowid X, xdb_rowid *pL)
{
	int cmp;
	xdb_rowid 			 L;
	xdb_idxm_t			*pIdxm = pIdxFilter->pIdxm;
	xdb_tblm_t 			*pTblm	= pIdxm->pTblm;
	xdb_stgmgr_t		*pStgMgr	= &pTblm->stg_mgr;

	// Goto the smallest node
	L = X;
	do {
		X = L;
		L = xdb_rb_precessor (pT, L);
		if (xdb_unlikely (XDB_RB_NULL == L)) {
			break;
		}
		void *pRow = XDB_IDPTR(pStgMgr, L);
		xdb_prefetch (pRow);
		cmp = xdb_row_cmp (pTblm, pRow, pIdxm->pFields, pIdxFilter->pIdxVals, pIdxFilter->match_cnt);
	} while (!cmp);
	*pL = L;
	return X;
}

XDB_STATIC xdb_rowid 
xdb_rbtree_query (xdb_conn_t *pConn, xdb_idxfilter_t *pIdxFilter, xdb_rowset_t *pRowSet)
{
	int					cmp;
	xdb_rowid 			 X, S, L = 0;
	xdb_rbnode_t 		*pX, *pS;
	void 		 		*pRow;
	xdb_idxm_t			*pIdxm = pIdxFilter->pIdxm;
	xdb_rbtree_t 		*pT		= pIdxm->pRbtrHdr;
	xdb_tblm_t 			*pTblm	= pIdxm->pTblm;
	xdb_stgmgr_t		*pStgMgr	= &pTblm->stg_mgr;
	int					count = pIdxFilter->idx_flt_cnt;

	//affect multi-thead performance
#if !defined (XDB_HPO)
	pT->query_times++;
#endif

	xdb_rbtlog ("RBTree cmp %d\n", pIdxFilter->match_opt);

	switch (pIdxFilter->match_opt) {
	case XDB_TOK_EQ:
	case XDB_TOK_GE:
	case XDB_TOK_GT:
		// Find the 1st eq node
		X = xdb_rb_find (pT, pIdxFilter, &L, &cmp);
		if (xdb_likely (XDB_RB_NULL != X)) {
			// Find equl node
			if (xdb_likely (XDB_TOK_GT != pIdxFilter->match_opt)) {
				// GE or EQ
				if (pIdxFilter->match_cnt < pIdxm->fld_count) {
					X = xdb_rb_find_minimun (pT, pIdxFilter, X, &L);
				}
			} else {
				// GT, Goto 1st > node
				do {
					X = xdb_rb_successor (pT, X);
					if (xdb_unlikely (XDB_RB_NULL == X)) {
						break;
					}
					pRow = XDB_IDPTR(pStgMgr, X);
					xdb_prefetch (pRow);
					cmp = xdb_row_cmp (pTblm, pRow, pIdxm->pFields, pIdxFilter->pIdxVals, pIdxFilter->match_cnt);
				} while (!cmp);
			}
		} else {
			// don't find equal node
			if (XDB_TOK_EQ == pIdxFilter->match_opt) {
				// nothing found
				X = XDB_RB_NULL;
			} else {
				// GT case
				X = L;
				if (cmp > 0) {
					X = xdb_rb_successor (pT, X);
				}
			}
		}
		break;

	case XDB_TOK_LT:
	case XDB_TOK_LE:
		// Goto the min node
		if (1 == pIdxm->fld_count) {
			X = xdb_rb_minimum (pT, pT->rb_root);
		} else {
			// Find the 1st eq node
			X = xdb_rb_find (pT, pIdxFilter, &L, &cmp);
			if (xdb_likely (XDB_RB_NULL != X)) {
				X = xdb_rb_find_minimun (pT, pIdxFilter, X, &L);
			} else {
				if (cmp > 0) {
					// precessor is parent
					X = L;
				} else {
					X = XDB_RB_NULL;
				}
			}
		}
		break;

	default:
		return XDB_OK;
	}		

	if (xdb_unlikely (XDB_RB_NULL == X)) {
		return XDB_OK;
	}

	pRow = XDB_IDPTR(pStgMgr, X);
	xdb_prefetch (pRow);
	do {
		// X is first
		if (xdb_unlikely (pIdxFilter->match_opt != XDB_TOK_EQ)) {
			// check if exceed boundary
			bool eq = xdb_row_isequal (pTblm, pRow, pIdxm->pFields, pIdxFilter->pIdxVals, pIdxFilter->match_cnt-1);
			if (!eq) {
				// doesn't satfiy match condition, stop
				break;
			}
#ifdef RBT_TBD
			if (pIdxFilter->match_opt2 > 0) {
				// check the other boundary
				cmp = 0; // cmp the last field
				if (cmp<0 || ((0==cmp)&&(XDB_TOK_LT == pIdxFilter->match_opt2))) {
					xdb_rbtlog ("%d is beyond range\n", X);
					break;
				}
			}
#endif
		}

		if (xdb_likely (xdb_row_valid (pConn, pTblm, pRow, X))) {
			// Compare rest fields
			if ((0 == count) || xdb_row_and_match (pIdxm->pTblm, pRow, pIdxFilter->pIdxFlts, count)) {
				if (xdb_unlikely (-XDB_E_FULL == xdb_rowset_add (pRowSet, X, pRow))) {
					return XDB_OK;
				}
			}
		}

		// Check sibling
		pX = XDB_RB_NODE(X);
		for (S = pX->color_sibling&XDB_ROWID_MASK; XDB_RB_NULL != S; S = pS->rb_next) {
			pS = XDB_RB_NODE(S);
			pRow = XDB_IDPTR(pStgMgr, S);

			if (xdb_likely (xdb_row_valid (pConn, pTblm, pRow, X))) {
				// Compare rest fields
				if ((0 == count) || xdb_row_and_match (pIdxm->pTblm, pRow, pIdxFilter->pIdxFlts, count)) {
					if (xdb_unlikely (-XDB_E_FULL == xdb_rowset_add (pRowSet, S, pRow))) {
						return XDB_OK;
					}
				}
			}
		}

		if ((pIdxFilter->match_cnt == pIdxm->fld_count) && (XDB_TOK_EQ == pIdxFilter->match_opt)) {
			break;
		}

		// Get successor
		X = xdb_rb_successor (pT, X);
		if (xdb_unlikely (XDB_RB_NULL == X)) {
			break;
		}
		pRow = XDB_IDPTR(pStgMgr, X);
		if (XDB_TOK_EQ == pIdxFilter->match_opt) {
			cmp = xdb_row_cmp (pTblm, pRow, pIdxm->pFields, pIdxFilter->pIdxVals, pIdxFilter->match_cnt);
			if (cmp) {
				break;
			}
		}
	} while (1);

	return XDB_OK;
}

XDB_STATIC int 
xdb_rbtree_close (xdb_idxm_t *pIdxm)
{
	char path[XDB_PATH_LEN + 32];
	xdb_tblm_t *pTblm = pIdxm->pTblm;

	xdb_sprintf (path, "%s/T%06d/I%02d.idx", pTblm->pDbm->db_path, XDB_OBJ_ID(pTblm), XDB_OBJ_ID(pIdxm));
	xdb_stg_close (&pIdxm->stg_mgr);

	return XDB_OK;
}

XDB_STATIC int 
xdb_rbtree_drop (xdb_idxm_t *pIdxm)
{
	char path[XDB_PATH_LEN + 32];
	xdb_tblm_t *pTblm = pIdxm->pTblm;

	xdb_sprintf (path, "%s/T%06d/I%02d.idx", pTblm->pDbm->db_path, XDB_OBJ_ID(pTblm), XDB_OBJ_ID(pIdxm));
	xdb_stg_drop (&pIdxm->stg_mgr, path);

	return XDB_OK;
}

XDB_STATIC int 
xdb_rbtree_create (xdb_idxm_t *pIdxm)
{
	xdb_tblm_t *pTblm = pIdxm->pTblm;
	char path[XDB_PATH_LEN + 32];
	xdb_sprintf (path, "%s/T%06d/I%02d.idx", pTblm->pDbm->db_path, XDB_OBJ_ID(pTblm), XDB_OBJ_ID(pIdxm));

	xdb_stghdr_t stg_hdr = {.stg_magic = 0xE7FCFDFB, .blk_flags=XDB_STG_NOALLOC, .blk_size = sizeof(xdb_rbnode_t), 
							.ctl_off = 0, .blk_off = XDB_OFFSET(xdb_rbtree_t, rb_node) + sizeof(xdb_rbnode_t)};
	pIdxm->stg_mgr.pOps = pTblm->stg_mgr.pOps;
	pIdxm->stg_mgr.pStgHdr	= &stg_hdr;
	int rc = xdb_stg_open (&pIdxm->stg_mgr, path, NULL, NULL);
	if (rc != XDB_OK) {
		xdb_errlog ("Failed to create index %s", XDB_OBJ_NAME(pIdxm));
	}

	pIdxm->pRbtrHdr = (xdb_rbtree_t*)pIdxm->stg_mgr.pStgHdr;
	pIdxm->node_cap = XDB_STG_CAP(&pIdxm->stg_mgr);

	return XDB_OK;
}

XDB_STATIC int 
xdb_rbtree_init (xdb_idxm_t *pIdxm)
{
	memset (pIdxm->pRbtrHdr, 0, sizeof(*pIdxm->pRbtrHdr));
	return XDB_OK;
}

XDB_STATIC int 
xdb_rbtree_sync (xdb_idxm_t *pIdxm)
{
	xdb_stg_sync (&pIdxm->stg_mgr,    0, 0, false);
	return 0;
}

static xdb_idx_ops s_xdb_rbtree_ops = {
	.idx_add 	= xdb_rbtree_add,
	.idx_rem 	= xdb_rbtree_rem,
	.idx_query 	= xdb_rbtree_query,
	//.idx_query2	= xdb_rbtree_query2,
	.idx_create = xdb_rbtree_create,
	.idx_drop 	= xdb_rbtree_drop,
	.idx_close 	= xdb_rbtree_close,
	.idx_init	= xdb_rbtree_init,
	.idx_sync	= xdb_rbtree_sync
};
