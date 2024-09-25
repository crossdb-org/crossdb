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

const uint8_t s_XDB_TOK_pri[] =
{
	// logical
	[XDB_TOK_OR]	= 0,
	[XDB_TOK_AND]	= 1,

	// comparison operator
	[XDB_TOK_EQ]	= 2,
	[XDB_TOK_NE]	= 2,
	[XDB_TOK_GE]	= 2,
	[XDB_TOK_GT]	= 2,
	[XDB_TOK_LE]	= 2,
	[XDB_TOK_LT]	= 2,
	[XDB_TOK_IN]	= 2,
	[XDB_TOK_BTWN]	= 2,
		
	// bit operator: | & ^ << >>

	// arithmetic operator
	[XDB_TOK_ADD]	= 6,
	[XDB_TOK_SUB]	= 6,
	[XDB_TOK_MUL]	= 7,
	[XDB_TOK_DIV]	= 7,
	[XDB_TOK_MOD]	= 7,

	[XDB_TOK_LP]	= 10,
	[XDB_TOK_RP]	= 10,
};

int xdb_expr_eval (xdb_value_t *pOut, xdb_value_t *pExpVal, int id, int count)
{
	int nid;
	int pri = s_XDB_TOK_pri[pExpVal[id+1].sup_type];

	for (;id < count; id = nid) {
		int nid;
		xdb_value_t *pVal, val;
		if ((id+3 < count) && (s_XDB_TOK_pri[pExpVal[id+3].sup_type] > pri)) {
			val.sup_type = XDB_TYPE_BIGINT;
			val.ival = 0;
			nid = xdb_expr_eval (&val, pExpVal, id + 1, count);
			pVal = &val;
		} else {
			nid = id + 3;
			pVal = &pExpVal[id+3];
		}

		switch (pVal[id+1].sup_type) {
		case XDB_TOK_ADD:
			switch (pVal->sup_type) {
			case XDB_TOK_BIGINT:
				pOut->ival += pVal->ival;
				break;
			case XDB_TOK_DOUBLE:
				pOut->fval += pVal->fval;
				break;
			}
			break;
		case XDB_TOK_SUB:
			switch (pVal[id].sup_type) {
			case XDB_TOK_BIGINT:
				pOut->ival -= pVal->ival;
				break;
			case XDB_TOK_DOUBLE:
				pOut->fval -= pVal->fval;
				break;
			}
		case XDB_TOK_MUL:
			switch (pVal[id].sup_type) {
			case XDB_TOK_BIGINT:
				pOut->ival *= pVal->ival;
				break;
			case XDB_TOK_DOUBLE:
				pOut->fval *= pVal->fval;
				break;
			}
		}
	}
}

void expr_test ()
{
	xdb_value_t expr[] = {
		{.sup_type = XDB_TOK_BIGINT, .ival = 4},
		{.sup_type = XDB_TOK_ADD},
		{.sup_type = XDB_TOK_BIGINT, .ival = 3},
	};

	xdb_value_t val = {.sup_type = XDB_TYPE_BIGINT, .ival = 0};
	xdb_expr_eval (&val, expr, 0, 3);
	printf ("%d\n", val.ival);
}
