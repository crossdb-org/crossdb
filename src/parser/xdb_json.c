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

static bool xdb_json_match_ary (xdb_token_t *pTkn, const char *path, int len, xdb_value_t	*pVal);

static bool xdb_json_match_obj (xdb_token_t *pTkn, const char *path, int len, xdb_value_t	*pVal)
{
	bool bFound;

	while (1) {
		xdb_token_type type = xdb_next_token_mark (pTkn, false);
		XDB_EXPECT3 (type == XDB_TOK_STR, "expect field, but got %s\n", xdb_tok2str(type));
		bFound = (path && (pTkn->tk_len == len) && !memcmp (pTkn->token, path, len));

		type = xdb_next_token_mark (pTkn, false);
		XDB_EXPECT3 (type == XDB_TOK_COLON, "expect :, but got %s\n", xdb_tok2str(type));
		type = xdb_next_token_mark (pTkn, false);

		switch (type) {
		case XDB_TOK_LB:
			bFound = xdb_json_match_obj (pTkn, NULL, 0, NULL);
			if (bFound) {
				return true;
			}
			break;
		case XDB_TOK_LBK:
			bFound = xdb_json_match_ary (pTkn, NULL, 0, NULL);
			if (bFound) {
				return true;
			}
			break;
		case XDB_TOK_RB:
			return false;
		case XDB_TOK_NUM:
			if (bFound) {
				char buf[128];
				XDB_EXPECT3 (pTkn->tk_len < sizeof(buf), "too big num");
				memcpy (buf, pTkn->token, pTkn->tk_len);
				buf[pTkn->tk_len] = '\0';
				if (!pTkn->bFloat) {
					pVal->ival = atoll (pTkn->token);
					pVal->val_type = XDB_TYPE_BIGINT;
				} else {
					pVal->fval = atof (pTkn->token);
					pVal->val_type = XDB_TYPE_DOUBLE;
				}
				return true;
			}
			break;
		case XDB_TOK_STR:
			if (bFound) {
				pVal->val_type = XDB_TYPE_CHAR;
				pVal->str.str = pTkn->token;
				pVal->str.len = pTkn->tk_len;
				return true;
			}
			break;
		default:
			XDB_EXPECT3 (0, "wrong json format %s\n", xdb_tok2str(type));
		}

		type = xdb_next_token_mark (pTkn, false);
		if (XDB_TOK_RB == type) {
			return false;
		}
		XDB_EXPECT3 (XDB_TOK_COMMA == type, "expect , but got %s\n", xdb_tok2str(type));
	}

error:
	pVal->val_type = XDB_TYPE_NULL;
	return false;
}

static bool xdb_json_match_ary (xdb_token_t *pTkn, const char *path, int len, xdb_value_t	*pVal)
{
	bool bFound;
	while (1) {
		xdb_token_type type = xdb_next_token_mark (pTkn, false);
		switch (type) {
		case XDB_TOK_LB:
			bFound = xdb_json_match_obj (pTkn, NULL, 0, NULL);
			if (bFound) {
				return true;
			}
			break;
		case XDB_TOK_LBK:
			bFound = xdb_json_match_ary (pTkn, NULL, 0, NULL);
			if (bFound) {
				return true;
			}
			continue;
		case XDB_TOK_RBK:
			return false;
		case XDB_TOK_NUM:
			break;
		case XDB_TOK_STR:
			break;
		default:
			XDB_EXPECT3 (0, "wrong json format %s\n", xdb_tok2str(type));			
		}

		type = xdb_next_token_mark (pTkn, false);
		if (XDB_TOK_RBK == type) {
			return false;
		}
		XDB_EXPECT3 (XDB_TOK_COMMA == type, "expect , but got %s\n", xdb_tok2str(type));
	}

error:
	return false;
}

static bool xdb_json_extract (const char* json, const char *path, xdb_value_t *pVal)
{
	xdb_token_type type;
	int len = strlen(path);

	xdb_token_t token = XDB_TOK_INIT((char*)json);

	type = xdb_next_token_mark (&token, false);
	
	XDB_EXPECT3 (type == XDB_TOK_LB, "Expect {\n");

	bool bFound = xdb_json_match_obj (&token, path, len, pVal);
	if (bFound) {
		return true;
	}

#if 0
	while (1) {
		type = xdb_next_token_mark (&token, false);
		XDB_EXPECT3 (type == XDB_TOK_STR, "Expect ID %d\n", type);

		//printf ("json %.*s\n", token.tk_len, token.token);

		if ((token.tk_len == len) && !memcmp (token.token, path, len)) {
			type = xdb_next_token_mark (&token, false);
			XDB_EXPECT3 (type == XDB_TOK_COLON, "expect :\n");
			type = xdb_next_token_mark (&token, false);
			//printf ("found tken %s value %.*s\n", path, token.tk_len, token.token);
			if (XDB_TOK_STR == type) {
				pVal->val_type = XDB_TYPE_CHAR;
				pVal->str.str = token.token;
				pVal->str.len = token.tk_len;
			} else if (XDB_TOK_NUM == type) {
				char buf[128];
				XDB_EXPECT3 (token.tk_len < sizeof(buf), "too big num");
				memcpy (buf, token.token, token.tk_len);
				buf[token.tk_len] = '\0';
				if (!token.bFloat) {
					pVal->ival = atoll (token.token);
					pVal->val_type = XDB_TYPE_BIGINT;
				} else {
					pVal->fval = atof (token.token);
					pVal->val_type = XDB_TYPE_DOUBLE;
				}
			} else {
				xdb_errlog ("expect str or num for json but got %d\n", type);
				goto error;
			}
			return true;
		}
		type = xdb_next_token_mark (&token, false);
		XDB_EXPECT3 (type == XDB_TOK_COLON, "expect :\n");
		// skip value
		type = xdb_next_token_mark (&token, false);
		
		type = xdb_next_token_mark (&token, false);
		if (type == XDB_TOK_COMMA) {
			continue;
		} else if (type == XDB_TOK_RB) {
			goto error;
		}
	}
#endif

error:
	pVal->val_type = XDB_TYPE_NULL;
	return false;
}

