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
				pVal->val_str.str = pTkn->token;
				pVal->val_str.len = pTkn->tk_len;
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
				pVal->val_str.str = pTkn->token;
				pVal->val_str.len = pTkn->tk_len;
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

error:
	pVal->val_type = XDB_TYPE_NULL;
	return false;
}

static int xdb_json_replace (char* dst, xdb_str_t *pJson, const char *path, xdb_value_t *pVal)
{
	xdb_value_t val;
	int len = pJson->len;
	bool bOk = xdb_json_extract (pJson->str, path, &val);
	if (bOk) {
		int offset = val.val_str.str - pJson->str;
		if (dst != pJson->str) {
			memcpy (dst, pJson->str, offset);
			memcpy (dst + offset, pVal->val_str.str, pVal->val_str.len);
			memcpy (dst + offset + pVal->val_str.len, val.val_str.str + val.val_str.len, pJson->len - offset - val.val_str.len);
		} else {
			memmove (dst + offset + pVal->val_str.len, val.val_str.str + val.val_str.len, pJson->len - offset - val.val_str.len);
			memcpy (dst + offset, pVal->val_str.str, pVal->val_str.len);			
		}
		
		len += pVal->val_str.len - val.val_str.len;
		dst[len] = '\0';
	}
	return len;
}

