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

bool xdb_json_extract (const char* json, const char *path, xdb_value_t	*pVal)
{
	xdb_token_type type;
	int len = strlen(path);

	xdb_token_t token = XDB_TOK_INIT((char*)json);

	type = xdb_next_token_mark (&token, false);
	XDB_EXPECT3 (type == XDB_TOK_LB, "Expect {\n");

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

error:
	pVal->val_type = XDB_TYPE_NULL;
	return false;
}

