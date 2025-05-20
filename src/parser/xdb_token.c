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

#define XDB_TOK_INIT(sql)	{.tk_type=XDB_TOK_INV, .tk_sql=sql}

typedef struct {
	xdb_token_type	tk_type;
	char 			*tk_sql;
	char			tk_nxt;
	//xdb_str_t 	token;
	int				tk_len;
	bool			bFloat;
	char			*token;
} xdb_token_t;

static char s_tok_type[256] = {
	['\0'] = XDB_TOK_EOF,
	[':'] = XDB_TOK_COLON,
	[';'] = XDB_TOK_END,
	['('] = XDB_TOK_LP,
	[')'] = XDB_TOK_RP,
	['{'] = XDB_TOK_LB,
	['}'] = XDB_TOK_RB,
	['['] = XDB_TOK_LBK,
	[']'] = XDB_TOK_RBK,
	[','] = XDB_TOK_COMMA,
	['.'] = XDB_TOK_DOT,
	['\\'] = XDB_TOK_ESC,
	['\''] = XDB_TOK_STR,
	['"'] = XDB_TOK_STR,
	['`'] = XDB_TOK_STR,
	[' '] = XDB_TOK_SP,
	['\t'] = XDB_TOK_SP,
	['\r'] = XDB_TOK_SP,
	['\n'] = XDB_TOK_SP,
	['='] = XDB_TOK_EQ,
	['<'] = XDB_TOK_LT,
	['>'] = XDB_TOK_GT,
	['!'] = XDB_TOK_NEG,
	['+'] = XDB_TOK_ADD,
	['-'] = XDB_TOK_SUB,
	['*'] = XDB_TOK_MUL,
	['/'] = XDB_TOK_DIV,
	['?'] = XDB_TOK_QM,
	['0'] = XDB_TOK_NUM,
	['1'] = XDB_TOK_NUM,
	['2'] = XDB_TOK_NUM,
	['3'] = XDB_TOK_NUM,
	['4'] = XDB_TOK_NUM,
	['5'] = XDB_TOK_NUM,
	['6'] = XDB_TOK_NUM,
	['7'] = XDB_TOK_NUM,
	['8'] = XDB_TOK_NUM,
	['9'] = XDB_TOK_NUM,
};

static const char* xdb_tok2str(xdb_token_type tp) 
{
	const char *id2str[] = {
		[XDB_TOK_ID   ] = "ID",
		[XDB_TOK_NUM  ] = "NUM",
		[XDB_TOK_ESC  ] = "ESC",
		[XDB_TOK_STR  ] = "STR",
		[XDB_TOK_SP   ] = "SP",
		[XDB_TOK_EQ   ] = "=",
		[XDB_TOK_LT   ] = "<",
		[XDB_TOK_LE   ] = "<=",
		[XDB_TOK_GT   ] = ">",
		[XDB_TOK_GE   ] = ">=",
		[XDB_TOK_NE   ] = "<>",
		[XDB_TOK_ADD  ] = "+",
		[XDB_TOK_SUB  ] = "-",
		[XDB_TOK_MUL  ] = "*",
		[XDB_TOK_DIV  ] = "/",
		[XDB_TOK_MOD  ] = "%",
		[XDB_TOK_AND  ] = "AND",
		[XDB_TOK_OR   ] = "OR",
		[XDB_TOK_BXOR  ] = "BXOR",
		[XDB_TOK_NOT  ] = "NOT",
		[XDB_TOK_NEG  ] = "!",
		[XDB_TOK_COMMA] = ",",
		[XDB_TOK_DOT  ] = ".",
		[XDB_TOK_LP   ] = "(",
		[XDB_TOK_RP   ] = ")",
		[XDB_TOK_END  ] = "END",
		[XDB_TOK_EOF  ] = "EOF",
		[XDB_TOK_INV  ] = "INV"
	};
	return tp <= XDB_ARY_LEN(id2str)? id2str[tp] : "Unkonwn";
}

XDB_STATIC xdb_token_type 
xdb_next_token_mark (xdb_token_t *pTkn, bool bSplit)
{
	char *str, ch;
	xdb_token_type tp;

	// last is id, check if next is not space
	if ((XDB_TOK_ID == pTkn->tk_type) || (XDB_TOK_NUM == pTkn->tk_type)) {
		ch = s_tok_type[(uint8_t)pTkn->tk_nxt];
		if (XDB_TOK_SP != ch) {
			switch (ch) {
			case XDB_TOK_GT:
			case XDB_TOK_LT:
			case XDB_TOK_NEG:
			case XDB_TOK_SUB:
				pTkn->tk_sql--;
				goto parse_ch;
			default:
				break;
			}
			pTkn->tk_type = ch;
			return pTkn->tk_type;
		}
	}

	if (XDB_TOK_EOF == pTkn->tk_type) {
		return XDB_TOK_EOF;
	}
    // skip white space
    while (XDB_TOK_SP == s_tok_type[(uint8_t)*pTkn->tk_sql]) {
        pTkn->tk_sql++;
	}

	ch = s_tok_type[(uint8_t)*pTkn->tk_sql];

parse_ch:
	switch (ch) {
	case XDB_TOK_ID:
		if (xdb_unlikely ((('X' == *pTkn->tk_sql) || ('x' == *pTkn->tk_sql)) && '\''== pTkn->tk_sql[1])) {
			char hex1, hex2;
			pTkn->tk_sql += 2;
			pTkn->token = pTkn->tk_sql;
			str = pTkn->token;
			for (; (hex1=s_xdb_str_2_hex[(uint8_t)*pTkn->tk_sql]) >= 0 && (hex2=s_xdb_str_2_hex[(uint8_t)pTkn->tk_sql[1]]) >= 0; pTkn->tk_sql+=2) {
				*str++ = (hex1<<4) | hex2;
			}
			if (xdb_unlikely (*pTkn->tk_sql != '\'')) {
				return XDB_TOK_INV;
			}
			pTkn->tk_len = str - pTkn->token;
			if (bSplit) { *str = '\0'; }
			pTkn->tk_type = XDB_TOK_HEX;
			pTkn->tk_sql++;
			return XDB_TOK_HEX;
		}
		pTkn->tk_type = XDB_TOK_ID;
		pTkn->token = pTkn->tk_sql++;
		ch = s_tok_type[(uint8_t)*pTkn->tk_sql];
		while ((XDB_TOK_ID == ch) || (XDB_TOK_NUM == ch)) {
			pTkn->tk_sql++;
			ch = s_tok_type[(uint8_t)*pTkn->tk_sql];
		}
		pTkn->tk_len = pTkn->tk_sql - pTkn->token;
		pTkn->tk_nxt = *pTkn->tk_sql;
		if (bSplit) { *pTkn->tk_sql = '\0'; }
		pTkn->tk_sql++;
		return XDB_TOK_ID;
	case XDB_TOK_SUB:
		tp = s_tok_type[(uint8_t)*(pTkn->tk_sql+1)];
		if (XDB_TOK_GT == tp) {
			pTkn->tk_sql += 2;
			pTkn->tk_type = XDB_TOK_EXTRACT;
			return XDB_TOK_EXTRACT;
		} else if (XDB_TOK_NUM != tp) {
			break;
		}
		break;
	case XDB_TOK_NUM:
		if (('0' == *pTkn->tk_sql) && (('x' == *(pTkn->tk_sql+1)) || ('X' == *(pTkn->tk_sql+1)))) {
			char hex1, hex2;
			pTkn->tk_sql += 2;
			pTkn->token = pTkn->tk_sql;
			str = pTkn->token;
			for (; (hex1=s_xdb_str_2_hex[(uint8_t)*pTkn->tk_sql]) >= 0 && (hex2=s_xdb_str_2_hex[(uint8_t)pTkn->tk_sql[1]]) >= 0; pTkn->tk_sql+=2) {
				*str++ = (hex1<<4) | hex2;
			}
			if (xdb_unlikely (hex1 >= 0)) {
				return XDB_TOK_INV;
			}
			pTkn->tk_len = str - pTkn->token;
			if (bSplit) { *str = '\0'; }
			pTkn->tk_type = XDB_TOK_HEX;
			return XDB_TOK_HEX;
		}
		pTkn->tk_type = XDB_TOK_NUM;
		pTkn->token = pTkn->tk_sql;
		pTkn->bFloat = false;
		if (XDB_TOK_SUB == s_tok_type[(uint8_t)*pTkn->tk_sql]) {
			pTkn->tk_sql++;
		}
		while (XDB_TOK_NUM == s_tok_type[(uint8_t)*pTkn->tk_sql]) {
			pTkn->tk_sql++;
		}
		if (XDB_TOK_DOT == s_tok_type[(uint8_t)*pTkn->tk_sql]) {
			pTkn->bFloat = true;
			pTkn->tk_sql++;
			while (XDB_TOK_NUM == s_tok_type[(uint8_t)*pTkn->tk_sql]) {
				pTkn->tk_sql++;
			}
		}
		pTkn->tk_len = pTkn->tk_sql - pTkn->token;
		pTkn->tk_nxt = *pTkn->tk_sql;
		if (bSplit) { *pTkn->tk_sql = '\0'; }
		pTkn->tk_sql++;
		return XDB_TOK_NUM;
	case XDB_TOK_STR:
		pTkn->tk_type = XDB_TOK_STR;
		pTkn->token = pTkn->tk_sql++;
		str = pTkn->tk_sql;
		while (*pTkn->tk_sql && *pTkn->token != *pTkn->tk_sql) {
			if (xdb_unlikely ('\\' == *pTkn->tk_sql)) {
				switch (pTkn->tk_sql[1]) {
				case 'n': *str = '\n'; break;
				case 'r': *str = '\r'; break;
				case 't': *str = '\t'; break;
				case '\'':
				case '"':
				case '`':
				case '\\':
				case '%':
				case '_':
					*str = pTkn->tk_sql[1];
					break;
				default: 	return XDB_TOK_INV;
				}
				str++;
				pTkn->tk_sql += 2;				
			} else {
				*str++ = *pTkn->tk_sql++;
			}
		}
		if ('\0' == *pTkn->tk_sql) {
			return XDB_TOK_INV;
		}
		pTkn->token++;
		pTkn->tk_len = str - pTkn->token;
		if (bSplit) { *str = '\0'; }
		pTkn->tk_sql++;
		return XDB_TOK_STR;
	case XDB_TOK_GT:
		ch = s_tok_type[(uint8_t)*(pTkn->tk_sql+1)];
		if (XDB_TOK_EQ == ch) {
			pTkn->tk_sql += 2;
			pTkn->tk_type = XDB_TOK_GE;
			return XDB_TOK_GE;
		}
		pTkn->tk_sql++;
		pTkn->tk_type = XDB_TOK_GT;
		return XDB_TOK_GT;
	case XDB_TOK_LT:
		ch = s_tok_type[(uint8_t)*(pTkn->tk_sql+1)];
		if (XDB_TOK_EQ == ch) {
			pTkn->tk_sql += 2;
			pTkn->tk_type = XDB_TOK_LE;
			return XDB_TOK_LE;
		} else if (XDB_TOK_GT == ch) {
			pTkn->tk_sql += 2;
			pTkn->tk_type = XDB_TOK_NE;
			return XDB_TOK_NE;
		}
		pTkn->tk_sql++;
		pTkn->tk_type = XDB_TOK_LT;
		return XDB_TOK_LT;
	case XDB_TOK_NEG:
		ch = s_tok_type[(uint8_t)*(pTkn->tk_sql+1)];
		if (XDB_TOK_EQ == ch) {
			pTkn->tk_sql += 2;
			pTkn->tk_type = XDB_TOK_NE;
			return XDB_TOK_NE;
		}
		break;
	default:
		break;
	}

	pTkn->token = pTkn->tk_sql;
	pTkn->tk_type = s_tok_type[(uint8_t)*pTkn->tk_sql++];
	return pTkn->tk_type;
}

XDB_STATIC xdb_token_type 
xdb_next_token (xdb_token_t *pTkn)
{
	return xdb_next_token_mark (pTkn, true);
}

XDB_STATIC xdb_token_type 
xdb_next_token2 (xdb_token_t *pTkn, char ch)
{
	pTkn->tk_sql = strchr (pTkn->tk_sql, ch);
	if (NULL == pTkn->tk_sql) {
		return XDB_TOK_ERR;
	}
	*pTkn->tk_sql++ = '\0';
	pTkn->tk_type	= 0;
	return xdb_next_token (pTkn);
}
