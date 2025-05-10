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

#include <regex.h>

typedef struct xdb_str_t {
	char	*str;
	int 	len;
} xdb_str_t;

#define xdb_strcpy(dst,src)			xdb_strncpy(dst, src, sizeof(dst))

#define xdb_sprintf(dst, ...)	do { snprintf(dst, sizeof(dst), __VA_ARGS__); (dst)[sizeof(dst)-1] = '\0'; } while (0)

#ifdef _WIN32
#define strcasecmp				_stricmp	
#define strncasecmp 			_strnicmp
#endif // _WIN32

static inline char* 
xdb_strncpy (char *dst, const char *src, int len)
{
	strncpy (dst, src, len - 1); 
	dst[len-1] = '\0';
	return dst;
}

static inline char* 
xdb_strdup (const char *str, int len)
{
	if (0 == len) {
		len = strlen (str);
	}
	len++;
	void *dst = xdb_malloc(len); 
	if (NULL != dst) {
		memcpy(dst, str, len);
	}
	return dst;
}

static bool __xdb_str_like (const char *string, int strLen, const char *pattern, int patLen, bool bNoCase, bool *bSkipLongMat)
{
	while (patLen && strLen) {
		switch (*pattern) {
		case '%':
			while (patLen && ('%' == pattern[1])) {
				++pattern;
				--patLen;
			}
			if (1 == patLen) {
				return true;
			}
			while (strLen) {
				if (__xdb_str_like (string, strLen, pattern+1, patLen-1, bNoCase, bSkipLongMat)) {
					return true;
				}
				if (*bSkipLongMat) {
					return false;
				}
				++string;
				--strLen;
			}
			*bSkipLongMat = 1;
			return false;
		case '_':
			++string;
			--strLen;
			break;
		case '\\':
			if (patLen >= 2) {
				++pattern;
				--patLen;
			}
		// fall through
		default:
			if (!bNoCase) {
				if (*pattern != *string) {
					return false;
				}
		 	} else if (tolower((int)*pattern) != tolower((int)*string)) {
				return false;
			}
			++string;
			--strLen;
		 	break;
		}
		++pattern;
		--patLen;
		if (0 == strLen) {
			while ('%' == *pattern) {
				++pattern;
				--patLen;
			}
			break;
		}
	}
	if (0 == (patLen + strLen)) {
		return true;
	}
	return false;
}

int xdb_str_like2 (const char *string, int strLen, const char *pattern, int patLen, bool bNoCase)
{
	bool bSkipLongMat = 0;
	return __xdb_str_like (string, strLen, pattern, patLen, bNoCase, &bSkipLongMat);
}

int xdb_str_like (const char *string, const char *pattern, int bNoCase)
{
	return xdb_str_like2 (string, strlen(string), pattern, strlen(pattern), bNoCase);
}

void * xdb_str_regcomp (const char *pattern)
{
	regex_t *pRegex = xdb_calloc (sizeof (regex_t));
	if (NULL == pRegex) {
		return NULL;
	}
	if (0 != regcomp(pRegex, pattern, 0)) {
		xdb_errlog ("Can parse patter: '%s'\n", pattern);
		xdb_free (pRegex);
		pRegex = NULL;
	}
	printf ("new paater %s\n", pattern);
	return pRegex;
}

void xdb_str_regfree (void *pRegExp)
{
	regfree (pRegExp);
	xdb_free (pRegExp);
}

int xdb_str_regexec (const char *string, void *pRegExp)
{
	return 0 == regexec (pRegExp, string, 0, NULL, 0);
}

