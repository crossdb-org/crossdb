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

typedef struct xdb_str_t {
	char	*str;
	int 	len;
} xdb_str_t;

#define xdb_strcpy(dst,src)			xdb_strncpy(dst, src, sizeof(dst))

#define xdb_sprintf(dst, fmt...)	do { snprintf(dst, sizeof(dst), fmt); (dst)[sizeof(dst)-1] = '\0'; } while (0)

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
