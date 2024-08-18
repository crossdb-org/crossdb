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