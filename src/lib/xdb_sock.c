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

#if !defined(_WIN32)

#include <netinet/in.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/types.h>
#include <sys/socket.h>

	#define xdb_sock_open(domain,type,protocol)		socket(domain, type, protocol)
	#define xdb_sock_read(sockfd, buf, len) 		read(sockfd, buf, len)
	#define xdb_sock_write(sockfd, buf, len)		write(sockfd, buf, len)
	#define xdb_sock_close(sockfd)					close(sockfd)
	#define xdb_sock_connect(sockfd,addr,addrlen) 	connect(sockfd,addr,addrlen)
	#define xdb_sock_init()							xdb_signal_block(SIGPIPE)
	#define xdb_sock_exit()

#else

	static int __xdb_winError2Errno(int err) {
		switch (err) {
			case WSAEWOULDBLOCK:
				return EWOULDBLOCK;
			case WSAEINPROGRESS:
				return EINPROGRESS;
			case WSAEALREADY:
				return EALREADY;
			case WSAENOTSOCK:
				return ENOTSOCK;
			case WSAEDESTADDRREQ:
				return EDESTADDRREQ;
			case WSAEMSGSIZE:
				return EMSGSIZE;
			case WSAEPROTOTYPE:
				return EPROTOTYPE;
			case WSAENOPROTOOPT:
				return ENOPROTOOPT;
			case WSAEPROTONOSUPPORT:
				return EPROTONOSUPPORT;
			case WSAEOPNOTSUPP:
				return EOPNOTSUPP;
			case WSAEAFNOSUPPORT:
				return EAFNOSUPPORT;
			case WSAEADDRINUSE:
				return EADDRINUSE;
			case WSAEADDRNOTAVAIL:
				return EADDRNOTAVAIL;
			case WSAENETDOWN:
				return ENETDOWN;
			case WSAENETUNREACH:
				return ENETUNREACH;
			case WSAENETRESET:
				return ENETRESET;
			case WSAECONNABORTED:
				return ECONNABORTED;
			case WSAECONNRESET:
				return ECONNRESET;
			case WSAENOBUFS:
				return ENOBUFS;
			case WSAEISCONN:
				return EISCONN;
			case WSAENOTCONN:
				return ENOTCONN;
			case WSAETIMEDOUT:
				return ETIMEDOUT;
			case WSAECONNREFUSED:
				return ECONNREFUSED;
			case WSAELOOP:
				return ELOOP;
			case WSAENAMETOOLONG:
				return ENAMETOOLONG;
			case WSAEHOSTUNREACH:
				return EHOSTUNREACH;
			case WSAENOTEMPTY:
				return ENOTEMPTY;
			default:
				return EIO;
		}
	}

	#define xdb_sock_open(domain,type,protocol)	socket(domain, type, protocol)
	#define xdb_sock_close(sock)				closesocket(sock)

	XDB_STATIC ssize_t 
	xdb_sock_read(SOCKET sockfd, void *buf, size_t len) 
	{
		int ret = recv(sockfd, buf, len, 0);
		errno = (SOCKET_ERROR != ret) ? 0 : __xdb_winError2Errno(WSAGetLastError());
		return ret != SOCKET_ERROR ? ret : 0;
	}

	XDB_STATIC ssize_t 
	xdb_sock_write(SOCKET sockfd, const void *buf, size_t len)
	{
		int ret = send(sockfd, buf, len, 0);
		errno = (SOCKET_ERROR != ret) ? 0 : __xdb_winError2Errno(WSAGetLastError());
		return ret != SOCKET_ERROR ? ret : 0;
	}

	XDB_STATIC int 
	xdb_sock_connect(SOCKET sockfd, const struct sockaddr *addr, socklen_t addrlen) 
	{
		int ret = connect(sockfd,addr,addrlen);
		errno = (SOCKET_ERROR != ret) ? 0 : __xdb_winError2Errno(WSAGetLastError());
		return ret != SOCKET_ERROR ? ret : 0;
	}

	XDB_STATIC void 
	xdb_sock_init()
	{
	   WORD versionWanted = MAKEWORD(1, 1);
	   WSADATA wsaData;
	   WSAStartup(versionWanted, &wsaData);
	}
	#define socket_exit()	WSACleanup();
#endif

static inline int xdb_sock_SetTcpNoDelay (int fd, int val)
{
#ifndef _WIN32
    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val)) < 0) {
        return -1;
    }
#endif
    return 0;
}

XDB_STATIC void xdb_sockaddr_init (struct sockaddr_in *addr, int port, const char *host)
{
	memset (addr, 0, sizeof(*addr));
	addr->sin_family = AF_INET;
	addr->sin_port	= htons(port);
	in_addr_t hadd = inet_addr (host);
	memcpy (&addr->sin_addr, &hadd, sizeof (addr->sin_addr));
}

static int xdb_sock_vprintf (int sockfd, const char *format, va_list ap)
{
	char buf[32*1024], *pBuf = buf;
	va_list dupArgs;

	va_copy(dupArgs, ap);

    int len = vsnprintf(pBuf, sizeof(buf), format, ap);
	if (len >= sizeof(buf)) {
    	pBuf = (char*) xdb_malloc(len + 1 );
		if (NULL != pBuf) {
			vsnprintf (pBuf, len+1, format, dupArgs);
			pBuf[len] = '\0';
		}
	}

   va_end(dupArgs);

	if (NULL != pBuf) {
		len = xdb_sock_write (sockfd, pBuf, len);
	}
	if (pBuf != buf) {
		xdb_free (pBuf);
	}

	return len;
}

#if 0
static int xdb_sock_printf (int sockfd, const char *format, ...)
{
    va_list 	ap;

	va_start(ap, format);
    int len = xdb_sock_vprintf (sockfd, format, ap);
	va_end(ap);
	
	return len;
}

static int cdb_sock_puts (int sockfd, const char *str)
{
	int len = strlen(str);
	len = xdb_sock_write (sockfd, str, strlen(str));
	return len;
}

static int cdb_sock_putc (int sockfd, const char ch)
{
	return xdb_sock_write (sockfd, &ch, 1);
}
#endif
