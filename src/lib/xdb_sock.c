/******************************************************************************
* Copyright (c) 2024-present JC Wang. All rights reserved
*
*   https://crossdb.org
*   https://github.com/crossdb-org/crossdb
*
* This program is free software: you can use, redistribute, and/or modify it under
* the terms of the GNU Lesser General Public License, version 3 or later ("LGPL"),
* as published by the Free Software Foundation.
*
* This program is distributed in the hope that it will be useful, but WITHOUT
* ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS 
* FOR A PARTICULAR PURPOSE. See the GNU published General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public License along with 
* this program. If not, see <http://www.gnu.org/licenses/>.
******************************************************************************/

#include <netinet/in.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/types.h>
#include <sys/socket.h>

#if !defined(_WIN32)

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

