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


#define xdb_fclose(ptr)			do { if(ptr) { fclose(ptr); ptr=NULL; } } while (0)
#define xdb_fdclose(fd)			do { if(fd) { xdb_fdclose2(fd); fd=XDB_INV_FD; } } while (0)
#define	xdb_fexist(file) 		(access(file, F_OK) != -1)

#if 0
XDB_STATIC int 
xdb_getdir (const char *dir, char *path)
{
	int ret;
	char curpath[XDB_PATH_LEN + 1];

	if (NULL == getcwd (curpath, XDB_PATH_LEN)) {
		xdb_errlog ("Can't get current dir\n");
		return -1;
	}
	if ((NULL == dir) || ('\0' == *dir)) {
		strcpy (path, curpath);
	} else {
	 	ret = chdir(dir);
		if (0 != ret) {
			path[0] = '\0';
			return 0;
		}
		if (NULL == getcwd (path, XDB_PATH_LEN)) {
			xdb_errlog ("Can't get current dir\n");
			return -1;
		}
		ret = chdir (curpath);
		if (0 != ret) {
			xdb_errlog ("Can't chdir %s\n", curpath);
			return -1;
		}
	}
	return 0;
}
#endif

XDB_STATIC int 
xdb_rmdir (const char *dirname)
{
    DIR *dir;
    struct dirent *entry;
    char path[512];
    struct stat st;    
	int		ret;

    dir = opendir(dirname);
    if (dir == NULL) {
        // printf ("opendir %s failed\n", dirname);
        return -1;
    }

    while ((entry = readdir(dir)) != NULL) {
        if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, "..")) {
			continue;
		}
		snprintf(path, sizeof(path), "%s/%s", dirname, entry->d_name);
		path[sizeof(path) - 1] = '\0';
		//if (entry->d_type == DT_DIR) { // Windows N/A
        ret = stat (path, &st);
		if (ret < 0) {
			if (ret != 0) {
				xdb_errlog ("stat %s error %s\n", path, strerror(errno));
			}
			continue;
		}
        if (S_ISDIR(st.st_mode)) {
			//printf ("enter dir %s\n", entry->d_name);
			xdb_rmdir (path);
		} else {
			//printf ("remove file %s\n", entry->d_name);
            ret = remove (path);
			if (ret != 0) {
				xdb_errlog ("remove %s error %s\n", path, strerror(errno));
			}
		}
    }
    closedir (dir);

	//printf ("remove dir %s\n", dirname);
    ret = rmdir (dirname);
	if (ret != 0) {
		xdb_errlog ("rmdir %s error -> ret %d %s\n", dirname, ret, strerror(errno));
	}

	return ret;
}


/******************************************************************************
	Linux
******************************************************************************/

#ifndef _WIN32

#define XDB_MODE 	(S_IRWXU|S_IRWXG|S_IRWXO)

XDB_STATIC xdb_fd 
xdb_fdopen (const char *file)
{
	int fd = open (file, O_CREAT|O_RDWR, XDB_MODE);
	if (fd < 0) {
		xdb_errlog ("Can't open file %s err -> %d %s\n", file, fd, strerror(errno));
		return XDB_INV_FD;
	}
	return fd;
}

XDB_STATIC int 
xdb_fdclose2 (xdb_fd fd)
{
	int err = close (fd);
	if (err < 0) {
		xdb_errlog ("Can't close fd %d -> err %d %s\n", fd, err, strerror(errno));
	}
	return err;
}

XDB_STATIC int 
xdb_mkdir (const char *dir)
{
	if (xdb_fexist(dir)) {
		return 0;
	}
	int err  = mkdir (dir, XDB_MODE);
	int ret2 = chmod (dir, XDB_MODE);
	if (err < 0) {
	} else if (ret2 < 0) {
		return ret2;
	}
	return err;
}

XDB_STATIC xdb_size 
xdb_fsize (xdb_fd fd)
{
	struct stat stat;
	if (fstat (fd, &stat) < 0) {
		return -1;
	}
	return (xdb_size)stat.st_size;
}

XDB_STATIC int 
xdb_fsync (xdb_fd fd)
{
	int err = fsync (fd);
	if (err < 0) {
		xdb_errlog ("Can't fsync fd %d -> err %d %s\n", fd, err, strerror(errno));
	}
	return err;
}

XDB_STATIC int 
xdb_ftruncate (xdb_fd fd, uint64_t length)
{
	int err = 0;

#ifdef __APPLE__

	err = ftruncate (fd, (size_t)length);
	if (err < 0) {
		xdb_errlog ("Can't ftruncate fd %d length %"PRIu64" -> err %d %s\n", fd, length, err, strerror(errno));
	}

#else

	xdb_size fsize = xdb_fsize (fd);

	if (length < fsize) {
		err = ftruncate (fd, (size_t)length);
		if (err < 0) {
			xdb_errlog ("Can't ftruncate fd %d length %"PRIu64" -> err %d %s\n", fd, length, err, strerror(errno));
		}
	} else if (length > fsize) {
		// err = fallocate (fd, 0, fsize, length - fsize);
		err = posix_fallocate (fd, fsize, length - fsize);
		if (err > 0) {
			xdb_errlog ("Can't fallocate fd %d length %"PRIu64"(+%"PRIu64") -> err %d %s\n", fd, length, length - fsize, err, strerror(err));
			err = -err;
		}
	}
#endif
	return err;
}

#endif // #ifndef _WIN32


/******************************************************************************
	Windows
******************************************************************************/

#ifdef _WIN32

XDB_STATIC xdb_fd 
xdb_fdopen (const char *file)
{
	HANDLE fHandle = CreateFile(file,
						  GENERIC_READ | GENERIC_WRITE,
						  FILE_SHARE_READ | FILE_SHARE_WRITE,
						  NULL,
						  OPEN_ALWAYS,
						  FILE_ATTRIBUTE_NORMAL,
						  NULL);

	//printf ("dumpFileDescriptor 0x%x\n", dumpFileDescriptor);
	if (WIN_INVALID_HANDLE (fHandle)) {
		xdb_errlog ("Can't open file %s\n", file);
		return XDB_INV_FD;
	}

	return fHandle;
}

XDB_STATIC int 
xdb_fdclose2 (xdb_fd fd)
{
	int err = CloseHandle (fd) ? 0 : -1;
	if (err < 0) {
		xdb_errlog ("Can't close fd %p -> err %d\n", fd, err);
	}
	return err;
}

XDB_STATIC int 
xdb_mkdir (const char *dir)
{
	if (xdb_fexist(dir)) {
		return 0;
	}
	int err  = mkdir (dir);
	return err;
}

XDB_STATIC xdb_size 
xdb_fsize (xdb_fd fd)
{
	LARGE_INTEGER fsize;

	if (!GetFileSizeEx(fd, &fsize)) {
		return -1;
	}

	return (xdb_size)fsize.QuadPart;
}

XDB_STATIC int 
xdb_fsync (xdb_fd fd)
{
	// _commit () is similar to fsync
	int err = FlushFileBuffers (fd) ? 0 : -1;
	if (err < 0) {
		xdb_errlog ("Can't FlushFileBuffers fd %p -> err %d\n", fd, err);
	}
	return err;
}

XDB_STATIC int 
xdb_ftruncate (xdb_fd fd, uint64_t length)
{
	LONG upperBits = length>>32;
	int err  = SetFilePointer (fd, length&0xFFFFFFFF, &upperBits, FILE_BEGIN) ? 0 : -1;
	int ret2 = SetEndOfFile (fd) ? 0 : -1;
	if (err < 0) {
		xdb_errlog ("Can't SetFilePointerEx fd %p length %"PRIu64" -> err %d\n", fd, length, err);
	} else if (ret2 < 0) {
		xdb_errlog ("Can't SetEndOfFile fd %p length %"PRIu64" -> err %d\n", fd, length, ret2);
		return ret2;
	}
	return err;
}

#endif // #ifdef _WIN32


/******************************************************************************
	XDB File lock for Linux
******************************************************************************/

#ifndef _WIN32
XDB_STATIC int 
xdb_file_rdlock (xdb_fd fd, uint64_t start, int len)
{
	int rc;
	struct flock lock_info;
	lock_info.l_start = start;
	lock_info.l_len = len;
	lock_info.l_type = F_RDLCK;
	lock_info.l_whence = SEEK_SET;
	while ((rc = fcntl(fd, F_SETLKW, &lock_info)) && ((rc = errno) == EINTR)) {
		xdb_errlog ("Can't lock %d, retry\n", errno);
	}
	return rc !=0 ? -XDB_ERROR : XDB_OK;
}

XDB_STATIC int 
xdb_file_wrlock (xdb_fd fd, uint64_t start, int len)
{
	int rc;
	struct flock lock_info;
	lock_info.l_start = start;
	lock_info.l_len = len;
	lock_info.l_type = F_WRLCK;
	lock_info.l_whence = SEEK_SET;
	while ((rc = fcntl(fd, F_SETLKW, &lock_info)) && ((rc = errno) == EINTR)) {
		xdb_errlog ("Can't lock %d, retry\n", errno);
	}
	return rc !=0 ? -XDB_ERROR : XDB_OK;
}

XDB_STATIC int 
xdb_file_unlock (xdb_fd fd, uint64_t start, int len)
{
	int rc;
	struct flock lock_info;
	lock_info.l_start = start;
	lock_info.l_len = len;
	lock_info.l_type = F_UNLCK;
	lock_info.l_whence = SEEK_SET;
	while ((rc = fcntl(fd, F_SETLKW, &lock_info)) && ((rc = errno) == EINTR)) {
		xdb_errlog ("Can't lock %d, retry\n", errno);
	}
	return rc !=0 ? -XDB_ERROR : XDB_OK;
}
#endif // #ifndef _WIN32


/******************************************************************************
	XDB File lock for Windows
******************************************************************************/

#ifdef _WIN32
XDB_STATIC int 
xdb_file_rdlock (xdb_fd fd, uint64_t start, int len)
{
	int rc = 0;
	OVERLAPPED ov = {start};

	if (!LockFileEx(fd, 0, 0, len, 0, &ov)) {
		rc = GetLastError();
		xdb_errlog ("Can't rdlock file %d\n", rc);
	}
	return rc !=0 ? -XDB_ERROR : XDB_OK;
}

XDB_STATIC int 
xdb_file_wrlock (xdb_fd fd, uint64_t start, int len)
{
	int rc = 0;
	OVERLAPPED ov = {start};

	if (!LockFileEx(fd, LOCKFILE_EXCLUSIVE_LOCK, 0, len, 0, &ov)) {
		rc = GetLastError();
		xdb_errlog ("Can't wrlock file %d\n", rc);
	}
	return rc !=0 ? XDB_ERROR : XDB_OK;
}

XDB_STATIC int 
xdb_file_unlock (xdb_fd fd, uint64_t start, int len)
{
	OVERLAPPED ov = {start};
	UnlockFileEx(fd, 0, len, 0, &ov);
	return XDB_OK;
}
#endif // #ifdef _WIN32


/******************************************************************************
	Module Test
******************************************************************************/

#ifdef XDB_MOD_TEST
int main (int argc, char *argv[])
{
	return 0;
}
#endif
