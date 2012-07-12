/*
  Copyright (c) 2012 Red Hat, Inc. <http://www.redhat.com>
  This file is part of GlusterFS.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/


#ifndef _GLFS_H
#define _GLFS_H

/*
  Enforce the following flags as libgfapi is built
  with them, and we want programs linking against them to also
  be built with these flags. This is necessary as it affects
  some of the structures defined in libc headers (like struct stat)
  and those definitions need to be consistently compiled in
  both the library and the application.
*/

#ifndef _FILE_OFFSET_BITS
#define _FILE_OFFSET_BITS 64
#endif

#ifndef __USE_FILE_OFFSET64
#define __USE_FILE_OFFSET64
#endif

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/uio.h>
#include <unistd.h>
#include <sys/cdefs.h>

__BEGIN_DECLS

struct glfs;
typedef struct glfs glfs_t;

glfs_t *glfs_new (const char *volname);
int glfs_set_volfile (glfs_t *fs, const char *volfile);
int glfs_set_volfile_server (glfs_t *fs, const char *transport,
				 const char *host, int port);
int glfs_set_logging (glfs_t *fs, const char *logfile, int loglevel);

int glfs_init (glfs_t *fs);
void __glfs_entry (glfs_t *fs);

struct glfs_fd;
typedef struct glfs_fd glfs_fd_t;

glfs_fd_t *glfs_open (glfs_t *fs, const char *path, int flags);
glfs_fd_t *glfs_creat (glfs_t *fs, const char *path, int flags,
		       mode_t mode);
int glfs_close (glfs_fd_t *fd);

ssize_t glfs_read (glfs_fd_t *fd, void *buf, size_t count);
ssize_t glfs_write (glfs_fd_t *fd, const void *buf, size_t count);

ssize_t glfs_readv (glfs_fd_t *fd, const struct iovec *iov, int iovcnt);
ssize_t glfs_writev (glfs_fd_t *fd, const struct iovec *iov, int iovcnt);

ssize_t glfs_preadv (glfs_fd_t *fd, const struct iovec *iov, int iovcnt,
		     off_t offset);
ssize_t glfs_pwritev (glfs_fd_t *fd, const struct iovec *iov, int iovcnt,
		      off_t offset);

int glfs_truncate (glfs_t *fs, const char *path, struct stat *buf);
int glfs_ftruncate (glfs_fd_t *fd, off_t length);

int glfs_lstat (glfs_t *fs, const char *path, struct stat *buf);
int glfs_fstat (glfs_fd_t *fd, struct stat *buf);

int glfs_fsync (glfs_fd_t *fd);
int glfs_fdatasync (glfs_fd_t *fd);

__END_DECLS

#endif /* !_GLFS_H */
