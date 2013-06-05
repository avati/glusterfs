/*
  Copyright (c) 2013 Red Hat, Inc. <http://www.redhat.com>
  This file is part of GlusterFS.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/


#ifndef _GLFS_JAVA_H
#define _GLFS_JAVA_H

#include <stdbool.h>
#include "glfs.h"

long glfs_java_file_length (glfs_t *fs, const char *path);
bool glfs_java_file_exists (glfs_t *fs, const char *path);

long glfs_java_read (glfs_fd_t *fd, void *io_data, size_t size);
long glfs_java_write (glfs_fd_t *fd, void *io_data, size_t size);

bool glfs_java_file_createNewFile (glfs_t *fs, const char *path);
bool glfs_java_file_mkdir (glfs_t *fs, const char *path);

#endif /* !_GLFS_JAVA_H */
