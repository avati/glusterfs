/*
   Copyright (c) 2010 Gluster, Inc. <http://www.gluster.com>
   This file is part of GlusterFS.

   GlusterFS is free software; you can redistribute it and/or modify
   it under the terms of the GNU Affero General Public License as published
   by the Free Software Foundation; either version 3 of the License,
   or (at your option) any later version.

   GlusterFS is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Affero General Public License for more details.

   You should have received a copy of the GNU Affero General Public License
   along with this program.  If not, see
   <http://www.gnu.org/licenses/>.
*/

#ifndef _LIBGLUSTERFSCLIENT_SYNC_H
#define _LIBGLUSTERFSCLIENT_SYNC_H

#include "libglusterfsclient-types.h"

#ifdef __cplusplus
extern "C" {
#if 0
}
#endif
#endif


struct glfs_fd *glfs_sync_open (struct glfs_session *session, const char *path,
                                int flags, int mode);

struct glfs_fd *glfs_sync_creat (struct glfs_session *session, const char *path,
                                 int mode);

int glfs_sync_close (struct glfs_fd *fd);


#ifdef __cplusplus
}
#endif

#endif
