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

#ifndef _LIBGLUSTERFSCLIENT_H
#define _LIBGLUSTERFSCLIENT_H

#ifndef LIBGLUSTERFSCLIENT_USE_VERSION
#define LIBGLUSTERFSCLIENT_USE_VERSION 310
#endif


#include "libglusterfsclient-types.h"
#include "libglusterfsclient-sync.h"
#include "libglusterfsclient-async.h"


#ifdef __cplusplus
extern "C" {
#if 0
}
#endif
#endif

struct glfs_session *glfs_session_new (void);

int glfs_session_init_volfile (struct glfs_session *session,
                               const char *volfile);

int glfs_session_init_volume (struct glfs_session *session, const char *server,
                              const char *volume);

#ifdef __cplusplus
}
#endif

#endif /* !_LIBGLUSTERFSCLIENT_H */
