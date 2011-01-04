/*
   Copyright (c) 2006-2010 Gluster, Inc. <http://www.gluster.com>
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

#ifndef _FILE_SNAPSHOT_H
#define _FILE_SNAPSHOT_H

#ifndef _CONFIG_H
#define _CONFIG_H
#include "config.h"
#endif

#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <dirent.h>
#include <time.h>

#ifdef linux
#ifdef __GLIBC__
#include <sys/fsuid.h>
#else
#include <unistd.h>
#endif
#endif

#ifdef HAVE_SYS_XATTR_H
#include <sys/xattr.h>
#endif

#ifdef HAVE_SYS_EXTATTR_H
#include <sys/extattr.h>
#endif

#include "xlator.h"
#include "inode.h"
#include "compat.h"
#include "timer.h"
#include "file-snapshot-mem-types.h"

#define CHECK_SNAP_CMD(this,name)  do {                         \
                struct posix_private  *priv    = NULL;          \
                priv = this->private;                           \
                if (strrchr (name, priv->snap_cmd_symbol))      \
                        return _gf_true;                        \
                return _gf_false;                               \
        } while (0);

#define GF_SNAP_FILE_KEY        "trusted.glusterfs.snapshot.file"
#define GF_SNAP_TEMP_FILE_PATH  ".gfs-snapshot.part"
#define GF_GFID_KEY             "trusted.gfid"

struct snap_info {
        uint64_t start;
        uint64_t size;
};
struct snap_fds {
        int               fd;
        int               idx_len;
        int               idx_fd;
        struct snap_info *snap_idx;
};

#define GF_MAX_SNAPSHOT_LINKS 20

/**
 * posix_fd - internal structure common to file and directory fd's
 */

struct posix_fd {
	int     fd;      /* fd returned by the kernel */
	int32_t flags;   /* flags for open/creat      */
	char *  path;    /* used by setdents/getdents */
	DIR *   dir;     /* handle returned by the kernel */
        int     flushwrites;
        struct list_head list; /* to add to the janitor list */

        int32_t          snapshot;  /* whether snapshot ? yes/no */
        int32_t          fd_count;  /* length of the fd array in case of snapshot */
        struct snap_fds  snap_fd[GF_MAX_SNAPSHOT_LINKS];

        int              list_snapshots; /* in read, do 'readdir', and send entries */
};


struct posix_private {
	char   *base_path;
	int32_t base_path_length;

        gf_lock_t lock;

        char   *hostname;
        /* Statistics, provides activity of the server */

	struct timeval prev_fetch_time;
	struct timeval init_time;

        time_t last_landfill_check;
        int32_t janitor_sleep_duration;
        struct list_head janitor_fds;
        pthread_cond_t janitor_cond;
        pthread_mutex_t janitor_lock;

	int64_t read_value;    /* Total read, from init */
	int64_t write_value;   /* Total write, from init */
        int64_t nr_files;
/*
   In some cases, two exported volumes may reside on the same
   partition on the server. Sending statvfs info for both
   the volumes will lead to erroneous df output at the client,
   since free space on the partition will be counted twice.

   In such cases, user can disable exporting statvfs info
   on one of the volumes by setting this option.
*/
	gf_boolean_t    export_statfs;

	gf_boolean_t    o_direct;     /* always open files in O_DIRECT mode */


/* 
   decide whether posix_unlink does open (file), unlink (file), close (fd)
   instead of just unlink (file). with the former approach there is no lockout
   of access to parent directory during removal of very large files for the
   entire duration of freeing of data blocks.
*/ 
        gf_boolean_t    background_unlink;

/* janitor thread which cleans up /.trash (created by replicate) */
        pthread_t       janitor;
        gf_boolean_t    janitor_present;
        char *          trash_path;

        char *          snap_cmd_symbol; /* by default '@' */
};

#define POSIX_BASE_PATH(this) (((struct posix_private *)this->private)->base_path)

#define POSIX_BASE_PATH_LEN(this) (((struct posix_private *)this->private)->base_path_length)

#define MAKE_REAL_PATH(var, this, path) do {                            \
		var = alloca (strlen (path) + POSIX_BASE_PATH_LEN(this) + 24); \
                strcpy (var, POSIX_BASE_PATH(this));			\
                strcpy (&var[POSIX_BASE_PATH_LEN(this)], path);		\
        } while (0)



static inline int
gf_set_snapshot_flag_in_inode (xlator_t *this, loc_t *loc)
{
        int ret = -1;
        if (!loc || !loc->inode)
                goto out;

        ret = inode_ctx_put (loc->inode, this, 1);
out:
        return ret;
}

int gf_is_a_snapshot_file (xlator_t *this, const char *path);
int gf_create_fresh_snapshot (xlator_t *this, loc_t *loc, const char *path,
                              const char *snap_name, dict_t *params);
int gf_create_another_snapshot (xlator_t *this, loc_t *loc, const char *path,
                                const char *snap_name, dict_t *params);
int gf_check_and_change_snap_entry (xlator_t *this, const char *path,
                                    struct iatt *stbuf, dict_t *params);
int gf_snap_open_snapshot (xlator_t *this, struct posix_fd *pfd,
                           const char *path, const char *snap_name,
                           int32_t flags);
int gf_snap_readv (call_frame_t *frame, xlator_t *this, struct posix_fd *pfd,
                   off_t offset, size_t size);
int gf_snap_writev_update_index (xlator_t *this, struct snap_fds *pfd,
                                 off_t offset, int32_t op_ret);
int gf_snap_truncate_index (xlator_t *this, struct snap_fds *pfd,
                            off_t offset);

int gf_sync_snap_info_file (struct snap_fds *pfd);
int gf_sync_and_free_pfd (xlator_t *this, struct posix_fd *pfd);
int gf_check_if_snap_path (xlator_t *this, loc_t *loc, char *path,
                           struct iatt *buf, char **pathdup, dict_t *params);
int gf_snap_delete_snapshot (xlator_t *this, loc_t *loc, const char *path,
                             const char *snap_name);
int gf_snap_create_clone (xlator_t *this, const char *oldpath,
                          const char *snap_name, const char *newpath);

int posix_gfid_set (xlator_t *this, const char *path, dict_t *xattr_req);
int posix_lstat_with_gfid (xlator_t *this, const char *path, struct iatt *bufp);
int posix_fstat_with_gfid (xlator_t *this, int fd, struct iatt *stbuf_p);
int gf_snap_delete_full_path (xlator_t *this, const char *path);
int gf_snap_read_index_file (const char *index_path, int32_t open_flag,
                             struct snap_fds *snap);
int gf_snap_rename (xlator_t *this, const char *path, const char *from,
                    const char *to);

#endif /* _FILE_SNAPSHOT_H */
