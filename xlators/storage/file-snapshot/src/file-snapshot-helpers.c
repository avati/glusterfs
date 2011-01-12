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

#ifndef _CONFIG_H
#define _CONFIG_H
#include "config.h"
#endif

#include "file-snapshot.h"
#include "file-snapshot-mem-types.h"
#include "syscall.h"
#include "byte-order.h"

static int
sort_info_array (struct snap_info *curr, struct snap_info **new, int32_t len)
{
        int      idx      = 0;
        int      i        = 0;
        uint64_t tmp      = 0;

        struct snap_info *trav = NULL;

        trav = GF_CALLOC (sizeof (struct snap_info), len, 0);

        memcpy (trav, curr, (sizeof (struct snap_info) * len));

        /* Sort first */
        for (idx = 0; idx < len; idx++) {
                for (i = idx+1; i < len; i++) {
                        if (trav[idx].start > trav[i].start) {
                                tmp = trav[idx].start;
                                trav[idx].start = trav[i].start;
                                trav[i].start = tmp;
                                tmp = trav[idx].size;
                                trav[idx].size  = trav[i].size;
                                trav[i].size = tmp;
                        }
                }
        }

        /* Merge now */
        idx = 0;
        for (i = 1; i < (len-1); i++) {
                /* Increment the count */
                /*
                |------|
                          |-----|
                */
                if ((trav[idx].start + trav[idx].size) < trav[i].start) {
                        idx++;
                        trav[idx].start = trav[i].start;
                        trav[idx].size = trav[i].size;
                        continue;
                }

                /* Increase the 'size' */
                /*
                |------|
                       |------|


                |------|
                     |------|


                |------|
                |-----------|

                */
                if ((trav[idx].start + trav[idx].size) < (trav[i].start +
                                                          trav[i].size)) {
                        tmp = trav[i].size + trav[i].start;
                        trav[idx].size = tmp - trav[idx].start;
                        continue;
                }

                /* neglect the entry */
                /*
                |------|
                   |--|


                |------|
                |----|

                */
        }

        /* the latest 'idx' still points to valid entry, so increment it */
        idx++;

        *new = trav;

        if (len > idx) {
                gf_log ("sorting", GF_LOG_DEBUG, "reduced number of entries "
                        "to %d from %d", idx, len);
        }

        return idx;
}

int
gf_sync_snap_info_file (struct snap_fds *snap)
{
        struct snap_info *new_info = NULL;
        int               ret      = 0;
        int               len      = 0;
        int               new_len  = 0;
        uint64_t          start    = 0;
        uint64_t          size     = 0;

        if (!snap)
                return 0;

        len = (snap->idx_len * sizeof (struct snap_info));

        if (!len)
                goto write_index_file;

        new_len = sort_info_array (snap->snap_idx, &new_info,
                                   snap->idx_len);

        if (new_len < snap->idx_len) {
                /* Update the current 'snap_idx' array */
                snap->idx_len = new_len;
                memcpy (snap->snap_idx, &new_info, (new_len *
                                                    sizeof (struct snap_info)));
        }
        for (ret = 0; ret < new_len; ret++) {
                start = hton64 (new_info[ret].start);
                size  = hton64 (new_info[ret].size);
                new_info[ret].size  = size;
                new_info[ret].start = start;
        }

        len = (new_len * sizeof (struct snap_info));

write_index_file:
        lseek (snap->idx_fd, 0, SEEK_SET);
        if (len)
                ret = write (snap->idx_fd, (void *)new_info, len);
        else
                ret = ftruncate (snap->idx_fd, 0);

        if (new_info)
                GF_FREE (new_info);
        return ret;
}

int
gf_sync_and_free_pfd (xlator_t *this, struct posix_fd *pfd)
{
        int i = 0;

        gf_sync_snap_info_file (&pfd->snap_fd[0]);

        for (i = 1; i < pfd->fd_count; i++) {
                close (pfd->snap_fd[i].fd);
                GF_FREE (pfd->snap_fd[i].snap_idx);
                pfd->snap_fd[i].snap_idx = NULL;
        }
        close (pfd->snap_fd[0].idx_fd);
        GF_FREE (pfd->snap_fd[0].snap_idx);
        pfd->snap_fd[0].snap_idx = NULL;

        pfd->snap_fd[0].idx_len = 0;

        return 0;
}

static int
gf_create_snap_index(const char *path, const char *snap_name, off_t start,
                     size_t size)
{
        int ret = -1;
        int fd = 0;
        struct snap_info snap = {0,};
        char index_path[ZR_PATH_MAX] = {0,};

        snprintf (index_path, ZR_PATH_MAX, "%s/%s/index", path, snap_name);

        fd = creat (index_path, 0400);
        if (fd < 0)
                goto out;

        if (size || start) {
                snap.start = hton64 (start);
                snap.size  = hton64 (size);

                ret = write (fd, &snap, sizeof (struct snap_info));
                if (ret < 0)
                        goto out;
        }

        ret = close (fd);
out:
        return ret;
}

int
gf_is_a_snapshot_file (xlator_t *this, const char *path)
{
        int ret = 0;
        ret = sys_lgetxattr (path, GF_SNAP_FILE_KEY, NULL, 0);
        if (ret > 0)
                return 1;

        return 0;
}

int
gf_create_fresh_snapshot (xlator_t *this, loc_t *loc, const char *path,
                          const char *snap_name, dict_t *params)
{
        int              ret                      = -1;
        char             gfid[50]                 = {0,};
        char             temp_path[ZR_PATH_MAX]   = {0,};
        struct stat      stbuf                    = {0,};
        char             snap_path[ZR_PATH_MAX]   = {0,};
        char             parent_path[ZR_PATH_MAX] = {0,};
        fd_t            *iter_fd                  = NULL;
        struct posix_fd *pfd                      = NULL;
	uint64_t         tmp_pfd                  = 0;
        int              fd_found                 = 0;

        if (!loc || !loc->inode || !path)
                goto out;

        gf_log (this->name, GF_LOG_INFO, "path: (%s) snapshot: (%s)",
                path, snap_name);

        /* NOTICE: inode wide lock */
        LOCK (&loc->inode->lock);

        if (!list_empty (&loc->inode->fd_list)) {
                list_for_each_entry (iter_fd, &loc->inode->fd_list,
                                     inode_list) {
                        gf_log (this->name, GF_LOG_DEBUG,
                                "fd is open");
                        ret = fd_ctx_get (iter_fd, this, &tmp_pfd);
                        if (ret < 0) {
                                gf_log (this->name, GF_LOG_DEBUG,
                                        "pfd not found in fd's ctx");
                                goto out;
                        }
                        pfd = (struct posix_fd *)(long)tmp_pfd;
                        gf_sync_and_free_pfd (this, pfd);
                        close (pfd->fd);
                        pfd->fd = 0;
                        fd_found = 1;
                }
        }

        /* Get the required info (like, stat and gfid) from actual file,
           and move to temp file */
        {
                ret = stat (path, &stbuf);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "stat failed");
                        goto out;
                }

                ret = sys_lgetxattr (path, GF_GFID_KEY, gfid, 48);
                if (ret <= 0)
                        gf_log (this->name, GF_LOG_ERROR, "failed to get gfid of %s", path);
                strcpy (temp_path, path);
                strcat (temp_path, GF_SNAP_TEMP_FILE_PATH);
                ret = rename (path, temp_path);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "rename failed, %s -> %s",
                                path, temp_path);
                        goto out;
                }
        }

        /* Create a directory in the actual path */
        {
                ret = mkdir (path, 0750);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "mkdir failed, %s", path);
                        goto out;
                }
                ret = chown (path, stbuf.st_uid, stbuf.st_gid);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "chown failed, %s", path);
                        goto out;
                }

                ret = sys_lsetxattr (path, GF_SNAP_FILE_KEY, "yes", 4, 0);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "failed to set snap file %d", ret);
                        goto out;
                }
        }

        /* Create a directory for the snapshot name */
        {
                strcpy (snap_path, path);
                strcat (snap_path, "/");
                strcat (snap_path, snap_name);

                ret = mkdir (snap_path, 0750);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "mkdir of %s failed", snap_path);
                        goto out;
                }

                ret = chown (snap_path, stbuf.st_uid, stbuf.st_gid);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "chown failed, %s", snap_path);
                        goto out;
                }
        }

        /* Move the actual file to '$actual_name/$snap_name/data' and make
           it read-only */
        {
                strcat (snap_path, "/data");

                ret = rename (temp_path, snap_path);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "rename of %s -> %s failed",
                                temp_path, snap_path);
                        goto out;
                }

                ret = sys_lremovexattr (snap_path, GF_GFID_KEY);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "remove of gfid on %s failed",
                                snap_path);
                        goto out;
                }

                ret = posix_gfid_set (this, snap_path, params);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "setting gfid on %s failed: %d",
                                snap_path, ret);
                }

                ret = chmod (snap_path, 0400);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "chmod to read-only: %s",
                                snap_path);
                        goto out;
                }
                gf_create_snap_index (path, snap_name, 0, stbuf.st_size);
        }

        /* Create a directory '$actual_path/HEAD' in which the delta changes
           will go to a file called 'data' */
        {
                strcpy (snap_path, path);
                strcat (snap_path, "/HEAD");

                ret = mkdir (snap_path, 0750);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "%s: mkdir failed %s",
                                snap_path, strerror (errno));
                        goto out;
                }

                ret = chown (snap_path, stbuf.st_uid, stbuf.st_gid);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "chown failed, %s", snap_path);
                        goto out;
                }

                strcat (snap_path, "/data");
                ret = mknod (snap_path, stbuf.st_mode, 0);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "%s: mknod failed %s",
                                snap_path, strerror (errno));
                        goto out;
                }

                ret = chown (snap_path, stbuf.st_uid, stbuf.st_gid);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "chown failed, %s", snap_path);
                        goto out;
                }

                ret = truncate (snap_path, stbuf.st_size);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "%s: truncate failed %s",
                                snap_path, strerror (errno));
                        goto out;
                }

                ret = sys_lsetxattr (snap_path, GF_GFID_KEY, gfid, 16, 0);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "failed to set gfid %d on %s", ret, snap_path);
                        goto out;
                }

                gf_create_snap_index (path, "HEAD", 0, 0);
        }

        /* From the 'HEAD/' directory, add a pointer for parent snapshot */
        {
                strcpy (snap_path, path);
                strcat (snap_path, "/HEAD/parent");

                strcpy (parent_path, "../");
                strcat (parent_path, snap_name);

                ret = symlink (parent_path, snap_path);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR,
                                "%s: symlink (parent) failed %s",
                                snap_path, strerror (errno));
                        goto out;
                }
        }

        /* Open the fd if fd is found */
        if (fd_found) {
                list_for_each_entry (iter_fd, &loc->inode->fd_list,
                                     inode_list) {
                        ret = fd_ctx_get (iter_fd, this, &tmp_pfd);
                        if (ret < 0) {
                                gf_log (this->name, GF_LOG_ERROR,
                                        "pfd not found in fd's ctx");
                                goto out;
                        }
                        pfd = (struct posix_fd *)(long)tmp_pfd;
                        pfd->fd = gf_snap_open_snapshot (this, pfd, path,
                                                         NULL, pfd->flags);
                        if (pfd->fd == -1) {
                                ret = -1;
                                gf_log (this->name, GF_LOG_ERROR,
                                        "failed to open the snapshot %s", path);
                                goto out;
                        }
                }
        }
out:
        UNLOCK (&loc->inode->lock);

        if (ret) {
                /* Revert back to normal file */
                gf_log (this->name, GF_LOG_ERROR, "something failed");
        }

        if (!ret) {
                /* In the inode context tell that, the snapshot is taken */
                gf_log (this->name, GF_LOG_INFO, "snapshot successful");
                gf_set_snapshot_flag_in_inode (this, loc);
        }

        return ret;
}

int
gf_create_another_snapshot (xlator_t *this, loc_t *loc, const char *path,
                            const char *snap_name, dict_t *params)
{
        int              ret                      = -1;
        struct stat      stbuf                    = {0,};
        char             temp_path[ZR_PATH_MAX]   = {0,};
        char             snap_path[ZR_PATH_MAX]   = {0,};
        char             parent_path[ZR_PATH_MAX] = {0,};
        char             gfid[50]                 = {0,};
        fd_t            *iter_fd                  = NULL;
        struct posix_fd *pfd                      = NULL;
	uint64_t         tmp_pfd                  = 0;
        int              fd_found                 = 0;

        if (!loc || !loc->inode || !path)
                goto out;

        gf_log (this->name, GF_LOG_INFO, "path: (%s) snapshot: (%s)",
                path, snap_name);

        /* NOTICE: inode wide lock */
        LOCK (&loc->inode->lock);

        if (!list_empty (&loc->inode->fd_list)) {
                list_for_each_entry (iter_fd, &loc->inode->fd_list,
                                     inode_list) {
                        gf_log (this->name, GF_LOG_DEBUG,
                                "fd is open");
                        ret = fd_ctx_get (iter_fd, this, &tmp_pfd);
                        if (ret < 0) {
                                gf_log (this->name, GF_LOG_DEBUG,
                                        "pfd not found in fd's ctx");
                                goto out;
                        }
                        pfd = (struct posix_fd *)(long)tmp_pfd;
                        gf_sync_and_free_pfd (this, pfd);
                        close (pfd->fd);
                        pfd->fd = 0;
                        fd_found = 1;
                }
        }

        /* TODO: create 'child' symlinks in the parent */
        {
                //strcpy (snap_path, "../../");
                strcpy (snap_path, "../");
                strcat (snap_path, snap_name);

                strcpy (temp_path, path);
                strcat (temp_path, "/HEAD/parent/child");

/*
                ret = mkdir (temp_path, 0777);
                if (ret && (errno != EEXIST)) {
                        gf_log ("", GF_LOG_ERROR, "mkdir %s", temp_path);
                        goto out;
                }
                strcat (temp_path, "/");
                strcat (temp_path, snap_name);
*/

                ret = symlink (snap_path, temp_path);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "symlink %s -> %s",
                                snap_path, temp_path);
                        goto out;
                }
        }

        /* rename the delta present in 'HEAD/' to '$snap_name/' */
        {
                strcpy (snap_path, path);
                strcat (snap_path, "/");
                strcat (snap_path, snap_name);

                strcpy (temp_path, path);
                strcat (temp_path, "/HEAD");

                ret = rename (temp_path, snap_path);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "rename %s -> %s",
                                temp_path, snap_path);
                        goto out;
                }
                strcat (snap_path, "/data");
                ret = stat (snap_path, &stbuf);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "stat %s", snap_path);
                        goto out;
                }
                ret = sys_lgetxattr (snap_path, GF_GFID_KEY, gfid, 48);
                if (ret <= 0)
                        gf_log (this->name, GF_LOG_ERROR,
                                "failed to get gfid of %s", snap_path);

                ret = sys_lremovexattr (snap_path, GF_GFID_KEY);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "remove of gfid on %s failed",
                                snap_path);
                        goto out;
                }

                ret = posix_gfid_set (this, snap_path, params);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "setting gfid on %s failed: %d",
                                snap_path, ret);
                }

                ret = chmod (snap_path, 0400);
                if (ret)
                        goto out;
        }

        /* Create 'HEAD/' again to keep delta */
        {
                ret = mkdir (temp_path, 0750);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "mkdir %s", temp_path);
                        goto out;
                }
                ret = chown (temp_path, stbuf.st_uid, stbuf.st_gid);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "chown failed, %s", temp_path);
                        goto out;
                }

                strcat (temp_path, "/data");
                ret = mknod (temp_path, stbuf.st_mode, 0);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "mknod %s", temp_path);
                        goto out;
                }
                ret = chown (temp_path, stbuf.st_uid, stbuf.st_gid);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "chown failed, %s", temp_path);
                        goto out;
                }

                ret = sys_lsetxattr (temp_path, GF_GFID_KEY, gfid, 16, 0);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "failed to set gfid %d on %s", ret, temp_path);
                        goto out;
                }

                ret = truncate (temp_path, stbuf.st_size);
                if (ret)
                        goto out;

                gf_create_snap_index (path, "HEAD", 0, 0);
        }

        /* Create the link to parent snapshot */
        {
                strcpy (snap_path, path);
                strcat (snap_path, "/HEAD/parent");

                strcpy (parent_path, "../");
                strcat (parent_path, snap_name);

                ret = symlink (parent_path, snap_path);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "symlink %s -> %s",
                                parent_path, snap_path);
                        goto out;
                }
        }

        /* Open the fd if fd is found */
        if (fd_found) {
                list_for_each_entry (iter_fd, &loc->inode->fd_list,
                                     inode_list) {
                        ret = fd_ctx_get (iter_fd, this, &tmp_pfd);
                        if (ret < 0) {
                                gf_log (this->name, GF_LOG_DEBUG,
                                        "pfd not found in fd's ctx");
                                goto out;
                        }
                        pfd = (struct posix_fd *)(long)tmp_pfd;
                        pfd->fd = gf_snap_open_snapshot (this, pfd, path,
                                                         NULL, pfd->flags);
                        if (pfd->fd == -1) {
                                ret = -1;
                                gf_log (this->name, GF_LOG_ERROR,
                                        "failed to open the snapshot %s", path);
                                goto out;
                        }
                }
        }
out:
        UNLOCK (&loc->inode->lock);
        if (ret) {
                /* Revert back to normal file */
                gf_log (this->name, GF_LOG_ERROR, "something failed");
        }
        if (!ret)
                gf_log (this->name, GF_LOG_INFO, "snapshot successful");

        return ret;
}


int
gf_check_and_change_snap_entry (xlator_t *this, const char *path,
                                struct iatt *stbuf, dict_t *params)
{
        int  ret                    = 0;
        char snap_path[ZR_PATH_MAX] = {0,};

        if (!IA_ISDIR (stbuf->ia_type))
                goto out;

        if (!gf_is_a_snapshot_file (this, path))
                goto out;

        strcpy (snap_path, path);
        strcat (snap_path, "/HEAD/data");

        posix_gfid_set (this, snap_path, params);
        ret = posix_lstat_with_gfid (this, snap_path, stbuf);
        /* Bit hacky, so the posix_lookup() behaves properly to set
           the inode ctx */
        if (!ret)
                ret = 1;
        if (ret == -1)
                ret = 0;

out:
        return ret;
}

int
gf_snap_read_index_file (const char *index_path, int32_t open_flag,
                         struct snap_fds *snap)
{
        struct snap_info *trav     = NULL;
        struct stat       stbuf    = {0,};
        int               ret      = -1;
        size_t            len      = 0;
        size_t            i        = 0;
        int               index_fd = 0;
        uint64_t          start    = 0;
        uint64_t          size     = 0;
        int               calloc_len = 0;

        ret = stat (index_path, &stbuf);
        if (ret)
                goto out;

        len = stbuf.st_size / sizeof (struct snap_info);

        /* Open */
        calloc_len = len;
        if (open_flag != O_RDONLY) {
                calloc_len = len + 100000;
        }

        if (calloc_len == 0) {
                ret = 0;
                snap->idx_len = 0;
                snap->snap_idx = NULL;
                goto out;
        }

        index_fd = open (index_path, open_flag);
        if (index_fd < 0) {
                ret = -1;
                goto out;
        }

        trav = GF_CALLOC (sizeof (struct snap_info), calloc_len,
                          gf_posix_mt_snap_idx_t);

        /* read */
        ret = read (index_fd, trav, stbuf.st_size);
        if (ret < 0)
                goto out;

        for (i = 0; i < len; i++) {
                start = ntoh64 (trav[i].start);
                size  = ntoh64 (trav[i].size);

                trav[i].size = size;
                trav[i].start = start;
        }

        snap->snap_idx = trav;
        snap->idx_fd   = index_fd;
        snap->idx_len  = len;

        if (open_flag == O_RDONLY) {
                snap->idx_fd = 0;
                close (index_fd);
        }

        ret = 0;
out:
        return ret;
}


int
gf_snap_open_snapshot (xlator_t *this, struct posix_fd *pfd, const char *path,
                       const char *snap_name, int32_t flags)
{
        struct stat stbuf                 = {0,};
        int         _fd                   = -1;
        int         ret                   = -1;
        int         data_fd               = -1;
        int         idx                   = 0;

        char data_file_path[ZR_PATH_MAX]  = {0,};
        char index_file_path[ZR_PATH_MAX] = {0,};
        char parent_path[ZR_PATH_MAX]     = {0,};

        if (snap_name && ((O_ACCMODE & flags) != O_RDONLY)) {
                gf_log (this->name, GF_LOG_ERROR,
                        "only read only access on snapshots");
                errno = EPERM;
                ret = -1;
                goto out;
        }

        if (!snap_name)
                snap_name = "HEAD";

        strcpy (data_file_path, path);
        strcat (data_file_path, "/");
        strcat (data_file_path, snap_name);
        strcat (data_file_path, "/data");

        strcpy (index_file_path, path);
        strcat (index_file_path, "/");
        strcat (index_file_path, snap_name);
        strcat (index_file_path, "/index");

        data_fd = open (data_file_path, flags);
        if (data_fd == -1) {
                gf_log_callingfn (this->name, GF_LOG_ERROR,
                                  "failed to open %s", data_file_path);
                goto out;
        }

        _fd = data_fd;
        pfd->snap_fd[0].fd = data_fd;
        ret = gf_snap_read_index_file (index_file_path, O_RDWR, &pfd->snap_fd[0]);
        if (ret) {
                gf_log (this->name, GF_LOG_ERROR,
                        "failed to open %s", index_file_path);
                goto out;
        }
        strcpy (parent_path, path);
        strcat (parent_path, "/");
        strcat (parent_path, snap_name);
        strcat (parent_path, "/parent");

        idx = 1;
        ret = stat (parent_path, &stbuf);
        if (ret)
                goto done;

        while (1) {
                strcpy (data_file_path, parent_path);
                strcat (data_file_path, "/data");

                strcpy (index_file_path, parent_path);
                strcat (index_file_path, "/index");

                pfd->snap_fd[idx].fd = open (data_file_path, O_RDONLY);
                if (pfd->snap_fd[idx].fd == -1) {
                        ret = -1;
                        gf_log (this->name, GF_LOG_ERROR, "failed to open %s",
                                data_file_path);
                        goto out;
                }

                ret = gf_snap_read_index_file (index_file_path, O_RDONLY,
                                               &pfd->snap_fd[idx]);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "failed to open %s",
                                index_file_path);
                        goto out;
                }

                idx++;

                /*TODO: fix how we did our 'parent' link */
                strcat (parent_path, "/parent");

                ret = stat (parent_path, &stbuf);
                if (ret)
                        break;

        }
done:
        pfd->fd_count = idx;
        pfd->snapshot = 1;
        ret = 0;
out:
        if (ret) {
                /* Free up everything properly */
        }
        if (!ret) {
                /* Send the proper 'fd' back */
                ret = _fd;
        }

        return ret;
}

static size_t
get_next_recent_block (struct snap_fds *snapfd, off_t trav_off,
                       size_t trav_size, int fdidx, int arrayidx)
{
        int    k = 0;
        int    m = 0;
        size_t size1 = trav_size;

        for (k = 0; k < fdidx; k++) {
                for (m = 0; m < snapfd[k].idx_len; m++) {
                        if (m == arrayidx)
                                continue;
                        if ((snapfd[k].snap_idx[m].start > trav_off) &&
                            (snapfd[k].snap_idx[m].start < (trav_off + trav_size))) {
                                if (size1 > (snapfd[k].snap_idx[m].start -
                                             trav_off)) {
                                        size1 = (snapfd[k].snap_idx[m].start -
                                                 trav_off);
                                }
                        }
                }
        }
        return size1;
}

static int32_t
get_read_block_size (struct snap_fds *snapfd, int32_t fd_count, off_t trav_off,
                     size_t trav_size, size_t *return_size, int *_fd)
{
        size_t                 tmp_size   = 0;
        size_t                 tmp_size1  = 0;
        int32_t                i = 0;
        int32_t                j = 0;

        /* search for the proper block of the data */

        for (i = 0; i < fd_count; i++) {
                /* If the region is not in this 'fd', failover to next fd */
                for (j = 0; j < snapfd[i].idx_len; j++) {
                        if (!((snapfd[i].snap_idx[j].start <= trav_off) &&
                              ((snapfd[i].snap_idx[j].start +
                                snapfd[i].snap_idx[j].size) > trav_off))) {
                                continue;
                        }

                        tmp_size = (snapfd[i].snap_idx[j].size -
                                    (trav_off - snapfd[i].snap_idx[j].start));

                        /* get the next recent block */
                        tmp_size1 = get_next_recent_block (snapfd, trav_off,
                                                           trav_size, i, j);

                        if ((tmp_size1 != 0) && (tmp_size > tmp_size1)) {
                                tmp_size = tmp_size1;
                                tmp_size1 = 0;
                        }

                        if (tmp_size > trav_size)
                                tmp_size = trav_size;

                        *return_size = tmp_size;
                        *_fd = snapfd[i].fd;

                        return 0;
                }
        }

        return -1;
}

/* FIXME: I gotta tell you... this is one hell of a complex code :O */

int
gf_snap_readv (call_frame_t *frame, xlator_t *this, struct posix_fd *pfd,
               off_t offset, size_t size)
{
        int32_t                op_ret     = -1;
        int32_t                op_errno   = 0;
        int                    _fd        = -1;
        int                    count      = 0;
        int                    eob_flag   = 1; /* end of block */
        off_t                  trav_off   = 0;
        off_t                  tmp_offset = 0;
        size_t                 tmp_size   = 0;
        size_t                 trav_size  = 0;
        size_t                 total_read = 0;
        struct posix_private * priv       = NULL;
        struct iobuf         * iobuf      = NULL;
        struct iobref        * iobref     = NULL;
        struct iatt            stbuf      = {0,};
        struct iovec           vec        = {0,};

        priv = this->private;
        VALIDATE_OR_GOTO (priv, out);

        op_ret = posix_fstat_with_gfid (this, pfd->fd, &stbuf);
        if (op_ret == -1) {
                op_errno = errno;
                gf_log (this->name, GF_LOG_ERROR,
                        "fstat failed on fd=%d: %s", pfd->fd,
                        strerror (op_errno));
                goto out;
        }

        iobref = iobref_new ();
        if (!iobref) {
                gf_log (this->name, GF_LOG_ERROR,
                        "Out of memory.");
                goto out;
        }

        iobuf = iobuf_get (this->ctx->iobuf_pool);
        if (!iobuf) {
                gf_log (this->name, GF_LOG_ERROR,
                        "Out of memory.");
                goto out;
        }

        trav_off = offset;
        trav_size = size;

        if (size > (stbuf.ia_size - offset))
                trav_size = stbuf.ia_size - offset;

        do {
                /* read block calculation is bit tricky */
                op_ret = get_read_block_size (pfd->snap_fd, pfd->fd_count,
                                              trav_off, trav_size, &tmp_size,
                                              &_fd);
                tmp_offset = trav_off;
                if (tmp_size <= trav_size)
                        eob_flag = 0;

                if (tmp_offset >= stbuf.ia_size) {
                        gf_log (this->name, GF_LOG_DEBUG,
                                "we are at the last block, send EOF");
                        op_ret = 0;
                        /* Hack to notify higher layers of EOF. */
                        op_errno = ENOENT;
                        goto done;
                }
                op_ret = lseek (_fd, tmp_offset, SEEK_SET);
                if (op_ret == -1) {
                        op_errno = errno;
                        gf_log (this->name, GF_LOG_ERROR,
                                "lseek(%"PRId64") failed: %s",
                                tmp_offset, strerror (op_errno));
                        goto out;
                }
                op_ret = read (_fd, iobuf->ptr + total_read, tmp_size);
                if (op_ret == -1) {
                        op_errno = errno;
                        gf_log (this->name, GF_LOG_ERROR,
                                "read failed on fd=%p: %s", pfd,
                                strerror (op_errno));
                        goto out;
                }

                trav_off   += op_ret;
                trav_size  -= op_ret;
                total_read += op_ret;

                LOCK (&priv->lock);
                {
                        priv->read_value    += op_ret;
                        //priv->interval_read += op_ret;
                }
                UNLOCK (&priv->lock);

                /* Hack to notify higher layers of EOF. */
                if (stbuf.ia_size == 0)
                        op_errno = ENOENT;
                else if ((tmp_offset + tmp_size) == stbuf.ia_size)
                        op_errno = ENOENT;

                if ((trav_size == 0) || (op_ret < tmp_size) ||
                    ((offset + total_read) >= stbuf.ia_size)) {
                        vec.iov_base = iobuf->ptr;
                        vec.iov_len  = total_read;
                        count++;

                        iobref_add (iobref, iobuf);

                        goto done;
                }
        } while ((!eob_flag) && (trav_size > 0));

        if (eob_flag) {
                gf_log (this->name, GF_LOG_CRITICAL,
                        "something very wrong.. :O");
                /* Just for completion */
                _fd = pfd->fd;
                op_ret = lseek (_fd, offset, SEEK_SET);
                if (op_ret == -1) {
                        op_errno = errno;
                        gf_log (this->name, GF_LOG_ERROR,
                                "lseek(%"PRId64") failed: %s",
                                tmp_offset, strerror (op_errno));
                        goto out;
                }

                op_ret = read (_fd, iobuf->ptr, size);
                if (op_ret == -1) {
                        op_errno = errno;
                        gf_log (this->name, GF_LOG_ERROR,
                                "read failed on fd=%p: %s", pfd,
                                strerror (op_errno));
                        goto out;
                }

                vec.iov_base = iobuf->ptr;
                vec.iov_len  = op_ret;
                total_read = op_ret;
                count++;

                iobref_add (iobref, iobuf);
        }

done:
        /*
         *  readv successful, and we need to get the stat of the file
         *  we read from
         */

        op_ret = posix_fstat_with_gfid (this, pfd->fd, &stbuf);
        if (op_ret == -1) {
                op_errno = errno;
                gf_log (this->name, GF_LOG_ERROR,
                        "fstat failed on fd=%p: %s", pfd,
                        strerror (op_errno));
                goto out;
        }

        op_ret = total_read;
out:
        STACK_UNWIND_STRICT (readv, frame, op_ret, op_errno,
                             &vec, count, &stbuf, iobref);

        if (iobref)
                iobref_unref (iobref);
        if (iobuf)
                iobuf_unref (iobuf);

        return 0;
}

int
gf_snap_truncate_index (xlator_t *this, struct snap_fds *snap, off_t offset)
{
        int idx = 0;

        /* if offset is 0, that means, we have to start fresh with index */
        if (!snap)
                return 0;

        if (!offset) {
                snap->idx_len = 0;
                if (snap->snap_idx) {
                        snap->snap_idx[0].start = 0;
                        snap->snap_idx[0].size = 0;
                }
                goto out;
        }

        if (!snap->snap_idx) {
                snap->idx_len = 0;
                goto out;
        }

        /* Remove entries which starts after required offset */
        for (idx = 0; idx < snap->idx_len; idx++) {
                if (snap->snap_idx[idx].start >= offset) {
                        snap->idx_len--;
                        snap->snap_idx[idx].start =
                                snap->snap_idx[snap->idx_len].start;
                        snap->snap_idx[idx].size =
                                snap->snap_idx[snap->idx_len].size;
                        idx--; // check the currently put value again
                }
        }

        /* if there is a block which spans bigger than offset, make it proper */
        for (idx = 0; idx < snap->idx_len; idx++) {
                if (offset > (snap->snap_idx[idx].start +
                              snap->snap_idx[idx].size)) {
                        snap->snap_idx[idx].size = (offset -
                                                    snap->snap_idx[idx].start);
                }
        }

        if (snap->idx_len == 0) {
                snap->idx_len = 1;
                snap->snap_idx[0].start = offset;
                snap->snap_idx[0].size = 0;
        }

out:
        gf_sync_snap_info_file (snap);

        return 0;
}

int
gf_snap_writev_update_index (xlator_t *this, struct snap_fds *snap,
                             off_t offset, int32_t size)
{
        int temp_size = 0;
        int max_idx   = 0;
        int idx       = 0;

        max_idx = snap->idx_len;

        for (idx = 0; idx < max_idx; idx++) {
                /* Extending the previously written region */
                /*
                  |--------|
                           |--------|
                */
                if ((snap->snap_idx[idx].start +
                     snap->snap_idx[idx].size) == offset) {
                        snap->snap_idx[idx].size += size;
                        goto out;
                }

                /* Extending just the size */
                /*
                |---------|
                |-------------------|
                */
                if (snap->snap_idx[idx].start == offset) {
                        /* Same block gets overwritten with bigger data */
                        temp_size = (size - snap->snap_idx[idx].size);
                        if (temp_size > 0)
                                snap->snap_idx[idx].size = size;

                        goto out;
                }
                /* some of the write falls inside already
                   existing write.. */
                /*
                |----------|
                      |--------|
                */
                if ((snap->snap_idx[idx].start < offset) &&
                    ((snap->snap_idx[idx].start +
                      snap->snap_idx[idx].size) > offset)) {
                        /* This write falls in the already written
                         * region */
                        //gf_log ("", 1, "write-overlap");
                        temp_size = (size -
                                     ((snap->snap_idx[idx].start +
                                       snap->snap_idx[idx].size) -
                                      offset));
                        if (temp_size > 0)
                                snap->snap_idx[idx].size += temp_size;

                        goto out;
                }
        }

        snap->snap_idx[max_idx].start = offset;
        snap->snap_idx[max_idx].size  = size;
        snap->idx_len++;
        //if (!(snap->idx_len % 42))
        //        gf_sync_snap_info_file (pfd);

out:
        return 0;
}


int
gf_check_if_snap_path (xlator_t *this, loc_t *loc, char *path, struct iatt *buf,
                       char **pathdup, dict_t *params)
{
        int                   ret             = -1;
        char *                list_snap_str   = NULL;
        char                  temp_path[4096] = {0,};
        char                  list_sym[48]    = {0,};
        struct posix_private *priv            = NULL;

        priv = this->private;

        if (!strstr (loc->path, priv->snap_cmd_symbol)) {
                goto out;
        }

        strcpy (list_sym, priv->snap_cmd_symbol);
        strcat (list_sym, "list");

        list_snap_str = strstr (path, list_sym);
        if (list_snap_str) {
                /* Need to show the '@list' as directory */
                /* and entries inside it as regular files */
                memcpy (temp_path, path, (list_snap_str - path));
                path = temp_path;
                if (strstr (loc->name, list_sym))
                        goto done;

                /* Lookup on snapshots */
                strcat (temp_path, "/");
                strcat (temp_path, loc->name);
                /* Do not allow lookup on 'HEAD' */
                if (strcmp (loc->name, "HEAD") == 0)
                        goto out;

                if (pathdup)
                        *pathdup = gf_strdup (temp_path);
                strcat (temp_path, "/data");
                path = temp_path;

                goto done;
        }

        /* Show actual snapshot files as files */
        list_snap_str = strstr (path, priv->snap_cmd_symbol);
        if (list_snap_str) {
                /* 'HEAD' not looked up */
                if (strcmp (list_snap_str+1, "HEAD") == 0)
                        goto out;

                if (pathdup)
                        *pathdup = gf_strdup (path);
                memcpy (temp_path, path, (list_snap_str - path));
                strcat (temp_path, "/");
                strcat (temp_path, (list_snap_str + strlen (priv->snap_cmd_symbol)));
                strcat (temp_path, "/data");
                path = temp_path;
        }

done:
        posix_gfid_set (this, path, params);
        ret = posix_lstat_with_gfid (this, path, buf);
        if (ret == -1)
                goto out;

        if (IA_ISDIR (buf->ia_type) && !strstr (loc->name, list_sym)) {
                /* A snapshot is a file, if not, error out */
                ret = -1;
                goto out;
        }

        if (!IA_ISDIR (buf->ia_type) && strstr (loc->name, list_sym)) {
                /* '@'list is a directory, if not, error out */
                ret = -1;
                goto out;
        }

        /* HACKKKYYYY: some minor alteration to stbuf -bulde */
        //buf->ia_gfid[15]++;
        //buf->ia_ino++;

        ret = 0;
out:
        return ret;
}

int
gf_snap_create_clone (xlator_t *this, const char *oldpath,
                      const char *snap_name, const char *newpath)
{
        int          ret         = -1;
        struct stat  stbuf       = {0,};
        char        *parent_snap = NULL;
        char         src_path[ZR_PATH_MAX];
        char         dst_path[ZR_PATH_MAX];
        char         parent_snap_str[ZR_PATH_MAX];
        char         child_snap_str[ZR_PATH_MAX];

        if (!snap_name) {
                errno = ENOENT;
                goto out;
        }

        /* Mkdir new path */
        {
                ret = stat (oldpath, &stbuf);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "%s: stat failed %s",
                                oldpath, strerror (errno));
                        goto out;
                }

                ret = mkdir (newpath, 0750);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "%s: mkdir failed %s",
                                newpath, strerror (errno));
                        goto out;
                }

                ret = chown (newpath, stbuf.st_uid, stbuf.st_gid);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "%s: chown failed %s",
                                newpath, strerror (errno));
                        goto out;
                }

                ret = chmod (newpath, stbuf.st_mode);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "%s: chmod failed %s",
                                newpath, strerror (errno));
                        goto out;
                }
        }

        /* Set xattr saying its a snapshot */
        {
                ret = sys_lsetxattr (newpath, GF_SNAP_FILE_KEY, "yes", 4, 0);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR,
                                "%s: setting 'snap' key failed %s",
                                newpath, strerror (errno));
                        goto out;
                }
        }

        /* Replicate the setup which is in oldpath */
        {
                snprintf (src_path, ZR_PATH_MAX, "%s/%s", oldpath, snap_name);
                snprintf (dst_path, ZR_PATH_MAX, "%s/%s", newpath, snap_name);

                ret = mkdir (dst_path, 0750);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "%s: mkdir failed %s",
                                dst_path, strerror (errno));
                        goto out;
                }
                /* create new files as 'hardlinks' */
                strcat (src_path, "/data");
                strcat (dst_path, "/data");

                ret = stat (src_path, &stbuf);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "%s: stat failed %s",
                                src_path, strerror (errno));
                        goto out;
                }

                ret = link (src_path, dst_path);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR,
                                "%s -> %s: link failed %s",
                                src_path, dst_path, strerror (errno));
                        goto out;
                }

                snprintf (src_path, ZR_PATH_MAX, "%s/%s/index", oldpath, snap_name);
                snprintf (dst_path, ZR_PATH_MAX, "%s/%s/index", newpath, snap_name);

                ret = link (src_path, dst_path);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR,
                                "%s -> %s: link failed %s",
                                src_path, dst_path, strerror (errno));
                        goto out;
                }
        }

        /* Create 'HEAD/' in newpath */
        {
                snprintf (dst_path, ZR_PATH_MAX, "%s/HEAD", newpath);
                ret = mkdir (dst_path, 0750);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "%s: mkdir failed %s",
                                dst_path, strerror (errno));
                        goto out;
                }

                snprintf (dst_path, ZR_PATH_MAX, "%s/HEAD/data", newpath);
                ret = mknod (dst_path, stbuf.st_mode, 0);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "%s: mknod failed %s",
                                dst_path, strerror (errno));
                        goto out;
                }

                ret = chown (dst_path, stbuf.st_uid, stbuf.st_gid);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "%s: chown failed %s",
                                dst_path, strerror (errno));
                        goto out;
                }

                ret = truncate (dst_path, stbuf.st_size);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "%s: truncate failed %s",
                                dst_path, strerror (errno));
                        goto out;
                }

                gf_create_snap_index (newpath, "HEAD", 0, 0);

                snprintf (src_path, ZR_PATH_MAX, "%s/HEAD/parent", newpath);
                snprintf (dst_path, ZR_PATH_MAX, "../%s", snap_name);
                ret = symlink (dst_path, src_path);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "%s: symlink failed %s",
                                dst_path, strerror (errno));
                        goto out;
                }
        }

        /* Build all the parent snaps of 'snap_name' */
        parent_snap = (char *)snap_name;
        while (1) {
                snprintf (src_path, ZR_PATH_MAX, "%s/%s/parent", oldpath, parent_snap);
                snprintf (dst_path, ZR_PATH_MAX, "%s/%s/parent", newpath, parent_snap);

                ret = readlink (src_path, parent_snap_str, ZR_PATH_MAX);
                if (ret < 0)
                        break;

                ret = symlink (parent_snap_str, dst_path);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "%s: symlink failed %s",
                                dst_path, strerror (errno));
                        goto out;
                }

                parent_snap = strrchr (parent_snap_str, '/');

                snprintf (src_path, ZR_PATH_MAX, "%s/%s", oldpath, parent_snap);
                snprintf (dst_path, ZR_PATH_MAX, "%s/%s", newpath, parent_snap);

                ret = mkdir (dst_path, 0750);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "%s: mkdir failed %s",
                                dst_path, strerror (errno));
                        goto out;
                }

                /* create new files as 'hardlinks' */
                strcat (src_path, "/data");
                strcat (dst_path, "/data");

                ret = link (src_path, dst_path);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "link %s -> %s: %s",
                                src_path, dst_path, strerror (errno));
                        goto out;
                }

                snprintf (src_path, ZR_PATH_MAX, "%s/%s/index",
                          oldpath, parent_snap);
                snprintf (dst_path, ZR_PATH_MAX, "%s/%s/index",
                          newpath, parent_snap);

                ret = link (src_path, dst_path);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "link %s -> %s: %s",
                                src_path, dst_path, strerror (errno));
                        goto out;
                }

                snprintf (src_path, ZR_PATH_MAX, "%s/%s/child", oldpath, parent_snap);
                snprintf (dst_path, ZR_PATH_MAX, "%s/%s/child", newpath, parent_snap);

                ret = readlink (src_path, child_snap_str, ZR_PATH_MAX);
                if (ret > 0) {
                        ret = symlink (child_snap_str, dst_path);
                        if (ret) {
                                gf_log (this->name, GF_LOG_ERROR,
                                        "%s: failed to create 'child' link %s",
                                        dst_path, strerror (errno));
                                goto out;
                        }
                }
        }

        gf_log (this->name, GF_LOG_NORMAL, "cloning of '%s' -> '%s' successful",
                oldpath, newpath);

        ret = -1;
        /* set the errno properly */
        errno = EBUSY;

out:
        return ret;
}

int
gf_snap_rename (xlator_t *this, const char *path, const char *from,
                const char *to)
{
        int         ret = -1;
        char        src_path[ZR_PATH_MAX];
        char        dst_path[ZR_PATH_MAX];
        char        temp_str[ZR_PATH_MAX];
        char        temp_path[ZR_PATH_MAX];
        struct stat buf = {0,};

        if (!this || !path || !from || !to) {
                errno = EINVAL;
                goto out;
        }

        if ((strcmp (from, "list") == 0) ||
            (strcmp (from, "HEAD") == 0) ||
            (strcmp (to, "list") == 0) ||
            (strcmp (to, "HEAD") == 0)) {
                /* Reservered snapshot names */
                errno = EPERM;
                goto out;
        }

        snprintf (src_path, ZR_PATH_MAX, "%s/%s", path, from);
        snprintf (dst_path, ZR_PATH_MAX, "%s/%s", path, to);

        ret = stat (dst_path, &buf);
        if (!ret) {
                /* Don't overwrite existing snapshot */
                errno = EEXIST;
                goto out;
        }

        /* TODO: hold lock */

        /* Update 'child' link from parent */
        snprintf (temp_str, ZR_PATH_MAX, "../%s", to);
        snprintf (temp_path, ZR_PATH_MAX, "%s/%s/parent", path, from);
        ret = stat (temp_path, &buf);
        if (!ret) {
                snprintf (temp_path, ZR_PATH_MAX, "%s/%s/parent/child", path, from);
                ret = stat (temp_path, &buf);
                if (!ret) {
                        ret = unlink (temp_path);
                        if (ret) {
                                gf_log (this->name, GF_LOG_ERROR,
                                        "failed to unlink %s", temp_path);
                                goto out;
                        }
                        ret = symlink (temp_str, temp_path);
                        if (ret) {
                                gf_log (this->name, GF_LOG_ERROR,
                                        "failed to symlink %s -> %s",
                                        temp_path, temp_str);
                                goto out;
                        }
                }
        }

        /* Update 'parent' link from child */
        snprintf (temp_path, ZR_PATH_MAX, "%s/%s/child", path, from);
        ret = stat (temp_path, &buf);
        if (ret) {
                snprintf (temp_path, ZR_PATH_MAX, "%s/HEAD/parent", path);
        } else {
                snprintf (temp_path, ZR_PATH_MAX, "%s/%s/child/parent", path, from);
        }
        ret = stat (temp_path, &buf);
        if (!ret) {
                ret = unlink (temp_path);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR,
                                "failed to unlink %s", temp_path);
                        goto out;
                }
                ret = symlink (temp_str, temp_path);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR,
                                "failed to symlink %s -> %s",
                                temp_path, temp_str);
                        goto out;
                }
        }


        /* Do actual rename */
        ret = rename (src_path, dst_path);
        if (ret) {
                gf_log (this->name, GF_LOG_ERROR,
                        "rename of %s -> %s failed", src_path, dst_path);
                goto out;
        }

        /* All done */
        ret = 0;
out:
        /* Do Unlink */

        /* If there is a error, if reverting is required, revert the link back */

        return ret;
}

/* Delete section */
int
gf_get_the_block_to_be_written (struct snap_fds *current, struct snap_fds *child,
                                off_t *offset, size_t *size, int *last_offset)
{
        struct snap_info *a = NULL;
        struct snap_info *b = NULL;

        static int idx = -1;
        static int idx2 = -1;

        /* Take one index at a time */
        if (idx == -1) {
                idx  = current->idx_len - 1;
                idx2 = child->idx_len - 1;
        }

        a = current->snap_idx;
        b = child->snap_idx;

        while ((idx >= 0) && (idx2 >= 0)) {

                /* current block's offset higher than child block (all sorted) */
                if (a[idx].start > (b[idx2].start + b[idx2].size)) {
                        *offset = a[idx].start;
                        *size = a[idx].size;
                        break;
                }
                if ((a[idx].start >= b[idx2].start) &&
                    ((a[idx].size + a[idx].start) <=
                     (b[idx2].size + b[idx2].start))) {
                        idx--;
                        continue;
                }
                if ((a[idx].start >= b[idx2].start) &&
                    ((a[idx].size + a[idx].start) >
                     (b[idx2].size + b[idx2].start))) {
                        *offset = (b[idx2].start + b[idx2].size);
                        *size = (a[idx].start + a[idx].size) - *offset;
                        idx--;
                        break;
                }

                /* offset lesser than child's */
                if ((a[idx].start + a[idx].size) >
                    (b[idx2].start + b[idx2].size)) {
                        /* current block is bigger than child's block */
                        *offset = (b[idx2].start + b[idx2].size);
                        *size = (a[idx].start + a[idx].size) - *offset;
                        a[idx].size -= *size;
                        break;
                }

                if ((idx2) && (a[idx].start >
                               (b[idx2 - 1].start + b[idx2 - 1].size))) {
                        *offset = a[idx].start;
                        if ((a[idx].start + a[idx].size) >
                            b[idx2].start) {
                                *size = b[idx2].start - a[idx].start;
                                a[idx].size -= *size;
                        } else
                                *size = a[idx].size;

                        idx--;
                        break;
                }

                /* TODO: Think of some more cases, like child is truncated.. etc */

                idx2--;
        }

        if ((idx2 < 0) && (idx >= 0)) {
                if (!(*offset || *size)) {
                        *offset = a[idx].start;
                        *size = a[idx].size;
                        idx--;
                }
        }
        if ((idx == -1) && (idx2 == -1)) {
                *last_offset = 1;
        }

        return 0;
}


static int
__gf_snap_delete_snapshot (xlator_t *this, const char *path,
                           const char *snap_name, const char *child_name,
                           int link)
{
        char snap_path[ZR_PATH_MAX];
        char child_path[ZR_PATH_MAX];
        char snap_idx_path[ZR_PATH_MAX];
        char child_idx_path[ZR_PATH_MAX];
        char link_str[ZR_PATH_MAX];

        struct stat buf = {0,};
        struct snap_fds child_snap = {0,};
        struct snap_fds current_snap = {0,};

        int ret = -1;
        int snap_data_fd = 0;
        int child_data_fd = 0;
        off_t offset = 0;
        size_t size = 0;
        int last_offset = 0;
        int64_t tmp_size = 0;
        off_t tmp_offset = 0;
        char buffer[GF_UNIT_MB];

        gf_log (this->name, GF_LOG_DEBUG,
                "path:%s snap-name:%s child-name:%s is_linked:%d",
                path, snap_name, child_name, link);

        snprintf (child_path, ZR_PATH_MAX, "%s/%s/data", path, child_name);
        snprintf (snap_path, ZR_PATH_MAX, "%s/%s/data", path, snap_name);

        snprintf (child_idx_path, ZR_PATH_MAX, "%s/%s/index", path, child_name);
        snprintf (snap_idx_path, ZR_PATH_MAX, "%s/%s/index", path, snap_name);

        ret = stat (child_path, &buf);
        if (ret) {
                gf_log (this->name, GF_LOG_ERROR, "stat failed on %s: %s",
                        child_path, strerror (errno));
                goto out;
        }

        snap_data_fd = open (snap_path, O_RDWR);
        if (snap_data_fd < 0) {
                gf_log (this->name, GF_LOG_ERROR, "open failed %s: %s",
                        snap_path, strerror (errno));
                goto out;
        }
        child_data_fd = open (child_path, O_RDWR);
        if (child_data_fd < 0) {
                gf_log (this->name, GF_LOG_ERROR, "open failed %s: %s",
                        child_path, strerror (errno));
                goto out;
        }
        ret = gf_snap_read_index_file (child_idx_path, O_RDWR, &child_snap);
        if (ret) {
                gf_log (this->name, GF_LOG_ERROR, "open failed %s",
                        child_idx_path);
                goto out;
        }
        ret = gf_snap_read_index_file (snap_idx_path, O_RDONLY, &current_snap);
        if (ret) {
                gf_log (this->name, GF_LOG_ERROR, "open failed %s",
                        snap_idx_path);
                goto out;
        }

        do {
                gf_get_the_block_to_be_written (&current_snap, &child_snap,
                                                &offset, &size, &last_offset);
                tmp_offset = 0;
                for (tmp_size = (size > GF_UNIT_MB) ? GF_UNIT_MB : size;
                     tmp_size > 0;
                     tmp_size -= GF_UNIT_MB) {
                        ret = lseek (snap_data_fd, offset + tmp_offset, 0);
                        if (ret < 0) {
                                gf_log (this->name, GF_LOG_ERROR,
                                        "lseek failed %s: %s",
                                        snap_path, strerror (errno));
                                goto out;
                        }
                        ret = read (snap_data_fd, buffer, tmp_size);
                        if (ret < 0) {
                                gf_log (this->name, GF_LOG_ERROR,
                                        "read failed %s: %s", snap_path,
                                        strerror (errno));
                                goto out;
                        }
                        ret = lseek (child_data_fd, offset + tmp_offset, 0);
                        if (ret < 0) {
                                gf_log (this->name, GF_LOG_ERROR,
                                        "lseek failed %s: %s",
                                        child_path, strerror (errno));
                                goto out;
                        }
                        ret = write (child_data_fd, buffer, tmp_size);
                        if (ret < 0) {
                                gf_log (this->name, GF_LOG_ERROR,
                                        "write failed %s: %s",
                                        child_path, strerror (errno));
                                goto out;
                        }
                        gf_snap_writev_update_index (THIS, &child_snap,
                                                     offset + tmp_offset,
                                                     tmp_size);

                        tmp_offset += tmp_size;
                }
                if (!link) {
                        ret = ftruncate (snap_data_fd, offset);
                        if (ret)
                                gf_log (this->name, GF_LOG_ERROR, "truncate failed %s: %s",
                                        snap_path, strerror (errno));
                }
        } while (!last_offset);

        gf_sync_snap_info_file (&child_snap);
        GF_FREE (child_snap.snap_idx);
        GF_FREE (current_snap.snap_idx);
        child_snap.snap_idx = NULL;
        current_snap.snap_idx = NULL;
        close (snap_data_fd);
        close (child_data_fd);
        close (child_snap.idx_fd);

        /* Update 'child' link from parent */
        snprintf (snap_idx_path, ZR_PATH_MAX, "%s/%s/parent/child",
                  path, snap_name);
        ret = unlink (snap_idx_path);
        if (ret && (errno != ENOENT)) {
                gf_log (this->name, GF_LOG_ERROR,
                        "unlink failed %s: %s", snap_idx_path,
                        strerror (errno));
                goto out;
        }

        snprintf (snap_idx_path, ZR_PATH_MAX, "%s/%s/child", path, snap_name);
        ret = readlink (snap_idx_path, link_str, ZR_PATH_MAX);
        if (ret > 0) {
                snprintf (snap_idx_path, ZR_PATH_MAX, "%s/%s/parent/child",
                          path, snap_name);
                ret = symlink (link_str, snap_idx_path);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR,
                                "symlink failed %s: %s", snap_idx_path,
                                strerror (errno));
                        goto out;
                }
                memset (link_str, 0, strlen (link_str));
        }

        /* Update 'parent' link from child */
        snprintf (snap_idx_path, ZR_PATH_MAX, "%s/%s/parent", path, snap_name);
        ret = readlink (snap_idx_path, link_str, ZR_PATH_MAX);
        if (ret <= 0)
                gf_log (this->name, GF_LOG_ERROR, "failed to read the symlink %s",
                        snap_idx_path);

        snprintf (snap_idx_path, ZR_PATH_MAX, "%s/%s/parent", path, child_name);
        ret = unlink (snap_idx_path);
        if (ret) {
                gf_log (this->name, GF_LOG_ERROR, "unlink failed %s: %s",
                        snap_idx_path, strerror (errno));
                goto out;
        }
        ret = symlink (link_str, snap_idx_path);
        if (ret) {
                gf_log (this->name, GF_LOG_ERROR, "symlink failed %s: %s",
                        snap_idx_path, strerror (errno));
                goto out;
        }

        /* Remove the snap shot path from backend */
        {
                snprintf (link_str, ZR_PATH_MAX, "rm -rf %s/%s", path,
                          snap_name);
                gf_log (this->name, GF_LOG_INFO, "removing snap path %s", snap_name);
                ret = system (link_str);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR,
                                "failed to remove temp path %s", snap_name);
                        goto out;
                }

        }
        gf_log (this->name, GF_LOG_DEBUG, "successful unlink of %s (%s)",
                path, snap_name);

        ret = 0;
out:
        return ret;
}

static int
__gf_snap_delete_root_snapshot (xlator_t *this, const char *path,
                                const char *snap_name, const char *child_name)
{
        char snap_path[ZR_PATH_MAX];
        char child_path[ZR_PATH_MAX];
        char snap_idx_path[ZR_PATH_MAX];
        char child_idx_path[ZR_PATH_MAX];

        struct stat buf = {0,};
        struct snap_fds child_snap = {0,};
        struct snap_fds current_snap = {0,};

        int ret = -1;
        int snap_data_fd = 0;
        int child_data_fd = 0;
        int idx = 0;
        off_t offset = 0;
        size_t size = 0;
        int64_t tmp_size = 0;
        off_t tmp_offset = 0;
        char buffer[GF_UNIT_MB];
        char gfid[50] = {0,};

        snprintf (child_path, ZR_PATH_MAX, "%s/%s/data", path, child_name);
        snprintf (snap_path, ZR_PATH_MAX, "%s/%s/data", path, snap_name);

        snprintf (child_idx_path, ZR_PATH_MAX, "%s/%s/index", path, child_name);
        snprintf (snap_idx_path, ZR_PATH_MAX, "%s/%s/index", path, snap_name);

        ret = stat (child_path, &buf);
        if (ret) {
                gf_log (this->name, GF_LOG_ERROR, "stat failed %s: %s",
                        child_path, strerror (errno));
                goto out;
        }

        snap_data_fd = open (snap_path, O_RDWR);
        if (snap_data_fd < 0) {
                gf_log (this->name, GF_LOG_ERROR, "open failed %s: %s",
                        snap_path, strerror (errno));
                goto out;
        }

        child_data_fd = open (child_path, O_RDONLY);
        if (child_data_fd < 0) {
                gf_log (this->name, GF_LOG_ERROR, "open failed %s: %s",
                        child_path, strerror (errno));
                goto out;
        }

        ret = gf_snap_read_index_file (child_idx_path, O_RDONLY, &child_snap);
        if (ret) {
                gf_log (this->name, GF_LOG_ERROR, "open failed %s",
                        child_idx_path);
                goto out;
        }

        ret = gf_snap_read_index_file (snap_idx_path, O_RDONLY, &current_snap);
        if (ret) {
                gf_log (this->name, GF_LOG_ERROR, "open failed %s",
                        snap_idx_path);
                goto out;
        }

        /* Should be easy */
        /* Write everything from the child */
        for (idx = 0; idx < child_snap.idx_len; idx++) {
                size = child_snap.snap_idx[idx].size;
                offset = child_snap.snap_idx[idx].start;
                tmp_offset = 0;
                for (tmp_size = (size > GF_UNIT_MB) ? GF_UNIT_MB : size;
                     tmp_size > 0;
                     tmp_size -= GF_UNIT_MB) {
                        ret = lseek (child_data_fd, offset + tmp_offset, 0);
                        if (ret < 0) {
                                gf_log (this->name, GF_LOG_ERROR,
                                        "lseek failed %s: %s",
                                        child_path, strerror (errno));
                                goto out;
                        }
                        ret = read (child_data_fd, buffer, tmp_size);
                        if (ret < 0) {
                                gf_log (this->name, GF_LOG_ERROR,
                                        "read failed %s: %s", child_path,
                                        strerror (errno));
                                goto out;

                        }
                        ret = lseek (snap_data_fd, offset + tmp_offset, 0);
                        if (ret < 0) {
                                gf_log (this->name, GF_LOG_ERROR,
                                        "lseek failed %s: %s",
                                        snap_path, strerror (errno));
                                goto out;
                        }
                        ret = write (snap_data_fd, buffer, tmp_size);
                        if (ret < 0) {
                                gf_log (this->name, GF_LOG_ERROR,
                                        "write failed %s: %s",
                                        snap_path, strerror (errno));
                                goto out;

                        }
                        tmp_offset += tmp_size;
                }
        }
        close (snap_data_fd);
        close (child_data_fd);

        ret = truncate (snap_path, buf.st_size);
        if (ret) {
                gf_log (this->name, GF_LOG_ERROR,
                        "truncate failed %s: %s", snap_path, strerror (errno));
                goto out;
        }

        ret = chmod (snap_path, buf.st_mode);
        if (ret) {
                gf_log (this->name, GF_LOG_ERROR, "chmod failed %s: %s",
                        snap_path, strerror (errno));
                goto out;
        }
        /* TODO: do we need chown ? */

        ret = rename (snap_idx_path, child_idx_path);
        if (ret) {
                gf_log (this->name, GF_LOG_ERROR, "rename failed %s: %s",
                        snap_idx_path, strerror (errno));
                goto out;
        }

        ret = sys_lgetxattr (child_path, GF_GFID_KEY, gfid, 48);
        if (ret <= 0) {
                gf_log (this->name, GF_LOG_ERROR, "failed to get gfid of %s",
                        child_path);
        } else {
                ret = sys_lremovexattr (snap_path, GF_GFID_KEY);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "remove of gfid on %s failed",
                                snap_path);
                }

                ret = sys_lsetxattr (snap_path, GF_GFID_KEY, gfid, 16, 0);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR,
                                "setting gfid on %s failed: %d",
                                snap_path, ret);
                        goto out;
                }
        }

        /* Ideally gfid should be preserved */
        ret = rename (snap_path, child_path);
        if (ret) {
                gf_log (this->name, GF_LOG_ERROR,
                        "rename failed %s: %s", snap_path, strerror (errno));
                goto out;
        }

        snprintf (child_path, ZR_PATH_MAX, "%s/%s/parent", path, child_name);
        ret = unlink (child_path);
        if (ret) {
                gf_log (this->name, GF_LOG_ERROR, "unlink failed %s: %s",
                        child_path, strerror (errno));
                goto out;
        }

        snprintf (child_path, ZR_PATH_MAX, "%s/%s/child", path, snap_name);
        ret = unlink (child_path);
        if (ret) {
                gf_log (this->name, GF_LOG_DEBUG, "unlink on %s failed: %s",
                        child_path, strerror (errno));
        }

        snprintf (snap_path, ZR_PATH_MAX, "%s/%s", path, snap_name);

        ret = rmdir (snap_path);
        if (ret) {
                gf_log (this->name, GF_LOG_ERROR, "rmdir failed %s: %s",
                        snap_path, strerror (errno));
                goto out;
        }

        gf_log (this->name, GF_LOG_DEBUG, "delete successful");

        ret = 0;
out:
        return ret;
}

int
gf_snap_delete_snapshot (xlator_t *this, loc_t *loc, const char *path,
                         const char *snap_name)
{
        char         snap_path[ZR_PATH_MAX];
        char         parent_path[ZR_PATH_MAX];
        char         child_path[ZR_PATH_MAX];
        char         child_link[ZR_PATH_MAX];
        char         cmd_str[ZR_PATH_MAX];
        struct stat  buf           = {0,};
        int          ret           = -1;
        int          root_snapshot = 0;
        int          leaf_snapshot = 0;
        char        *child_name    = NULL;
        fd_t        *iter_fd       = NULL;
	uint64_t     tmp_pfd       = 0;
        int          fd_found      = 0;
        struct posix_fd *pfd       = NULL;

        gf_log (this->name, GF_LOG_DEBUG, "loc->path:%s path:%s snap_name:%s",
                loc->path, path, snap_name);

        /* Handle open fd cases too */

        if (strcmp (snap_name, "HEAD") == 0) {
                errno = EPERM;
                goto out;
        }

        /* If the file is cloned, then don't delete anything, but take-out
           the snapshot from namespace
        */
        snprintf (snap_path, ZR_PATH_MAX, "%s/%s/data", path, snap_name);
        ret = stat (snap_path, &buf);
        if (ret) {
                gf_log (this->name, GF_LOG_ERROR, "%s: stat failed %s",
                        snap_path, strerror (errno));
                goto out;
        }

        /* File is not cloned */
        strcpy (snap_path, path);
        strcat (snap_path, "/");
        strcat (snap_path, snap_name);

        strcpy (parent_path, snap_path);
        strcat (parent_path, "/parent");

        ret = stat (parent_path, &buf);
        if (ret)
                root_snapshot = 1;

        strcpy (child_path, snap_path);
        strcat (child_path, "/child");

        ret = readlink (child_path, child_link, 1024);
        if (ret < 0)
                leaf_snapshot = 1;

        /* ideally only one child per snapshot */
        if (ret > 0) {
                child_name = strrchr ((char *)child_link, '/');
                if (child_name) child_name++;
        }

        if (!child_name)
                child_name = "HEAD";

        ret = -1;
        errno = ENOSYS;

        if (!list_empty (&loc->inode->fd_list)) {
                list_for_each_entry (iter_fd, &loc->inode->fd_list,
                                     inode_list) {
                        gf_log (this->name, GF_LOG_DEBUG,
                                "fd is open");
                        ret = fd_ctx_get (iter_fd, this, &tmp_pfd);
                        if (ret < 0) {
                                gf_log (this->name, GF_LOG_DEBUG,
                                        "pfd not found in fd's ctx");
                                goto out;
                        }
                        pfd = (struct posix_fd *)(long)tmp_pfd;
                        gf_sync_and_free_pfd (this, pfd);
                        close (pfd->fd);
                        pfd->fd = 0;
                        fd_found = 1;
                }
        }

        if (root_snapshot && (buf.st_nlink == 1)) {
                gf_log (this->name, GF_LOG_DEBUG, "root snapshot");
                ret = __gf_snap_delete_root_snapshot (this, path, snap_name,
                                                      child_name);
        } else {
                gf_log (this->name, GF_LOG_DEBUG, "middle");
                ret = __gf_snap_delete_snapshot (this, path, snap_name,
                                                 child_name, buf.st_nlink - 1);
        }


        if (root_snapshot && leaf_snapshot) {
                gf_log (this->name, GF_LOG_INFO, "last snapshot deleted, "
                        "need to fallback to regular file mode");
                strcpy (parent_path, path);
                strcat (parent_path, "......gfs..tmp");
                ret = rename (path, parent_path);
                if (ret)
                        gf_log (this->name, GF_LOG_ERROR, "rename failed %s -> %s", path, parent_path);

                strcpy (child_path, parent_path);
                strcat (child_path, "/HEAD/data");
                ret = rename (child_path, path);
                if (ret)
                        gf_log (this->name, GF_LOG_ERROR, "rename failed %s -> %s", child_path, path);

                snprintf (cmd_str, ZR_PATH_MAX, "rm -rf %s", parent_path);
                gf_log (this->name, GF_LOG_DEBUG, "removing temp path %s", parent_path);
                ret = system (cmd_str);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR, "failed to remove temp path");
                        goto out;
                }
        }

        if (fd_found) {
                list_for_each_entry (iter_fd, &loc->inode->fd_list,
                                     inode_list) {
                        ret = fd_ctx_get (iter_fd, this, &tmp_pfd);
                        if (ret < 0) {
                                gf_log (this->name, GF_LOG_DEBUG,
                                        "pfd not found in fd's ctx");
                                goto out;
                        }
                        pfd = (struct posix_fd *)(long)tmp_pfd;
                        if (root_snapshot && leaf_snapshot) {
                                pfd->snapshot = 0;
                                pfd->fd = open (path, pfd->flags);
                                if (pfd->fd < 0) {
                                        gf_log (this->name, GF_LOG_ERROR,
                                                "failed to open %s", path);
                                }
                                goto out;
                        }

                        pfd->fd = gf_snap_open_snapshot (this, pfd, path,
                                                         NULL, pfd->flags);
                        if (pfd->fd == -1) {
                                ret = -1;
                                gf_log (this->name, GF_LOG_ERROR,
                                        "failed to open the snapshot %s", path);
                                goto out;
                        }
                }
        }

out:
        return ret;
}

int
gf_snap_delete_full_path (xlator_t *this, const char *path)
{
        int ret = 0;
        char cmd_str[ZR_PATH_MAX];

        snprintf (cmd_str, ZR_PATH_MAX, "rm -rf %s", path);

        gf_log (this->name, GF_LOG_INFO,
                "deleting the entire file (including snapshots): %s", path);

        system (cmd_str);

        return ret;
}
