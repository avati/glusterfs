/*
  Copyright (c) 2011 Gluster, Inc. <http://www.gluster.com>
  This file is part of GlusterFS.

  GlusterFS is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published
  by the Free Software Foundation; either version 3 of the License,
  or (at your option) any later version.

  GlusterFS is distributed in the hope that it will be useful, but
  WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program.  If not, see
  <http://www.gnu.org/licenses/>.
*/

#ifndef _CONFIG_H
#define _CONFIG_H
#include "config.h"
#endif

#include "glusterfs.h"
#include "logging.h"
#include "dict.h"
#include "xlator.h"
#include <assert.h>
#include <sys/time.h>


struct mdc_local;
typedef struct mdc_local mdc_local_t;

#define MDC_STACK_UNWIND(fop, frame, params ...) do {           \
                mdc_local_t *__local = NULL;                    \
                xlator_t    *__xl    = NULL;                    \
                if (frame) {                                    \
                        __xl         = frame->this;             \
                        __local      = frame->local;            \
                        frame->local = NULL;                    \
                }                                               \
                STACK_UNWIND_STRICT (fop, frame, params);       \
                mdc_local_wipe (__xl, __local);                 \
        } while (0)


struct md_cache {
        ia_prot_t     md_prot;
        uint32_t      md_nlink;
        uint32_t      md_uid;
        uint32_t      md_gid;
        uint32_t      md_atime;
        uint32_t      md_mtime;
        uint32_t      md_ctime;
        uint64_t      md_rdev;
        uint64_t      md_size;
        uint64_t      md_blocks;
        dict_t       *xattr;
};


struct mdc_local {
        loc_t     loc;
        loc_t     loc2;
        fd_t     *fd;
};


mdc_local_t *
mdc_local_get (call_frame_t *frame)
{
        mdc_local_t *local = NULL;

        local = frame->local;
        if (local)
                goto out;

        local = CALLOC (1, sizeof (*local));
        if (!local)
                goto out;

        frame->local = local;
out:
        return local;
}


void
mdc_local_wipe (xlator_t *this, mdc_local_t *local)
{
        if (!local)
                return;

        loc_wipe (&local->loc);

        loc_wipe (&local->loc2);

        if (local->fd)
                fd_unref (local->fd);

        FREE (local);
        return;
}


int
mdc_inode_wipe (xlator_t *this, inode_t *inode)
{
        int              ret = 0;
        uint64_t         mdc_int = 0;
        struct md_cache *mdc = NULL;

        ret = inode_ctx_del (inode, this, &mdc_int);
        if (ret != 0)
                goto out;

        mdc = (void *) (long) mdc_int;

        FREE (mdc);

        ret = 0;
out:
        return ret;
}


struct md_cache *
mdc_inode_prep (xlator_t *this, inode_t *inode)
{
        int ret = 0;
        struct md_cache *mdc = NULL;
        uint64_t         mdc_int = 0;

        LOCK (&inode->lock);
        {
                ret = __inode_ctx_get (inode, this, &mdc_int);
                mdc = (void *) (long) (mdc_int);
                if (ret == 0)
                        goto unlock;

                mdc = CALLOC (1, sizeof (*mdc));
                if (!mdc) {
                        gf_log (this->name, GF_LOG_ERROR,
                                "out of memory :(");
                        goto unlock;
                }

                mdc_int = (long) mdc;
                ret = __inode_ctx_set2 (inode, this, &mdc_int, 0);
                if (ret) {
                        gf_log (this->name, GF_LOG_ERROR,
                                "out of memory :(");
                        FREE (mdc);
                        mdc = NULL;
                }
        }
unlock:
        UNLOCK (&inode->lock);

        return mdc;
}


void
mdc_from_iatt (struct md_cache *mdc, struct iatt *iatt)
{
        mdc->md_prot    = iatt->ia_prot;
        mdc->md_nlink   = iatt->ia_nlink;
        mdc->md_uid     = iatt->ia_uid;
        mdc->md_gid     = iatt->ia_gid;
        mdc->md_atime   = iatt->ia_atime;
        mdc->md_mtime   = iatt->ia_mtime;
        mdc->md_ctime   = iatt->ia_ctime;
        mdc->md_rdev    = iatt->ia_rdev;
        mdc->md_size    = iatt->ia_size;
        mdc->md_blocks  = iatt->ia_blocks;
}

void
mdc_to_iatt (struct md_cache *mdc, struct iatt *iatt)
{
        iatt->ia_prot   = mdc->md_prot;
        iatt->ia_nlink  = mdc->md_nlink;
        iatt->ia_uid    = mdc->md_uid;
        iatt->ia_gid    = mdc->md_gid;
        iatt->ia_atime  = mdc->md_atime;
        iatt->ia_mtime  = mdc->md_mtime;
        iatt->ia_ctime  = mdc->md_ctime;
        iatt->ia_rdev   = mdc->md_rdev;
        iatt->ia_size   = mdc->md_size;
        iatt->ia_blocks = mdc->md_blocks;
}


int
mdc_inode_iatt_set (xlator_t *this, inode_t *inode, struct iatt *iatt)
{
        int              ret = -1;
        struct md_cache *mdc = NULL;

        if (!iatt->ia_ctime)
                return 0;

        mdc = mdc_inode_prep (this, inode);
        if (!mdc)
                goto out;

        mdc_from_iatt (mdc, iatt);

        ret = 0;
out:
        return ret;
}


int
mdc_inode_iatt_get (xlator_t *this, inode_t *inode, struct iatt *iatt)
{
        int              ret = -1;
        struct md_cache *mdc = NULL;

        if (inode_ctx_get (inode, this, NULL) != 0)
                goto out;

        mdc = mdc_inode_prep (this, inode);
        if (!mdc)
                goto out;

        mdc_to_iatt (mdc, iatt);

        uuid_copy (iatt->ia_gfid, inode->gfid);
        iatt->ia_ino    = inode->ino;
        iatt->ia_dev    = 42;
        iatt->ia_type   = inode->ia_type;

        ret = 0;
out:
        return ret;
}


int
mdc_inode_mtim_set (xlator_t *this, inode_t *inode, uint64_t mtim)
{
        int ret = 0;

        return ret;
}


int
mdc_inode_mtim_get (xlator_t *this, inode_t *inode, uint64_t *mtim)
{
        int ret = 0;

        return ret;
}


int
mdc_inode_xatt_set (xlator_t *this, inode_t *inode, dict_t *dict)
{
        int              ret = -1;
        struct md_cache *mdc = NULL;

        mdc = mdc_inode_prep (this, inode);
        if (!mdc)
                goto out;

        if (!dict)
                goto out;

        if (mdc->xattr)
                dict_unref (mdc->xattr);

        mdc->xattr = dict_ref (dict);

        ret = 0;
out:
        return ret;
}


int
mdc_inode_xatt_get (xlator_t *this, inode_t *inode, dict_t **dict)
{
        int              ret = -1;
        struct md_cache *mdc = NULL;

        if (inode_ctx_get (inode, this, NULL) != 0)
                goto out;

        mdc = mdc_inode_prep (this, inode);
        if (!mdc)
                goto out;

        if (!mdc->xattr)
                goto out;

        if (dict)
                *dict = dict_ref (mdc->xattr);

        ret = 0;
out:
        return ret;
}


void
checkfn (dict_t *this, char *key, data_t *value, void *data)
{
        struct {
                int ret;
                dict_t *rsp;
        } *pair = data;

        if (strcmp (key, "gfid-req") == 0)
                return;

        pair->ret = 0;
}


int
mdc_xattr_satisfied (xlator_t *this, dict_t *req, dict_t *rsp)
{
        struct {
                int ret;
                dict_t *rsp;
        } pair = {
                .ret = 1,
                .rsp = rsp,
        };

        dict_foreach (req, checkfn, &pair);

        return pair.ret;
}


int
mdc_lookup_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                int32_t op_ret,	int32_t op_errno, inode_t *inode,
                struct iatt *stbuf, dict_t *dict, struct iatt *postparent)
{
        mdc_local_t *local = NULL;

        local = frame->local;

        if (op_ret != 0)
                goto out;

        if (!local)
                goto out;

        if (local->loc.parent) {
                mdc_inode_iatt_set (this, local->loc.parent, postparent);
/*
                mdc_inode_mtim_set (this, local->loc.inode,
                                    postparent->ia_mtime);
*/
        }

        if (local->loc.inode) {
                mdc_inode_iatt_set (this, local->loc.inode, stbuf);
                mdc_inode_xatt_set (this, local->loc.inode, dict);
        }
out:
        MDC_STACK_UNWIND (lookup, frame, op_ret, op_errno, inode, stbuf,
                          dict, postparent);
        return 0;
}


int
mdc_lookup (call_frame_t *frame, xlator_t *this, loc_t *loc,
            dict_t *xattr_req)
{
        uint64_t     cached_mtime = 0;
        uint64_t     parent_mtime = 0;
        int          ret = 0;
        struct iatt  stbuf = {0, };
        struct iatt  postparent = {0, };
        dict_t      *xattr_rsp = NULL;
        mdc_local_t *local = NULL;


        local = mdc_local_get (frame);
        if (!local)
                goto uncached;

        loc_copy (&local->loc, loc);


        ret = mdc_inode_iatt_get (this, loc->inode, &stbuf);
        if (ret != 0)
                goto uncached;

        if (loc->parent) {
                ret = mdc_inode_iatt_get (this, loc->parent, &postparent);
                if (ret != 0)
                        goto uncached;

                parent_mtime = postparent.ia_mtime;

                ret = mdc_inode_mtim_get (this, loc->inode, &cached_mtime);
                if (ret != 0)
                        goto uncached;

/*
                if (cached_mtime < parent_mtime)
                        goto uncached;
*/
        }

        if (xattr_req) {
                ret = mdc_inode_xatt_get (this, loc->inode, &xattr_rsp);
                if (ret != 0)
                        goto uncached;

                if (!mdc_xattr_satisfied (this, xattr_req, xattr_rsp))
                        goto uncached;
        }

        MDC_STACK_UNWIND (lookup, frame, 0, 0, loc->inode, &stbuf,
                          xattr_rsp, &postparent);

        if (xattr_rsp)
                dict_unref (xattr_rsp);

        return 0;

uncached:
        STACK_WIND (frame, mdc_lookup_cbk, FIRST_CHILD (this),
                    FIRST_CHILD (this)->fops->lookup, loc, xattr_req);

        if (xattr_rsp)
                dict_unref (xattr_rsp);

        return 0;
}


int
mdc_stat_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
              int32_t op_ret, int32_t op_errno, struct iatt *buf)
{
        mdc_local_t  *local = NULL;

        if (op_ret != 0)
                goto out;

        local = frame->local;
        if (!local)
                goto out;

        mdc_inode_iatt_set (this, local->loc.inode, buf);

out:
        MDC_STACK_UNWIND (stat, frame, op_ret, op_errno, buf);

        return 0;
}


int
mdc_stat (call_frame_t *frame, xlator_t *this, loc_t *loc)
{
        int           ret;
        struct iatt   stbuf;
        mdc_local_t  *local = NULL;

        local = mdc_local_get (frame);
        if (!local)
                goto uncached;

        loc_copy (&local->loc, loc);

        ret = mdc_inode_iatt_get (this, loc->inode, &stbuf);
        if (ret != 0)
                goto uncached;

        MDC_STACK_UNWIND (stat, frame, 0, 0, &stbuf);

        return 0;

uncached:
        STACK_WIND (frame, mdc_stat_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->stat,
                    loc);
        return 0;
}


int
mdc_fstat_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
               int32_t op_ret, int32_t op_errno, struct iatt *buf)
{
        mdc_local_t  *local = NULL;

        if (op_ret != 0)
                goto out;

        local = frame->local;
        if (!local)
                goto out;

        mdc_inode_iatt_set (this, local->fd->inode, buf);

out:
        MDC_STACK_UNWIND (fstat, frame, op_ret, op_errno, buf);

        return 0;
}


int
mdc_fstat (call_frame_t *frame, xlator_t *this, fd_t *fd)
{
        int           ret;
        struct iatt   stbuf;
        mdc_local_t  *local = NULL;

        local = mdc_local_get (frame);
        if (!local)
                goto uncached;

        local->fd = fd_ref (fd);

        ret = mdc_inode_iatt_get (this, fd->inode, &stbuf);
        if (ret != 0)
                goto uncached;

        MDC_STACK_UNWIND (fstat, frame, 0, 0, &stbuf);

        return 0;

uncached:
        STACK_WIND (frame, mdc_fstat_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->fstat,
                    fd);
        return 0;
}


int
mdc_truncate_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                  int32_t op_ret, int32_t op_errno,
                  struct iatt *prebuf, struct iatt *postbuf)
{
        mdc_local_t  *local = NULL;

        local = frame->local;

        if (op_ret != 0)
                goto out;

        if (!local)
                goto out;

        mdc_inode_iatt_set (this, local->loc.inode, postbuf);

out:
        MDC_STACK_UNWIND (truncate, frame, op_ret, op_errno, prebuf, postbuf);

        return 0;
}


int
mdc_truncate (call_frame_t *frame, xlator_t *this, loc_t *loc,
              off_t offset)
{
        mdc_local_t  *local = NULL;

        local = mdc_local_get (frame);

        local->loc.inode = inode_ref (loc->inode);

        STACK_WIND (frame, mdc_truncate_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->truncate,
                    loc, offset);
        return 0;
}


int
mdc_ftruncate_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                   int32_t op_ret, int32_t op_errno,
                   struct iatt *prebuf, struct iatt *postbuf)
{
        mdc_local_t  *local = NULL;

        local = frame->local;

        if (op_ret != 0)
                goto out;

        if (!local)
                goto out;

        mdc_inode_iatt_set (this, local->fd->inode, postbuf);

out:
        MDC_STACK_UNWIND (ftruncate, frame, op_ret, op_errno, prebuf, postbuf);

        return 0;
}


int
mdc_ftruncate (call_frame_t *frame, xlator_t *this, fd_t *fd,
               off_t offset)
{
        mdc_local_t  *local = NULL;

        local = mdc_local_get (frame);

        local->fd = fd_ref (fd);

        STACK_WIND (frame, mdc_ftruncate_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->ftruncate,
                    fd, offset);
        return 0;
}


int
mdc_mknod_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
               int32_t op_ret, int32_t op_errno, inode_t *inode,
               struct iatt *buf, struct iatt *preparent, struct iatt *postparent)
{
        mdc_local_t *local = NULL;

        local = frame->local;

        if (op_ret != 0)
                goto out;

        if (!local)
                goto out;

        if (local->loc.parent) {
                mdc_inode_iatt_set (this, local->loc.parent, postparent);
        }

        if (local->loc.inode) {
                mdc_inode_iatt_set (this, local->loc.inode, buf);
        }
out:
        MDC_STACK_UNWIND (mknod, frame, op_ret, op_errno, inode, buf,
                          preparent, postparent);
        return 0;
}


int
mdc_mknod (call_frame_t *frame, xlator_t *this, loc_t *loc,
           mode_t mode, dev_t rdev, dict_t *params)
{
        mdc_local_t  *local = NULL;

        local = mdc_local_get (frame);

        loc_copy (&local->loc, loc);

        STACK_WIND (frame, mdc_mknod_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->mknod,
                    loc, mode, rdev, params);
        return 0;
}


int
mdc_mkdir_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
               int32_t op_ret, int32_t op_errno, inode_t *inode,
               struct iatt *buf, struct iatt *preparent, struct iatt *postparent)
{
        mdc_local_t *local = NULL;

        local = frame->local;

        if (op_ret != 0)
                goto out;

        if (!local)
                goto out;

        if (local->loc.parent) {
                mdc_inode_iatt_set (this, local->loc.parent, postparent);
        }

        if (local->loc.inode) {
                mdc_inode_iatt_set (this, local->loc.inode, buf);
        }
out:
        MDC_STACK_UNWIND (mkdir, frame, op_ret, op_errno, inode, buf,
                          preparent, postparent);
        return 0;
}


int
mdc_mkdir (call_frame_t *frame, xlator_t *this, loc_t *loc,
           mode_t mode, dict_t *params)
{
        mdc_local_t  *local = NULL;

        local = mdc_local_get (frame);

        loc_copy (&local->loc, loc);

        STACK_WIND (frame, mdc_mkdir_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->mkdir,
                    loc, mode, params);
        return 0;
}


int
mdc_unlink_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                int32_t op_ret, int32_t op_errno,
                struct iatt *preparent, struct iatt *postparent)
{
        mdc_local_t *local = NULL;

        local = frame->local;

        if (op_ret != 0)
                goto out;

        if (!local)
                goto out;

        if (local->loc.parent) {
                mdc_inode_iatt_set (this, local->loc.parent, postparent);
        }

out:
        MDC_STACK_UNWIND (unlink, frame, op_ret, op_errno,
                          preparent, postparent);
        return 0;
}


int
mdc_unlink (call_frame_t *frame, xlator_t *this, loc_t *loc)
{
        mdc_local_t  *local = NULL;

        local = mdc_local_get (frame);

        loc_copy (&local->loc, loc);

        STACK_WIND (frame, mdc_unlink_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->unlink,
                    loc);
        return 0;
}




int
mdc_rmdir_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                int32_t op_ret, int32_t op_errno,
                struct iatt *preparent, struct iatt *postparent)
{
        mdc_local_t *local = NULL;

        local = frame->local;

        if (op_ret != 0)
                goto out;

        if (!local)
                goto out;

        if (local->loc.parent) {
                mdc_inode_iatt_set (this, local->loc.parent, postparent);
        }

out:
        MDC_STACK_UNWIND (rmdir, frame, op_ret, op_errno,
                          preparent, postparent);
        return 0;
}


int
mdc_rmdir (call_frame_t *frame, xlator_t *this, loc_t *loc, int flag)
{
        mdc_local_t  *local = NULL;

        local = mdc_local_get (frame);

        loc_copy (&local->loc, loc);

        STACK_WIND (frame, mdc_rmdir_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->rmdir,
                    loc, flag);
        return 0;
}


int
mdc_symlink_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                 int32_t op_ret, int32_t op_errno, inode_t *inode,
                 struct iatt *buf, struct iatt *preparent, struct iatt *postparent)
{
        mdc_local_t *local = NULL;

        local = frame->local;

        if (op_ret != 0)
                goto out;

        if (!local)
                goto out;

        if (local->loc.parent) {
                mdc_inode_iatt_set (this, local->loc.parent, postparent);
        }

        if (local->loc.inode) {
                mdc_inode_iatt_set (this, local->loc.inode, buf);
        }
out:
        MDC_STACK_UNWIND (symlink, frame, op_ret, op_errno, inode, buf,
                          preparent, postparent);
        return 0;
}


int
mdc_symlink (call_frame_t *frame, xlator_t *this, const char *linkname,
             loc_t *loc, dict_t *params)
{
        mdc_local_t  *local = NULL;

        local = mdc_local_get (frame);

        loc_copy (&local->loc, loc);

        STACK_WIND (frame, mdc_symlink_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->symlink,
                    linkname, loc, params);
        return 0;
}


int
mdc_rename_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                int32_t op_ret, int32_t op_errno, struct iatt *buf,
                struct iatt *preoldparent, struct iatt *postoldparent,
                struct iatt *prenewparent, struct iatt *postnewparent)
{
        mdc_local_t *local = NULL;

        local = frame->local;

        if (op_ret != 0)
                goto out;

        if (!local)
                goto out;

        if (local->loc.parent) {
                mdc_inode_iatt_set (this, local->loc.parent, postoldparent);
        }

        if (local->loc.inode) {
                mdc_inode_iatt_set (this, local->loc.inode, buf);
        }

        if (local->loc2.parent) {
                mdc_inode_iatt_set (this, local->loc2.parent, postnewparent);
        }
out:
        MDC_STACK_UNWIND (rename, frame, op_ret, op_errno, buf,
                          preoldparent, postoldparent, prenewparent, postnewparent);
        return 0;
}


int
mdc_rename (call_frame_t *frame, xlator_t *this,
            loc_t *oldloc, loc_t *newloc)
{
        mdc_local_t  *local = NULL;

        local = mdc_local_get (frame);

        loc_copy (&local->loc, oldloc);
        loc_copy (&local->loc2, newloc);

        STACK_WIND (frame, mdc_rename_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->rename,
                    oldloc, newloc);
        return 0;
}


int
mdc_link_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
              int32_t op_ret, int32_t op_errno, inode_t *inode, struct iatt *buf,
              struct iatt *preparent, struct iatt *postparent)
{
        mdc_local_t *local = NULL;

        local = frame->local;

        if (op_ret != 0)
                goto out;

        if (!local)
                goto out;

        if (local->loc.inode) {
                mdc_inode_iatt_set (this, local->loc.inode, buf);
        }

        if (local->loc2.parent) {
                mdc_inode_iatt_set (this, local->loc2.parent, postparent);
        }
out:
        MDC_STACK_UNWIND (link, frame, op_ret, op_errno, inode, buf,
                          preparent, postparent);
        return 0;
}


int
mdc_link (call_frame_t *frame, xlator_t *this,
          loc_t *oldloc, loc_t *newloc)
{
        mdc_local_t  *local = NULL;

        local = mdc_local_get (frame);

        loc_copy (&local->loc, oldloc);
        loc_copy (&local->loc2, newloc);

        STACK_WIND (frame, mdc_link_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->link,
                    oldloc, newloc);
        return 0;
}


int
mdc_create_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                int32_t op_ret, int32_t op_errno, fd_t *fd, inode_t *inode,
                struct iatt *buf, struct iatt *preparent, struct iatt *postparent)
{
        mdc_local_t *local = NULL;

        local = frame->local;

        if (op_ret != 0)
                goto out;

        if (!local)
                goto out;

        if (local->loc.parent) {
                mdc_inode_iatt_set (this, local->loc.parent, postparent);
        }

        if (local->loc.inode) {
                mdc_inode_iatt_set (this, inode, buf);
        }
out:
        MDC_STACK_UNWIND (create, frame, op_ret, op_errno, fd, inode, buf,
                          preparent, postparent);
        return 0;
}


int
mdc_create (call_frame_t *frame, xlator_t *this, loc_t *loc, int flags,
            mode_t mode, fd_t *fd, dict_t *params)
{
        mdc_local_t  *local = NULL;

        local = mdc_local_get (frame);

        loc_copy (&local->loc, loc);

        STACK_WIND (frame, mdc_create_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->create,
                    loc, flags, mode, fd, params);
        return 0;
}


int
mdc_readv_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
               int32_t op_ret, int32_t op_errno,
               struct iovec *vector, int32_t count,
               struct iatt *stbuf, struct iobref *iobref)
{
        mdc_local_t  *local = NULL;

        local = frame->local;

        if (op_ret != 0)
                goto out;

        if (!local)
                goto out;

        mdc_inode_iatt_set (this, local->fd->inode, stbuf);

out:
        MDC_STACK_UNWIND (readv, frame, op_ret, op_errno, vector, count,
                          stbuf, iobref);

        return 0;
}


int
mdc_readv (call_frame_t *frame, xlator_t *this, fd_t *fd, size_t size,
           off_t offset)
{
        mdc_local_t  *local = NULL;

        local = mdc_local_get (frame);

        local->fd = fd_ref (fd);

        STACK_WIND (frame, mdc_readv_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->readv,
                    fd, size, offset);
        return 0;
}


int
mdc_writev_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
               int32_t op_ret, int32_t op_errno,
               struct iatt *prebuf, struct iatt *postbuf)
{
        mdc_local_t  *local = NULL;

        local = frame->local;

        if (op_ret == -1)
                goto out;

        if (!local)
                goto out;

        mdc_inode_iatt_set (this, local->fd->inode, postbuf);

out:
        MDC_STACK_UNWIND (writev, frame, op_ret, op_errno, prebuf, postbuf);

        return 0;
}


int
mdc_writev (call_frame_t *frame, xlator_t *this, fd_t *fd, struct iovec *vector,
            int count, off_t offset, struct iobref *iobref)
{
        mdc_local_t  *local = NULL;

        local = mdc_local_get (frame);

        local->fd = fd_ref (fd);

        STACK_WIND (frame, mdc_writev_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->writev,
                    fd, vector, count, offset, iobref);
        return 0;
}


int
mdc_setattr_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                 int32_t op_ret, int32_t op_errno,
                 struct iatt *prebuf, struct iatt *postbuf)
{
        mdc_local_t  *local = NULL;

        local = frame->local;

        if (op_ret != 0)
                goto out;

        if (!local)
                goto out;

        mdc_inode_iatt_set (this, local->loc.inode, postbuf);

out:
        MDC_STACK_UNWIND (setattr, frame, op_ret, op_errno, prebuf, postbuf);

        return 0;
}


int
mdc_setattr (call_frame_t *frame, xlator_t *this, loc_t *loc,
             struct iatt *stbuf, int valid)
{
        mdc_local_t  *local = NULL;

        local = mdc_local_get (frame);

        loc_copy (&local->loc, loc);

        STACK_WIND (frame, mdc_setattr_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->setattr,
                    loc, stbuf, valid);
        return 0;
}


int
mdc_fsetattr_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                  int32_t op_ret, int32_t op_errno,
                  struct iatt *prebuf, struct iatt *postbuf)
{
        mdc_local_t  *local = NULL;

        local = frame->local;

        if (op_ret != 0)
                goto out;

        if (!local)
                goto out;

        mdc_inode_iatt_set (this, local->fd->inode, postbuf);

out:
        MDC_STACK_UNWIND (fsetattr, frame, op_ret, op_errno, prebuf, postbuf);

        return 0;
}


int
mdc_fsetattr (call_frame_t *frame, xlator_t *this, fd_t *fd,
              struct iatt *stbuf, int valid)
{
        mdc_local_t  *local = NULL;

        local = mdc_local_get (frame);

        local->fd = fd_ref (fd);

        STACK_WIND (frame, mdc_setattr_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->fsetattr,
                    fd, stbuf, valid);
        return 0;
}



int
mdc_fsync_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
               int32_t op_ret, int32_t op_errno,
               struct iatt *prebuf, struct iatt *postbuf)
{
        mdc_local_t  *local = NULL;

        local = frame->local;

        if (op_ret != 0)
                goto out;

        if (!local)
                goto out;

        mdc_inode_iatt_set (this, local->fd->inode, postbuf);

out:
        MDC_STACK_UNWIND (fsync, frame, op_ret, op_errno, prebuf, postbuf);

        return 0;
}


int
mdc_fsync (call_frame_t *frame, xlator_t *this, fd_t *fd, int datasync)
{
        mdc_local_t  *local = NULL;

        local = mdc_local_get (frame);

        local->fd = fd_ref (fd);

        STACK_WIND (frame, mdc_fsync_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->fsync,
                    fd, datasync);
        return 0;
}


int
mdc_forget (xlator_t *this, inode_t *inode)
{
        mdc_inode_wipe (this, inode);

        return 0;
}


int
init (xlator_t *this)
{
        return 0;
}


void
fini (xlator_t *this)
{
        return;
}


struct xlator_fops fops = {
        .lookup      = mdc_lookup,
        .stat        = mdc_stat,
        .fstat       = mdc_fstat,
        .truncate    = mdc_truncate,
        .ftruncate   = mdc_ftruncate,
        .mknod       = mdc_mknod,
        .mkdir       = mdc_mkdir,
        .unlink      = mdc_unlink,
        .rmdir       = mdc_rmdir,
        .symlink     = mdc_symlink,
        .rename      = mdc_rename,
        .link        = mdc_link,
        .create      = mdc_create,
        .readv       = mdc_readv,
        .writev      = mdc_writev,
        .setattr     = mdc_setattr,
        .fsetattr    = mdc_fsetattr,
        .fsync       = mdc_fsync,
};


struct xlator_cbks cbks = {
        .forget      = mdc_forget,
};

struct volume_options options[] = {
        { .key = {NULL},
          .type = 0,
          .min = 0,
          .max = 0,
          .default_value = NULL,
          .description = NULL,
        },
};
