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
        loc_t    loc;
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
        int ret = 0;

        return ret;
}


int
mdc_inode_xatt_get (xlator_t *this, inode_t *inode, dict_t **dict)
{
        int ret = 0;

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
/*
                mdc_inode_xatt_set (this, local->loc.inode, dict);
*/
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

        return 0;

uncached:
        STACK_WIND (frame, mdc_lookup_cbk, FIRST_CHILD (this),
                    FIRST_CHILD (this)->fops->lookup, loc, xattr_req);
        return 0;
}


int
mdc_stat_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
              int32_t op_ret, int32_t op_errno, struct iatt *buf)
{
        MDC_STACK_UNWIND (stat, frame, op_ret, op_errno, buf);

        return 0;
}


int
mdc_stat (call_frame_t *frame, xlator_t *this, loc_t *loc)
{
        int          ret;
        struct iatt  stbuf;

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
