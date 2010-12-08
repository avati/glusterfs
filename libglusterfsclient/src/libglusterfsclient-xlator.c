/*
  Copyright (c) 2011 Gluster, Inc. <http://www.gluster.com>
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
#include "config.h"
#endif

#include "libglusterfsclient-internals.h"


int
libglusterfsclient_forget (xlator_t *this, inode_t *inode)
{
        return 0;
}


int
libglusterfsclient_release (xlator_t *this, fd_t *fd)
{
        return 0;
}


int
libglusterfsclient_releasedir (xlator_t *this, fd_t *fd)
{
        return 0;
}


int
libglusterfsclient_notify (xlator_t *this, int event, void *data)
{
        return 0;
}


int
libglusterfsclient_init (xlator_t *this)
{
        return 0;
}


void
libglusterfsclient_fini (xlator_t *this)
{
        return;
}


int
libglusterfsclient_mem_acct_init (xlator_t *this)
{
        int     ret = -1;

        if (!this)
                return ret;

        ret = xlator_mem_acct_init (this, glfs_mt_end + 1);

        if (ret != 0) {
                gf_log(this->name, GF_LOG_ERROR, "Memory accounting init"
                       "failed");
                return ret;
        }

        return ret;
}


struct xlator_fops libglusterfsclient_fops = { };


struct xlator_cbks libglusterfsclient_cbks = {
        .forget     = libglusterfsclient_forget,
        .release    = libglusterfsclient_release,
        .releasedir = libglusterfsclient_releasedir
};


struct xlator_dumpops libglusterfsclient_dumpops = { };


int
glfs_session_set_master (struct glfs_session *session)
{
        glusterfs_ctx_t     *ctx = NULL;
        xlator_t            *master = NULL;
        int                  ret = -1;


        ctx = session->ctx;

        master = GF_CALLOC (1, sizeof (*master),
                            glfs_mt_xlator_t);
        if (!master)
                goto err;

        master->name = gf_strdup ("libglusterfsclient");
        if (!master->name)
                goto err;

        master->type = gf_strdup ("access/libglusterfsclient");
        if (!master->type)
                goto err;

        master->fops = &libglusterfsclient_fops;
        master->cbks = &libglusterfsclient_cbks;
        master->init = &libglusterfsclient_init;
        master->fini = &libglusterfsclient_fini;
        master->notify = &libglusterfsclient_notify;
        master->dumpops = &libglusterfsclient_dumpops;
        master->mem_acct_init = &libglusterfsclient_mem_acct_init;
//        master->reconfigure = &libglusterfsclient_reconfigure;


        INIT_LIST_HEAD (&master->volume_options);

        master->ctx      = ctx;
        master->options  = get_new_dict ();

        ret = xlator_init (master);
        if (ret)
                goto err;

        ctx->master = master;

        return 0;

err:
        if (master) {
                xlator_destroy (master);
        }

        return -1;
}

