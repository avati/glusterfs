/*
  Copyright (c) 2012 Red Hat, Inc. <http://www.redhat.com>
  This file is part of GlusterFS.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/


/*
  TODO:
  - set proper pid/lk_owner to call frames (currently buried in syncop)
  - fix logging.c/h to store logfp and loglevel in glusterfs_ctx_t and
    reach it via THIS.
  - cleanup libglusterfs and eliminate _ALL_ glusterfs_ctx_get() calls.
  - fd migration on graph switch.
*/

#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>
#include <sys/types.h>
#include <unistd.h>
#include <limits.h>

#ifndef _CONFIG_H
#define _CONFIG_H
#include "config.h"
#endif

#include "glusterfs.h"
#include "logging.h"
#include "stack.h"
#include "event.h"
#include "glfs-mem-types.h"
#include "common-utils.h"
#include "syncop.h"
#include "call-stub.h"

#include "glfs-internal.h"


static gf_boolean_t
vol_assigned (cmd_args_t *args)
{
	return args->volfile || args->volfile_server;
}


static char *
generate_uuid ()
{
	char	       tmp_str[1024] = {0,};
	char	       hostname[256] = {0,};
	struct timeval tv = {0,};
	char	       now_str[32];

	if (gettimeofday (&tv, NULL) == -1) {
		gf_log ("glfs", GF_LOG_ERROR,
			"gettimeofday: failed %s",
			strerror (errno));
	}

	if (gethostname (hostname, sizeof hostname) == -1) {
		gf_log ("glfs", GF_LOG_ERROR,
			"gethostname: failed %s",
			strerror (errno));
	}

	gf_time_fmt (now_str, sizeof now_str, tv.tv_sec, gf_timefmt_Ymd_T);
	snprintf (tmp_str, sizeof tmp_str, "%s-%d-%s:%" GF_PRI_SUSECONDS,
		  hostname, getpid(), now_str, tv.tv_usec);

	return gf_strdup (tmp_str);
}


static int
glusterfs_ctx_defaults_init (glusterfs_ctx_t *ctx)
{
	call_pool_t   *pool = NULL;
	int	       ret = -1;
	cmd_args_t    *cmd_args = NULL;

	xlator_mem_acct_init (THIS, glfs_mt_end);

	cmd_args = &ctx->cmd_args;
	cmd_args->fuse_attribute_timeout   = 1.0;
	cmd_args->fuse_entry_timeout	   = 1.0;

	ctx->process_uuid = generate_uuid ();
	if (!ctx->process_uuid) {
		goto err;
	}

	ctx->page_size	= 128 * GF_UNIT_KB;

	ctx->iobuf_pool = iobuf_pool_new ();
	if (!ctx->iobuf_pool) {
		goto err;
	}

	ctx->event_pool = event_pool_new (DEFAULT_EVENT_POOL_SIZE);
	if (!ctx->event_pool) {
		goto err;
	}

	ctx->env = syncenv_new (0);
	if (!ctx->env) {
		goto err;
	}

	pool = GF_CALLOC (1, sizeof (call_pool_t),
			  glfs_mt_call_pool_t);
	if (!pool) {
		goto err;
	}

	/* frame_mem_pool size 112 * 4k */
	pool->frame_mem_pool = mem_pool_new (call_frame_t, 4096);
	if (!pool->frame_mem_pool) {
		goto err;
	}
	/* stack_mem_pool size 256 * 1024 */
	pool->stack_mem_pool = mem_pool_new (call_stack_t, 1024);
	if (!pool->stack_mem_pool) {
		goto err;
	}

	ctx->stub_mem_pool = mem_pool_new (call_stub_t, 1024);
	if (!ctx->stub_mem_pool) {
		goto err;
	}

	ctx->dict_pool = mem_pool_new (dict_t, GF_MEMPOOL_COUNT_OF_DICT_T);
	if (!ctx->dict_pool)
		goto err;

	ctx->dict_pair_pool = mem_pool_new (data_pair_t,
					    GF_MEMPOOL_COUNT_OF_DATA_PAIR_T);
	if (!ctx->dict_pair_pool)
		goto err;

	ctx->dict_data_pool = mem_pool_new (data_t, GF_MEMPOOL_COUNT_OF_DATA_T);
	if (!ctx->dict_data_pool)
		goto err;

	INIT_LIST_HEAD (&pool->all_frames);
	INIT_LIST_HEAD (&ctx->cmd_args.xlator_options);
	LOCK_INIT (&pool->lock);
	ctx->pool = pool;

	pthread_mutex_init (&(ctx->lock), NULL);

	ret = 0;
err:
	if (ret && pool) {
		if (pool->frame_mem_pool)
			mem_pool_destroy (pool->frame_mem_pool);
		if (pool->stack_mem_pool)
			mem_pool_destroy (pool->stack_mem_pool);
		GF_FREE (pool);
	}

	if (ret && ctx) {
		if (ctx->stub_mem_pool)
			mem_pool_destroy (ctx->stub_mem_pool);
		if (ctx->dict_pool)
			mem_pool_destroy (ctx->dict_pool);
		if (ctx->dict_data_pool)
			mem_pool_destroy (ctx->dict_data_pool);
		if (ctx->dict_pair_pool)
			mem_pool_destroy (ctx->dict_pair_pool);
	}

	return ret;
}


static int
logging_init (glusterfs_ctx_t *ctx)
{
	cmd_args_t *cmd_args = NULL;
	int	    ret = 0;

	cmd_args = &ctx->cmd_args;

	if (cmd_args->log_file == NULL) {
		ret = gf_asprintf (&cmd_args->log_file, "/dev/stderr");
		if (ret == -1) {
			fprintf (stderr, "failed to set the log file path\n");
			return -1;
		}
	}

	if (gf_log_init (cmd_args->log_file) == -1) {
		fprintf (stderr, "ERROR: failed to open logfile %s\n",
			 cmd_args->log_file);
		return -1;
	}

	gf_log_set_loglevel (cmd_args->log_level);

	return 0;
}


static int
create_master (struct glfs *fs)
{
	int		 ret = 0;
	cmd_args_t	*cmd_args = NULL;
	xlator_t	*master = NULL;

	cmd_args = &fs->ctx->cmd_args;

	master = GF_CALLOC (1, sizeof (*master),
			    glfs_mt_xlator_t);
	if (!master)
		goto err;

	master->name = gf_strdup ("gfapi");
	if (!master->name)
		goto err;

	if (xlator_set_type (master, "mount/api") == -1) {
		gf_log ("glfs", GF_LOG_ERROR,
			"master xlator for %s initialization failed",
			fs->volname);
		goto err;
	}

	master->ctx	 = fs->ctx;
	master->private	 = fs;
	master->options	 = get_new_dict ();
	if (!master->options)
		goto err;

	if (cmd_args->fuse_attribute_timeout >= 0) {
		ret = dict_set_double (master->options, ZR_ATTR_TIMEOUT_OPT,
				       cmd_args->fuse_attribute_timeout);

		if (ret < 0) {
			gf_log ("glfs", GF_LOG_ERROR,
				"failed to set dict value for key %s",
				ZR_ATTR_TIMEOUT_OPT);
			goto err;
		}
	}

	if (cmd_args->fuse_entry_timeout >= 0) {
		ret = dict_set_double (master->options, ZR_ENTRY_TIMEOUT_OPT,
				       cmd_args->fuse_entry_timeout);
		if (ret < 0) {
			gf_log ("glfs", GF_LOG_ERROR,
				"failed to set dict value for key %s",
				ZR_ENTRY_TIMEOUT_OPT);
			goto err;
		}
	}

	ret = xlator_init (master);
	if (ret) {
		gf_log ("glfs", GF_LOG_ERROR,
			"failed to initialize fuse translator");
		goto err;
	}

	fs->ctx->master = master;
	THIS = master;

	return 0;

err:
	if (master) {
		xlator_destroy (master);
	}

	return -1;
}


static FILE *
get_volfp (struct glfs *fs)
{
	int	     ret = 0;
	cmd_args_t  *cmd_args = NULL;
	FILE	    *specfp = NULL;
	struct stat  statbuf;

	cmd_args = &fs->ctx->cmd_args;

	ret = lstat (cmd_args->volfile, &statbuf);
	if (ret == -1) {
		gf_log ("glfs", GF_LOG_ERROR,
			"%s: %s", cmd_args->volfile, strerror (errno));
		return NULL;
	}

	if ((specfp = fopen (cmd_args->volfile, "r")) == NULL) {
		gf_log ("glfs", GF_LOG_ERROR,
			"volume file %s: %s",
			cmd_args->volfile,
			strerror (errno));
		return NULL;
	}

	gf_log ("glfs", GF_LOG_DEBUG,
		"loading volume file %s", cmd_args->volfile);

	return specfp;
}


int
glfs_volumes_init (struct glfs *fs)
{
	FILE		   *fp = NULL;
	cmd_args_t	   *cmd_args = NULL;
	int		    ret = 0;

	cmd_args = &fs->ctx->cmd_args;

	if (!vol_assigned (cmd_args))
		return -1;

	if (cmd_args->volfile_server) {
		ret = glfs_mgmt_init (fs);
		goto out;
	}

	fp = get_volfp (fs);

	if (!fp) {
		gf_log ("glfs", GF_LOG_ERROR,
			"Cannot reach volume specification file");
		ret = -1;
		goto out;
	}

	ret = glfs_process_volfp (fs, fp);
	if (ret)
		goto out;

out:
	return ret;
}


///////////////////////////////////////////////////////////////////////////////

void
__glfs_entry_fs (struct glfs *fs)
{
	THIS = fs->ctx->master;
}


void
__glfs_entry_fd (struct glfs_fd *fd)
{
	THIS = fd->fd->inode->table->xl->ctx->master;
}


void
glfs_fd_destroy (struct glfs_fd *glfd)
{
	if (!glfd)
		return;
	if (glfd->fd)
		fd_unref (glfd->fd);
	GF_FREE (glfd);
}

xlator_t *
glfs_fd_subvol (struct glfs_fd *glfd)
{
	xlator_t    *subvol = NULL;

	if (!glfd)
		return NULL;

	subvol = glfd->fd->inode->table->xl;

	return subvol;
}


xlator_t *
glfs_active_subvol (struct glfs *fs)
{
	xlator_t      *subvol = NULL;
	inode_table_t *itable = NULL;

	pthread_mutex_lock (&fs->mutex);
	{
		while (!fs->init)
			pthread_cond_wait (&fs->cond, &fs->mutex);

		subvol = fs->active_subvol;
	}
	pthread_mutex_unlock (&fs->mutex);

	if (!subvol)
		return NULL;

	if (!subvol->itable) {
		itable = inode_table_new (0, subvol);
		if (!itable) {
			errno = ENOMEM;
			return NULL;
		}

		subvol->itable = itable;

		glfs_first_lookup (subvol);
	}

	return subvol;
}


static void *
glfs_poller (void *data)
{
	struct glfs  *fs = NULL;

	fs = data;

	event_dispatch (fs->ctx->event_pool);

	return NULL;
}


struct glfs *
glfs_new (const char *volname)
{
	struct glfs   *fs = NULL;
	int	       ret = -1;

	ret = glusterfs_globals_init ();
	if (ret)
		return NULL;

	fs = GF_CALLOC (1, sizeof (*fs), glfs_mt_glfs_t);
	if (!fs)
		return NULL;

	fs->ctx = glusterfs_ctx_get ();
	if (!fs->ctx) {
		return NULL;
	}

	ret = glusterfs_ctx_defaults_init (fs->ctx);
	if (ret)
		return NULL;

	fs->ctx->cmd_args.volfile_id = gf_strdup (volname);

	fs->volname = gf_strdup (volname);

	pthread_mutex_init (&fs->mutex, NULL);
	pthread_cond_init (&fs->cond, NULL);

	return fs;
}


int
glfs_set_volfile (struct glfs *fs, const char *volfile)
{
	cmd_args_t  *cmd_args = NULL;

	cmd_args = &fs->ctx->cmd_args;

	if (vol_assigned (cmd_args))
		return -1;

	cmd_args->volfile = gf_strdup (volfile);

	return 0;
}


int
glfs_set_volfile_server (struct glfs *fs, const char *transport,
			 const char *host, int port)
{
	cmd_args_t  *cmd_args = NULL;

	cmd_args = &fs->ctx->cmd_args;

	if (vol_assigned (cmd_args))
		return -1;

	cmd_args->volfile_server = gf_strdup (host);
	cmd_args->volfile_server_transport = gf_strdup (transport);
	cmd_args->max_connect_attempts = 2;

	return 0;
}


int
glfs_set_logging (struct glfs *fs, const char *logfile, int loglevel)
{
	cmd_args_t  *cmd_args = NULL;

	cmd_args = &fs->ctx->cmd_args;

	cmd_args->log_level = loglevel;
	cmd_args->log_file = gf_strdup (logfile);

	return 0;
}


int
glfs_init_wait (struct glfs *fs)
{
	int   ret = -1;

	pthread_mutex_lock (&fs->mutex);
	{
		while (!fs->init)
			pthread_cond_wait (&fs->cond,
					   &fs->mutex);
		ret = fs->ret;
	}
	pthread_mutex_unlock (&fs->mutex);

	return ret;
}


void
glfs_init_done (struct glfs *fs, int ret)
{
	if (fs->init_cbk) {
		fs->init_cbk (fs, ret);
		return;
	}

	pthread_mutex_lock (&fs->mutex);
	{
		fs->init = 1;
		fs->ret = ret;

		pthread_cond_broadcast (&fs->cond);
	}
	pthread_mutex_unlock (&fs->mutex);
}


int
glfs_init_common (struct glfs *fs)
{
	int  ret = -1;

	logging_init (fs->ctx);

	ret = create_master (fs);
	if (ret)
		return ret;

	ret = pthread_create (&fs->poller, NULL, glfs_poller, fs);
	if (ret)
		return ret;

	ret = glfs_volumes_init (fs);
	if (ret)
		return ret;

	return ret;
}


int
glfs_init_async (struct glfs *fs, glfs_init_cbk cbk)
{
	int  ret = -1;

	fs->init_cbk = cbk;

	ret = glfs_init_common (fs);

	return ret;
}


int
glfs_init (struct glfs *fs)
{
	int  ret = -1;

	ret = glfs_init_common (fs);
	if (ret)
		return ret;

	ret = glfs_init_wait (fs);

	return ret;
}


struct glfs_fd *
glfs_open (struct glfs *fs, const char *path, int flags)
{
	int              ret = -1;
	struct glfs_fd  *glfd = NULL;
	xlator_t        *subvol = NULL;
	loc_t            loc = {0, };
	struct iatt      iatt = {0, };

	__glfs_entry_fs (fs);

	subvol = glfs_active_subvol (fs);
	if (!subvol) {
		ret = -1;
		errno = EIO;
		goto out;
	}

	glfd = GF_CALLOC (1, sizeof (*glfd), glfs_mt_glfs_fd_t);
	if (!glfd)
		goto out;

	ret = glfs_resolve (fs, subvol, path, &loc, &iatt);
	if (ret)
		goto out;

	if (IA_ISDIR (iatt.ia_type)) {
		ret = -1;
		errno = EISDIR;
		goto out;
	}

	if (!IA_ISREG (iatt.ia_type)) {
		ret = -1;
		errno = EINVAL;
		goto out;
	}

	glfd->fd = fd_create (loc.inode, getpid());
	if (!glfd->fd) {
		ret = -1;
		errno = ENOMEM;
		goto out;
	}

	ret = syncop_open (subvol, &loc, flags, glfd->fd);
out:
	loc_wipe (&loc);

	if (ret && glfd) {
		glfs_fd_destroy (glfd);
		glfd = NULL;
	}

	return glfd;
}


int
glfs_close (struct glfs_fd *glfd)
{
	xlator_t  *subvol = NULL;
	int        ret = -1;

	__glfs_entry_fd (glfd);

	subvol = glfs_fd_subvol (glfd);

	ret = syncop_fsync (subvol, glfd->fd);

	glfs_fd_destroy (glfd);

	return ret;
}


int
glfs_lstat (struct glfs *fs, const char *path, struct stat *stat)
{
	int              ret = -1;
	xlator_t        *subvol = NULL;
	loc_t            loc = {0, };
	struct iatt      iatt = {0, };

	__glfs_entry_fs (fs);

	subvol = glfs_active_subvol (fs);
	if (!subvol) {
		ret = -1;
		errno = EIO;
		goto out;
	}

	ret = glfs_resolve (fs, subvol, path, &loc, &iatt);

	if (ret == 0 && stat)
		iatt_to_stat (&iatt, stat);
out:
	loc_wipe (&loc);

	return ret;
}


struct glfs_fd *
glfs_creat (struct glfs *fs, const char *path, int flags, mode_t mode)
{
	int              ret = -1;
	struct glfs_fd  *glfd = NULL;
	xlator_t        *subvol = NULL;
	loc_t            loc = {0, };
	struct iatt      iatt = {0, };
	uuid_t           gfid;
	dict_t          *xattr_req = NULL;

	__glfs_entry_fs (fs);

	subvol = glfs_active_subvol (fs);
	if (!subvol) {
		ret = -1;
		errno = EIO;
		goto out;
	}

	xattr_req = dict_new ();
	if (!xattr_req) {
		ret = -1;
		errno = ENOMEM;
		goto out;
	}

	uuid_generate (gfid);
	ret = dict_set_static_bin (xattr_req, "gfid-req", gfid, 16);
	if (ret) {
		ret = -1;
		errno = ENOMEM;
		goto out;
	}

	glfd = GF_CALLOC (1, sizeof (*glfd), glfs_mt_glfs_fd_t);
	if (!glfd)
		goto out;

	ret = glfs_resolve (fs, subvol, path, &loc, &iatt);
	if (ret == -1 && errno != ENOENT)
		/* Any other type of error is fatal */
		goto out;

	if (ret == -1 && errno == ENOENT && !loc.parent)
		/* The parent directory or an ancestor even
		   higher does not exist
		*/
		goto out;

	if (loc.inode) {
		if (flags & O_EXCL) {
			ret = -1;
			errno = EEXIST;
			goto out;
		}

		if (IA_ISDIR (iatt.ia_type)) {
			ret = -1;
			errno = EISDIR;
			goto out;
		}

		if (!IA_ISREG (iatt.ia_type)) {
			ret = -1;
			errno = EINVAL;
			goto out;
		}
	}

	if (ret == -1 && errno == ENOENT) {
		loc.inode = inode_new (loc.parent->table);
		if (!loc.inode) {
			ret = -1;
			errno = ENOMEM;
			goto out;
		}
	}

	glfd->fd = fd_create (loc.inode, getpid());
	if (!glfd->fd) {
		ret = -1;
		errno = ENOMEM;
		goto out;
	}

	ret = syncop_create (subvol, &loc, flags, mode, glfd->fd, xattr_req);
out:
	loc_wipe (&loc);

	if (xattr_req)
		dict_destroy (xattr_req);

	if (ret && glfd) {
		glfs_fd_destroy (glfd);
		glfd = NULL;
	}

	return glfd;
}
