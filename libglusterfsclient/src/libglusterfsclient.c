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

#include "libglusterfsclient.h"
#include "libglusterfsclient-mem-types.h"
#include "libglusterfsclient-internals.h"

#include "glusterfs.h"
#include "xlator.h"
#include "event.h"
#include "call-stub.h"
#include "logging.h"

#include <stdio.h>
#include <sys/time.h>
#include <sys/resource.h>

#define DEFAULT_EVENT_POOL_SIZE            16384


void *
glfs_poll_thread (void *data)
{
        struct glfs_session *session = data;
        glusterfs_ctx_t     *ctx = NULL;
        int                  ret = 0;

        ctx = session->ctx;

        ret = event_dispatch (ctx->event_pool);

        return NULL;
}


static char *
generate_uuid ()
{
        char           tmp_str[1024] = {0,};
        char           hostname[256] = {0,};
        struct timeval tv = {0,};
        struct tm      now = {0, };
        char           now_str[32];

        if (gettimeofday (&tv, NULL) == -1) {
                gf_log ("glusterfsd", GF_LOG_ERROR,
                        "gettimeofday: failed %s",
                        strerror (errno));
        }

        if (gethostname (hostname, 256) == -1) {
                gf_log ("glusterfsd", GF_LOG_ERROR,
                        "gethostname: failed %s",
                        strerror (errno));
        }

        localtime_r (&tv.tv_sec, &now);
        strftime (now_str, 32, "%Y/%m/%d-%H:%M:%S", &now);
        snprintf (tmp_str, 1024, "%s-%d-%s:%" GF_PRI_SUSECONDS,
                  hostname, getpid(), now_str, tv.tv_usec);

        return gf_strdup (tmp_str);
}


static int
glusterfs_ctx_defaults_init (glusterfs_ctx_t *ctx)
{
        cmd_args_t    *cmd_args = NULL;
        struct rlimit  lim = {0, };
        call_pool_t   *pool = NULL;

        xlator_mem_acct_init (THIS, glfs_mt_end);

        ctx->process_uuid = generate_uuid ();
        if (!ctx->process_uuid)
                return -1;

        ctx->page_size  = 128 * GF_UNIT_KB;

        ctx->iobuf_pool = iobuf_pool_new (8 * GF_UNIT_MB, ctx->page_size);
        if (!ctx->iobuf_pool)
                return -1;

        ctx->event_pool = event_pool_new (DEFAULT_EVENT_POOL_SIZE);
        if (!ctx->event_pool)
                return -1;

        pool = GF_CALLOC (1, sizeof (call_pool_t),
                          glfs_mt_call_pool_t);
        if (!pool)
                return -1;

        /* frame_mem_pool size 112 * 16k */
        pool->frame_mem_pool = mem_pool_new (call_frame_t, 16384);

        if (!pool->frame_mem_pool)
                return -1;

        /* stack_mem_pool size 256 * 8k */
        pool->stack_mem_pool = mem_pool_new (call_stack_t, 8192);

        if (!pool->stack_mem_pool)
                return -1;

        ctx->stub_mem_pool = mem_pool_new (call_stub_t, 1024);
        if (!ctx->stub_mem_pool)
                return -1;

        INIT_LIST_HEAD (&pool->all_frames);
        LOCK_INIT (&pool->lock);
        ctx->pool = pool;

        pthread_mutex_init (&(ctx->lock), NULL);

        cmd_args = &ctx->cmd_args;
        INIT_LIST_HEAD (&cmd_args->xlator_options);

        lim.rlim_cur = RLIM_INFINITY;
        lim.rlim_max = RLIM_INFINITY;
        setrlimit (RLIMIT_CORE, &lim);

        return 0;
}


static int
logging_init (glusterfs_ctx_t *ctx)
{
        if (gf_log_init ("/dev/stderr") == -1) {
                fprintf (stderr,
                         "failed to open logfile %s.  exiting\n",
                         "/dev/stderr");
                return -1;
        }

        gf_log_set_loglevel (GF_LOG_NORMAL);

        return 0;
}


glusterfs_ctx_t *
glusterfs_ctx_new ()
{
        glusterfs_ctx_t     *ctx = NULL;
        int                  ret = -1;

        ret = glusterfs_globals_init ();
        if (ret)
                return NULL;

        ctx = glusterfs_ctx_get ();
        if (!ctx)
                return NULL;

        ret = glusterfs_ctx_defaults_init (ctx);
        if (ret)
                return NULL;

        ret = logging_init (ctx);
        if (ret)
                return NULL;

        return ctx;
}


struct glfs_session *
glfs_session_new (void)
{
        struct glfs_session *session = NULL;
        glusterfs_ctx_t     *ctx = NULL;

        session = GF_CALLOC (1, sizeof (*session), glfs_mt_session_t);
        if (!session)
                return NULL;

        ctx = glusterfs_ctx_new ();
        if (!ctx)
                return NULL;

        session->ctx  = ctx;

        glfs_session_set_master (session);

        return session;
}


glusterfs_graph_t *
glusterfs_graph_from_file (const char *volfile)
{
        int                 ret = -1;
        FILE               *specfp = NULL;
        glusterfs_graph_t  *graph = NULL;
        struct stat         statbuf = {0, };

        ret = lstat (volfile, &statbuf);
        if (ret == -1) {
                gf_log ("libgluterfsclient", GF_LOG_ERROR,
                        "%s: %s", volfile, strerror (errno));
                goto out;
        }

        if ((specfp = fopen (volfile, "r")) == NULL) {
                gf_log ("libglusterfsclient", GF_LOG_ERROR,
                        "volume file %s: %s",
                        volfile, strerror (errno));
                goto out;
        }

        graph = glusterfs_graph_construct (specfp);

out:
        if (specfp)
                fclose (specfp);

        return graph;
}


int
glfs_session_init_volfile (struct glfs_session *session, const char *volfile)
{
        int                 ret = -1;
        glusterfs_graph_t  *graph = NULL;
        glusterfs_ctx_t    *ctx = NULL;


        ctx = session->ctx;

        graph = glusterfs_graph_from_file (volfile);
        if (!graph) {
                ret = -1;
                goto out;
        }

        ret = glusterfs_graph_prepare (graph, ctx);

        if (ret) {
                glusterfs_graph_destroy (graph);
                ret = -1;
                goto out;
        }

        ret = glusterfs_graph_activate (graph, ctx);

        if (ret) {
                glusterfs_graph_destroy (graph);
                ret = -1;
                goto out;
        }

        ret = pthread_create (&session->pollthread, NULL, glfs_poll_thread,
                              session);

        if (ret) {
                glusterfs_graph_destroy (graph);
                ret = -1;
                goto out;
        }

out:
        if (!ctx->active) {
                /* there is some error in setting up the first graph itself */
                return -1;
        }

        if (ret)
                return -1;

        return 0;
}

