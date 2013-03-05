/*
  Copyright (c) 2013 Red Hat, Inc. <http://www.redhat.com>
  This file is part of GlusterFS.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/


#ifndef _CONFIG_H
#define _CONFIG_H
#include "config.h"
#endif

#include "glusterfs.h"
#include "logging.h"
#include "dict.h"
#include "xlator.h"
#include "inode.h"
#include "call-stub.h"
#include "defaults.h"
#include "qemu-block-memory-types.h"
#include "qemu-block.h"
#include "qb-coroutines.h"


int
qb_format_and_resume (void *opaque)
{
	CoroutineSynctask *cs = NULL;
	qb_local_t *local = NULL;
	call_frame_t *frame = NULL;
	call_stub_t *stub = NULL;
	inode_t *inode = NULL;
	char filename[64];
	qb_inode_t *qb_inode = NULL;
	Error *local_err = NULL;

	cs = opaque;

	local = DO_UPCAST(qb_local_t, cs, cs);
	frame = local->frame;
	stub = local->stub;
	inode = local->inode;

	qb_inode_to_filename (inode, filename, 64);

	qb_inode = qb_inode_ctx_get (frame->this, inode);

	bdrv_img_create (filename, qb_inode->fmt, 0, 0,
			 0, qb_inode->size, 0, &local_err, true);

	if (error_is_set (&local_err)) {
		gf_log (frame->this->name, GF_LOG_ERROR, "%s",
			error_get_pretty (local_err));
		error_free (local_err);
		QB_STUB_UNWIND (stub, -1, EIO);
		return 0;
	}

	QB_STUB_UNWIND (stub, 0, 0);

	return 0;
}


int
qb_co_open (void *opaque)
{
	CoroutineSynctask *cs = NULL;
	qb_local_t *local = NULL;
	call_frame_t *frame = NULL;
	call_stub_t *stub = NULL;
	inode_t *inode = NULL;
	char filename[64];
	qb_inode_t *qb_inode = NULL;
	BlockDriverState *bs = NULL;
	BlockDriver *drv = NULL;
	int ret = 0;
	int op_errno = 0;

	cs = opaque;

	local = DO_UPCAST(qb_local_t, cs, cs);
	frame = local->frame;
	stub = local->stub;
	inode = local->inode;

	qb_inode = qb_inode_ctx_get (frame->this, inode);
	if (qb_inode->bs) {
		/* FIXME: we need locks around this when
		   enabling multithreaded syncop/coroutine
		   for qemu-block
		*/
		qb_inode->refcnt++;
		goto out;
	}

	bs = bdrv_new (uuid_utoa (inode->gfid));
	if (!bs) {
		op_errno = ENOMEM;
		gf_log (THIS->name, GF_LOG_ERROR,
			"could not allocate @bdrv for gfid:%s",
			uuid_utoa (inode->gfid));
		goto err;
	}

	drv = bdrv_find_format (qb_inode->fmt);
	if (!drv) {
		op_errno = EINVAL;
		gf_log (THIS->name, GF_LOG_ERROR,
			"Unknown file format: %s for gfid:%s",
			qb_inode->fmt, uuid_utoa (inode->gfid));
		goto err;
	}

	qb_inode_to_filename (inode, filename, 64);

	ret = bdrv_open (bs, filename, BDRV_O_RDWR, drv);
	if (ret < 0) {
		op_errno = -ret;
		gf_log (THIS->name, GF_LOG_ERROR,
			"Unable to bdrv_open() gfid:%s (%s)",
			uuid_utoa (inode->gfid), strerror (op_errno));
		goto err;
	}

	qb_inode->refcnt = 1;
	qb_inode->bs = bs;
out:
	QB_STUB_RESUME (stub);

	return 0;
err:
	QB_STUB_UNWIND (stub, -1, op_errno);
	return 0;
}


int
qb_co_close (void *opaque)
{
	CoroutineSynctask *cs = NULL;
	qb_local_t *local = NULL;
	call_frame_t *frame = NULL;
	inode_t *inode = NULL;
	qb_inode_t *qb_inode = NULL;
	BlockDriverState *bs = NULL;

	local = DO_UPCAST(qb_local_t, cs, cs);
	inode = local->inode;

	qb_inode = qb_inode_ctx_get (THIS, inode);

	if (!--qb_inode->refcnt) {
		bs = qb_inode->bs;
		qb_inode->bs = NULL;
		bdrv_delete (bs);
	}

	frame = local->frame;
	frame->local = NULL;
	qb_local_free (THIS, local);
	STACK_DESTROY (frame->root);

	return 0;
}
