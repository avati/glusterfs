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


qb_inode_t *
__qb_inode_ctx_get (xlator_t *this, inode_t *inode)
{
        uint64_t    value    = 0;
        qb_inode_t *qb_inode = NULL;

        __inode_ctx_get (inode, this, &value);
        qb_inode = (qb_inode_t *)(unsigned long) value;

        return qb_inode;
}


qb_inode_t *
qb_inode_ctx_get (xlator_t *this, inode_t *inode)
{
        qb_inode_t *qb_inode = NULL;

        LOCK (&inode->lock);
        {
                qb_inode = __qb_inode_ctx_get (this, inode);
        }
        UNLOCK (&inode->lock);

        return qb_inode;
}


qb_inode_t *
qb_inode_ctx_del (xlator_t *this, inode_t *inode)
{
        uint64_t    value    = 0;
        qb_inode_t *qb_inode = NULL;

        inode_ctx_del (inode, this, &value);
        qb_inode = (qb_inode_t *)(unsigned long) value;

        return qb_inode;
}


int
qb_inode_cleanup (xlator_t *this, inode_t *inode, int warn)
{
	qb_inode_t *qb_inode = NULL;

	qb_inode = qb_inode_ctx_del (this, inode);

	if (!qb_inode)
		return 0;

	if (warn)
		gf_log (this->name, GF_LOG_WARNING,
			"inode %s no longer block formatted",
			uuid_utoa (inode->gfid));

	/* free (qb_inode->bs); */

	GF_FREE (qb_inode);

	return 0;
}


int
qb_iatt_fixup (xlator_t *this, inode_t *inode, struct iatt *iatt)
{
	qb_inode_t *qb_inode = NULL;

	qb_inode = qb_inode_ctx_get (this, inode);
	if (!qb_inode)
		return 0;

	iatt->ia_size = qb_inode->size;

	return 0;
}


int
qb_format_extract (xlator_t *this, char *format, inode_t *inode)
{
	char       *s = NULL;
	uint64_t    size = 0;
	char        fmt[QB_XATTR_VAL_MAX+1] = {0, };
	qb_inode_t *qb_inode = NULL;

	strncpy (fmt, format, QB_XATTR_VAL_MAX);
	s = strchr (fmt, ':');
	if (!s)
		goto invalid;
	if (s == fmt)
		goto invalid;

	*s = 0; s++;
	if (!*s || strchr (s, ':'))
		goto invalid;

	if (gf_string2bytesize (s, &size))
		goto invalid;

	if (!size)
		goto invalid;

	qb_inode = qb_inode_ctx_get (this, inode);
	if (!qb_inode)
		qb_inode = GF_CALLOC (1, sizeof (*qb_inode),
				      gf_qb_mt_qb_inode_t);
	if (!qb_inode)
		return ENOMEM;

	strncpy (qb_inode->fmt, fmt, QB_XATTR_VAL_MAX);
	qb_inode->size = size;
	qb_inode->size_str = s;

	inode_ctx_set (inode, this, (void *)&qb_inode);
	return 0;
invalid:
	gf_log (this->name, GF_LOG_WARNING,
		"invalid format '%s' in inode %s", format,
		uuid_utoa (inode->gfid));
	return EINVAL;
}


void
qb_local_free (xlator_t *this, qb_local_t *local)
{
	if (local->inode)
		inode_unref (local->inode);
	if (local->fd)
		fd_unref (local->fd);
	GF_FREE (local);
}


int
qb_local_init (call_frame_t *frame)
{
	qb_local_t *qb_local = NULL;

	qb_local = GF_CALLOC (1, sizeof (*qb_local), gf_qb_mt_qb_local_t);
	if (!qb_local)
		return -1;

	qb_local->frame = frame;
	frame->local = qb_local;

	return 0;
}


int
qb_lookup_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
	       int op_ret, int op_errno, inode_t *inode, struct iatt *buf,
	       dict_t *xdata, struct iatt *postparent)
{
	char *format = NULL;
	qb_conf_t *conf = NULL;

	conf = this->private;

	if (op_ret == -1)
		goto out;

	if (!xdata)
		goto out;

	if (dict_get_str (xdata, conf->qb_xattr_key, &format))
		goto out;

	if (!format) {
		qb_inode_cleanup (this, inode, 1);
		goto out;
	}

	op_errno = qb_format_extract (this, format, inode);
	if (op_errno)
		op_ret = -1;

	qb_iatt_fixup (this, inode, buf);
out:
	QB_STACK_UNWIND (lookup, frame, op_ret, op_errno, inode, buf,
			 xdata, postparent);
	return 0;
}


int
qb_lookup (call_frame_t *frame, xlator_t *this, loc_t *loc, dict_t *xdata)
{
	qb_conf_t *conf = NULL;

	conf = this->private;

	xdata = xdata ? dict_ref (xdata) : dict_new ();

	if (!xdata)
		goto enomem;

	if (dict_set_int32 (xdata, conf->qb_xattr_key, 0))
		goto enomem;

	STACK_WIND (frame, qb_lookup_cbk, FIRST_CHILD(this),
		    FIRST_CHILD(this)->fops->lookup, loc, xdata);
	dict_unref (xdata);
	return 0;
enomem:
	QB_STACK_UNWIND (lookup, frame, -1, ENOMEM, 0, 0, 0, 0);
	if (xdata)
		dict_unref (xdata);
	return 0;
}


int
qb_setxattr_common (call_frame_t *frame, xlator_t *this, call_stub_t *stub,
		    dict_t *xattr, inode_t *inode)
{
	char *format = NULL;
	int op_errno = 0;
	qb_local_t *qb_local = NULL;
	data_t *data = NULL;

	if (!(data = dict_get (xattr, "trusted.glusterfs.block-format"))) {
		QB_STUB_RESUME (stub);
		return 0;
	}

	format = alloca (data->len + 1);
	memcpy (format, data->data, data->len);
	format[data->len] = 0;

	op_errno = qb_format_extract (this, format, inode);
	if (op_errno) {
		QB_STUB_UNWIND (stub, -1, op_errno);
		return 0;
	}

	qb_local = frame->local;

	qb_local->stub = stub;
	qb_local->inode = inode_ref (inode);
	strncpy (qb_local->fmt, format, QB_XATTR_VAL_MAX);

	qb_coroutine (frame, qb_format_and_resume);

	return 0;
}


int
qb_setxattr (call_frame_t *frame, xlator_t *this, loc_t *loc, dict_t *xattr,
	     int flags, dict_t *xdata)
{
	call_stub_t *stub = NULL;

	if (qb_local_init (frame) != 0)
		goto enomem;

	stub = fop_setxattr_stub (frame, default_setxattr_resume, loc, xattr,
				  flags, xdata);
	if (!stub)
		goto enomem;

	qb_setxattr_common (frame, this, stub, xattr, loc->inode);

	return 0;
enomem:
	QB_STACK_UNWIND (setxattr, frame, -1, ENOMEM, 0);
	return 0;
}


int
qb_fsetxattr (call_frame_t *frame, xlator_t *this, fd_t *fd, dict_t *xattr,
	      int flags, dict_t *xdata)
{
	call_stub_t *stub = NULL;

	if (qb_local_init (frame) != 0)
		goto enomem;

	stub = fop_fsetxattr_stub (frame, default_fsetxattr_resume, fd, xattr,
				   flags, xdata);
	if (!stub)
		goto enomem;

	qb_setxattr_common (frame, this, stub, xattr, fd->inode);

	return 0;
enomem:
	QB_STACK_UNWIND (fsetxattr, frame, -1, ENOMEM, 0);
	return 0;
}


int
qb_open_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
	     int op_ret, int op_errno, fd_t *fd, dict_t *xdata)
{
	call_stub_t *stub = NULL;
	qb_local_t *qb_local = NULL;

	qb_local = frame->local;

	if (op_ret < 0)
		goto unwind;

	if (!qb_inode_ctx_get (this, qb_local->inode))
		goto unwind;

	stub = fop_open_cbk_stub (frame, NULL, op_ret, op_errno, fd, xdata);
	if (!stub) {
		op_ret = -1;
		op_errno = ENOMEM;
		goto unwind;
	}

	qb_local->stub = stub;

	qb_coroutine (frame, qb_co_open);

	return 0;
unwind:
	QB_STACK_UNWIND (open, frame, op_ret, op_errno, fd, xdata);
	return 0;
}


int
qb_open (call_frame_t *frame, xlator_t *this, loc_t *loc, int flags,
	 fd_t *fd, dict_t *xdata)
{
	qb_local_t *qb_local = NULL;
	qb_inode_t *qb_inode = NULL;

	qb_inode = qb_inode_ctx_get (this, loc->inode);
	if (!qb_inode) {
		STACK_WIND (frame, default_open_cbk, FIRST_CHILD(this),
			    FIRST_CHILD(this)->fops->open, loc, flags, fd,
			    xdata);
		return 0;
	}

	if (qb_local_init (frame) != 0)
		goto enomem;

	qb_local = frame->local;

	qb_local->inode = inode_ref (loc->inode);
	qb_local->fd = fd_ref (fd);

	STACK_WIND (frame, qb_open_cbk, FIRST_CHILD(this),
		    FIRST_CHILD(this)->fops->open, loc, flags, fd, xdata);
	return 0;
enomem:
	QB_STACK_UNWIND (open, frame, -1, ENOMEM, 0, 0);
	return 0;
}


int
qb_forget (xlator_t *this, inode_t *inode)
{
	return qb_inode_cleanup (this, inode, 0);
}


int
qb_release (xlator_t *this, fd_t *fd)
{
	call_frame_t *frame = NULL;

	frame = create_frame (this, this->ctx->pool);
	if (!frame) {
		gf_log (this->name, GF_LOG_ERROR,
			"Could not allocate frame. "
			"Leaking QEMU BlockDriverState");
		return -1;
	}

	if (qb_local_init (frame) != 0) {
		gf_log (this->name, GF_LOG_ERROR,
			"Could not allocate local. "
			"Leaking QEMU BlockDriverState");
		STACK_DESTROY (frame->root);
		return -1;
	}

	if (qb_coroutine (frame, qb_co_close) != 0) {
		gf_log (this->name, GF_LOG_ERROR,
			"Could not allocate coroutine. "
			"Leaking QEMU BlockDriverState");
		qb_local_free (this, frame->local);
		frame->local = NULL;
		STACK_DESTROY (frame->root);
	}

	return 0;
}

int
mem_acct_init (xlator_t *this)
{
        int     ret = -1;

        ret = xlator_mem_acct_init (this, gf_qb_mt_end + 1);

        if (ret)
                gf_log (this->name, GF_LOG_ERROR, "Memory accounting init "
                        "failed");
        return ret;
}


int
reconfigure (xlator_t *this, dict_t *options)
{
	return 0;
}


int
init (xlator_t *this)
{
        qb_conf_t *conf    = NULL;
        int32_t    ret     = -1;

        if (!this->children || this->children->next) {
                gf_log (this->name, GF_LOG_ERROR,
                        "FATAL: qemu-block (%s) not configured with exactly "
                        "one child", this->name);
                goto out;
        }

        conf = GF_CALLOC (1, sizeof (*conf), gf_qb_mt_qb_conf_t);
        if (!conf)
                goto out;

        /* configure 'option window-size <size>' */
        GF_OPTION_INIT ("default-password", conf->default_password, str, out);

	/* qemu coroutines use "co_mutex" for synchronizing among themselves.
	   However "co_mutex" itself is not threadsafe if the coroutine framework
	   is multithreaded (which usually is not). However synctasks are
	   fundamentally multithreaded, so for now create a syncenv which has
	   scaling limits set to max 1 thread so that the qemu coroutines can
	   execute "safely".

	   Future work: provide an implementation of "co_mutex" which is
	   threadsafe and use the global multithreaded ctx->env syncenv.
	*/
	conf->env = syncenv_new (0, 1, 1);

        this->private = conf;

        ret = 0;

	snprintf (conf->qb_xattr_key, QB_XATTR_KEY_MAX, QB_XATTR_KEY_FMT,
		  this->name);

	cur_mon = (void *) 1;

	bdrv_init ();

out:
        if (ret)
                GF_FREE (conf);

        return ret;
}


void
fini (xlator_t *this)
{
        qb_conf_t *conf = NULL;

        conf = this->private;

        this->private = NULL;

        GF_FREE (conf);

	return;
}


struct xlator_fops fops = {
	.lookup      = qb_lookup,
	.fsetxattr   = qb_fsetxattr,
	.setxattr    = qb_setxattr,
	.open        = qb_open,
/*
        .writev      = qb_writev,
        .readv       = qb_readv,
        .flush       = qb_flush,
        .fsync       = qb_fsync,
        .stat        = qb_stat,
        .fstat       = qb_fstat,
        .truncate    = qb_truncate,
        .ftruncate   = qb_ftruncate,
        .setattr     = qb_setattr,
        .fsetattr    = qb_fsetattr,
	.getxattr    = qb_getxattr,
	.fgetxattr   = qb_fgetxattr
*/
};


struct xlator_cbks cbks = {
        .forget   = qb_forget,
	.release  = qb_release,
};


struct xlator_dumpops dumpops = {
};


struct volume_options options[] = {
        { .key  = {"default-password"},
          .type = GF_OPTION_TYPE_STR,
          .default_value = "",
          .description = "Default password for the AES encrypted block images."
        },
        { .key = {NULL} },
};
