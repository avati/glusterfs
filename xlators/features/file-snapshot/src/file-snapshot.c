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

/*


*/

enum fs_file_mode {
	FS_FILE_MODE_ROOT, /* Represents the root of a snapshot family
			      relation tree.
			   */
	FS_FILE_MODE_SNAP, /* Represents a read-only SNAP, which is
			      a backing file for at least one (or more)
			      other SNAPs or HEADs.

			      A SNAP cannot be accessed directly (it is
			      never mapped to a user-visible inode). In
			      order to access a SNAP, a HEAD has to be
			      created with the desired SNAP as the
			      backing file.
			   */

	FS_FILE_MODE_HEAD, /* Represents a read-write HEAD, which is
			      a user visible and editable.

			      A HEAD is associated with one and only one
			      user-visible inode. A head ALWAYS has a backing
			      SNAP file. A HEAD is a logical file,
			      and requires the entire ancestry of SNAPs
			      all the way up to the ROOT SNAP in order to
			      serve all possible READ() requests.
			   */
};


typedef struct fs_local {
	call_stub_t *stub;
} fs_local_t;


typedef struct fs_inode {
	inode_t *backing;
} fs_inode_t;


#define FS_STACK_UNWIND(type, frame ...) do {	\
	fs_local_t *__local = NULL;		\
	__local = frame->local;			\
	frame->local = NULL;			\
	STACK_UNWIND_STRICT(type, frame);	\
	fs_local_destroy (local, THIS);		\
	} while (0)


static void
fs_local_destroy (fs_local_t *local, xlator_t *this)
{
	if (!local)
		return;

	GF_FREE (local);
}


static int
fs_lookup_cbk (call_frame_t *frame, void *cookie, xlator_t *this, int op_ret,
	       int op_errno, inode_t *inode, struct iatt *iatt, dict_t *xdata,
	       struct iatt *postparent)
{
	FS_STACK_UNWIND (lookup, frame, op_ret, op_errno, inode, iatt,
			 xdata, postparent);
	return 0;
}


static int
fs_lookup (call_frame_t *frame, xlator_t *this, loc_t *loc, dict_t *xdata)
{
	STACK_WIND (frame, fs_lookup_cbk, FIRST_CHILD (this),
		    FIRST_CHILD (this)->fops->lookup, loc, xdata);
	return 0;
}


int
init (xlator_t *this)
{
        if (!this->children || this->children->next) {
                gf_log (this->name, GF_LOG_ERROR,
                        "not configured with exactly one child. exiting");
                return -1;
        }

        if (!this->parents) {
                gf_log (this->name, GF_LOG_WARNING,
                        "dangling volume. check volfile ");
        }

        return 0;
}


void
fini (xlator_t *this)
{
        return;
}


struct xlator_fops fops = {
	.lookup = fs_lookup,
};


struct xlator_cbks cbks = {
};


struct volume_options options[] = {
        { .key  = {NULL} },
};
