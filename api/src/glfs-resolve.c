/*
  Copyright (c) 2012 Red Hat, Inc. <http://www.redhat.com>
  This file is part of GlusterFS.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/


#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>
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


void
glfs_first_lookup (xlator_t *subvol)
{
	loc_t  loc = {0, };
	int    ret = -1;

	loc.inode = subvol->itable->root;
	memset (loc.gfid, 0, 16);
	loc.gfid[15] = 1;
	loc.path = "/";
	loc.name = "";

	ret = syncop_lookup (subvol, &loc, 0, 0, 0, 0);

	gf_log (subvol->name, GF_LOG_DEBUG, "first lookup complete %d", ret);

	return;
}


int
glfs_loc_touchup (loc_t *loc)
{
	char *path = NULL;
	int   ret = -1;
	char *bn = NULL;

	ret = inode_path (loc->parent, loc->name, &path);

	loc->path = path;

	if (ret < 0 || !path) {
		ret = -1;
		errno = ENOMEM;
		goto out;
	}

	bn = strrchr (path, '/');
	if (bn)
		bn++;
	loc->name = bn;
	ret = 0;
out:
	return ret;
}


inode_t *
glfs_resolve_component (struct glfs *fs, xlator_t *subvol, inode_t *parent,
			const char *component, struct iatt *iatt)
{
	loc_t        loc = {0, };
	inode_t     *inode = NULL;
	int          reval = 0;
	int          ret = -1;
	int          glret = -1;
	struct iatt  ciatt = {0, };
	uuid_t       gfid;
	dict_t      *xattr_req = NULL;

	loc.name = component;

	loc.parent = inode_ref (parent);
	uuid_copy (loc.pargfid, parent->gfid);

	xattr_req = dict_new ();
	if (!xattr_req) {
		errno = ENOMEM;
		goto out;
	}

	ret = dict_set_static_bin (xattr_req, "gfid-req", gfid, 16);
	if (ret) {
		errno = ENOMEM;
		goto out;
	}

	loc.inode = inode_grep (parent->table, parent, component);

	if (loc.inode) {
		uuid_copy (loc.gfid, loc.inode->gfid);
		reval = 1;
	} else {
		uuid_generate (gfid);
		loc.inode = inode_new (parent->table);
	}

	if (!loc.inode)
		goto out;


	glret = glfs_loc_touchup (&loc);
	if (glret < 0) {
		ret = -1;
		goto out;
	}

	ret = syncop_lookup (subvol, &loc, xattr_req, &ciatt, NULL, NULL);
	if (ret && reval) {
		inode_unref (loc.inode);
		loc.inode = inode_new (parent->table);
		if (!loc.inode)
			goto out;
		uuid_generate (gfid);
		ret = syncop_lookup (subvol, &loc, xattr_req, &ciatt,
				     NULL, NULL);
	}
	if (ret)
		goto out;

	inode = inode_link (loc.inode, loc.parent, component, &ciatt);
	if (inode)
		inode_lookup (inode);
	if (iatt)
		*iatt = ciatt;
out:
	if (xattr_req)
		dict_destroy (xattr_req);

	loc_wipe (&loc);

	return inode;
}


int
glfs_resolve (struct glfs *fs, xlator_t *subvol, const char *origpath,
	      loc_t *loc, struct iatt *iatt)
{
	inode_t    *inode = NULL;
	inode_t    *parent = NULL;
	char       *saveptr = NULL;
	char       *path = NULL;
	char       *component = NULL;
	char       *next_component = NULL;
	int         ret = -1;
	struct iatt ciatt = {0, };

	path = gf_strdup (origpath);
	if (!path) {
		errno = ENOMEM;
		return -1;
	}

	parent = NULL;
	inode = inode_ref (subvol->itable->root);

	for (component = strtok_r (path, "/", &saveptr);
	     component; component = next_component) {

		next_component = strtok_r (NULL, "/", &saveptr);

		if (parent)
			inode_unref (parent);

		parent = inode;

		inode = glfs_resolve_component (fs, subvol, parent,
						component, &ciatt);
		if (!inode)
			break;

		if (!next_component)
			break;

		if (!IA_ISDIR (ciatt.ia_type)) {
			/* next_component exists and this component is
			   not a directory
			*/
			inode_unref (inode);
			inode = NULL;
			ret = -1;
			errno = ENOTDIR;
			break;
		}
	}

	if (parent && next_component)
		/* resolution failed mid-way */
		goto out;

	/* At this point, all components up to the last parent directory
	   have been resolved successfully (@parent). Resolution of basename
	   might have failed (@inode) if at all.
	*/

	loc->parent = parent;
	if (parent) {
		uuid_copy (loc->pargfid, parent->gfid);
		loc->name = component;
	}

	loc->inode = inode;
	if (inode) {
		uuid_copy (loc->gfid, inode->gfid);
		if (iatt)
			*iatt = ciatt;
		ret = 0;
	}

	glfs_loc_touchup (loc);
out:
	GF_FREE (path);

	/* do NOT loc_wipe here as only last component might be missing */

	return ret;
}

