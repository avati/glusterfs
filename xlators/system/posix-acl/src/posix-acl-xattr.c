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


#include <stdio.h>
#include <stdlib.h>

#include "posix-acl.h"
#include "posix-acl-xattr.h"


int
posix_ace_cmp (const void *val1, const void *val2)
{
        const struct posix_ace *ace1 = NULL;
        const struct posix_ace *ace2 = NULL;
        int                     ret = 0;

        ace1 = val1;
        ace2 = val2;

        ret = (ace1->tag - ace2->tag);
        if (!ret)
                ret = (ace1->id - ace2->id);

        return ret;
}


void
posix_acl_normalize (xlator_t *this, struct posix_acl *acl)
{
        qsort (acl->entries, acl->count, sizeof (struct posix_ace *),
               posix_ace_cmp);
}


struct posix_acl *
posix_acl_from_xattr (xlator_t *this, const char *xattr_buf, int xattr_size)
{
        struct posix_acl_xattr_header   *header = NULL;
        struct posix_acl_xattr_entry    *entry = NULL;
        struct posix_acl                *acl = NULL;
        struct posix_ace                *ace = NULL;
        int                              size = 0;
        int                              count = 0;
        int                              i = 0;

        size = xattr_size;

        if (size < sizeof (*header))
                return NULL;

        size -= sizeof (*header);

        if (size % sizeof (*entry))
                return NULL;

        count = size / sizeof (*entry);

        header = (struct posix_acl_xattr_header *) (xattr_buf);
        entry = (struct posix_acl_xattr_entry *) (header + 1);

        if (header->version != htole32 (2))
                return NULL;

        acl = posix_acl_new (this, count);
        if (!acl)
                return NULL;

        ace = acl->entries;

        for (i = 0; i < count; i++) {
                ace->tag  = letoh16 (entry->tag);
                ace->perm = letoh16 (entry->perm);

                switch (ace->tag) {
                case POSIX_ACL_USER_OBJ:
                case POSIX_ACL_GROUP_OBJ:
                case POSIX_ACL_MASK:
                case POSIX_ACL_OTHER:
                        ace->id = POSIX_ACL_UNDEFINED_ID;
                        break;

                case POSIX_ACL_GROUP:
                case POSIX_ACL_USER:
                        ace->id = letoh32 (entry->id);
                        break;

                default:
                        goto err;
                }

                ace++;
                entry++;
        }

        posix_acl_normalize (this, acl);

        return acl;
err:
        posix_acl_destroy (this, acl);
        return NULL;
}


int
posix_acl_to_xattr (xlator_t *this, struct posix_acl *acl, char *xattr_buf,
                    int xattr_size)
{
        int                             size = 0;
        struct posix_acl_xattr_header  *header = NULL;
        struct posix_acl_xattr_entry   *entry = NULL;
        struct posix_ace               *ace = NULL;
        int                             i = 0;

        size = sizeof (*header) + (acl->count * sizeof (*entry));

        if (xattr_size < size)
                return size;

        header = (struct posix_acl_xattr_header *) (xattr_buf);
        entry = (struct posix_acl_xattr_entry *) (header + 1);
        ace = acl->entries;

        header->version = htole32 (2);

        for (i = 0; i < acl->count; i++) {
                entry->tag   = htole16 (ace->tag);
                entry->perm  = htole16 (ace->perm);

                switch (ace->tag) {
                case POSIX_ACL_USER:
                case POSIX_ACL_GROUP:
                        entry->id  = htole32 (ace->id);
                        break;
                default:
                        entry->id = POSIX_ACL_UNDEFINED_ID;
                        break;
                }

                ace++;
                entry++;
        }

        return 0;
}


int
posix_acl_matches_xattr (xlator_t *this, struct posix_acl *acl, const char *buf,
                         int size)
{
        struct posix_acl  *acl2 = NULL;
        int                ret = 1;

        acl2 = posix_acl_from_xattr (this, buf, size);
        if (!acl2)
                return 0;

        if (acl->count != acl2->count) {
                ret = 0;
                goto out;
        }

        if (memcmp (acl->entries, acl2->entries,
                    (acl->count * sizeof (struct posix_ace))))
                ret = 0;
out:
        posix_acl_destroy (this, acl2);

        return ret;
}

