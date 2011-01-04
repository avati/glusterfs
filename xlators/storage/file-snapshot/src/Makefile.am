
xlator_LTLIBRARIES = file-snapshot.la
xlatordir = $(libdir)/glusterfs/$(PACKAGE_VERSION)/xlator/storage

file_snapshot_la_LDFLAGS = -module -avoidversion

file_snapshot_la_SOURCES = file-snapshot.c file-snapshot-helpers.c
file_snapshot_la_LIBADD = $(top_builddir)/libglusterfs/src/libglusterfs.la

noinst_HEADERS = file-snapshot.h file-snapshot-mem-types.h

AM_CFLAGS = -fPIC -fno-strict-aliasing -D_FILE_OFFSET_BITS=64 -D_GNU_SOURCE \
            -D$(GF_HOST_OS) -Wall -I$(top_srcdir)/libglusterfs/src -shared \
            -nostartfiles -I$(top_srcdir)/contrib/md5 -I$(top_srcdir)/rpc/xdr/src \
            -I$(top_srcdir)/rpc/rpc-lib/src $(GF_CFLAGS)

CLEANFILES =

