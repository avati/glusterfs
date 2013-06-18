xlator_LTLIBRARIES = file-snapshot.la
xlatordir = $(libdir)/glusterfs/$(PACKAGE_VERSION)/xlator/testing/features

file_snapshot_la_LDFLAGS = -module -avoid-version

file_snapshot_la_SOURCES = file-snapshot.c
file_snapshot_la_LIBADD = $(top_builddir)/libglusterfs/src/libglusterfs.la

noinst_HEADERS = file-snapshot-mem-types.h

AM_CPPFLAGS = $(GF_CPPFLAGS) -I$(top_srcdir)/libglusterfs/src

AM_CFLAGS = -Wall $(GF_CFLAGS)

CLEANFILES =

