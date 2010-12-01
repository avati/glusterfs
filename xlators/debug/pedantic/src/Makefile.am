
xlator_LTLIBRARIES = pedantic.la
xlatordir = $(libdir)/glusterfs/$(PACKAGE_VERSION)/xlator/debug

pedantic_la_LDFLAGS = -module -avoidversion

pedantic_la_SOURCES = pedantic.c
pedantic_la_LIBADD = $(top_builddir)/libglusterfs/src/libglusterfs.la

AM_CFLAGS = -fPIC -D_FILE_OFFSET_BITS=64 -D_GNU_SOURCE -Wall -D$(GF_HOST_OS)\
	-I$(top_srcdir)/libglusterfs/src -shared -nostartfiles $(GF_CFLAGS)

CLEANFILES = 

