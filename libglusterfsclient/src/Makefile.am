lib_LTLIBRARIES = libglusterfsclient.la
noinst_HEADERS = libglusterfsclient-internals.h
libglusterfsclient_HEADERS = libglusterfsclient.h libglusterfsclient-types.h libglusterfsclient-sync.h libglusterfsclient-async.h
libglusterfsclientdir = $(includedir)

libglusterfsclient_la_SOURCES = libglusterfsclient.c libglusterfsclient-sync.c \
	libglusterfsclient-async.c libglusterfsclient-internals.c \
	libglusterfsclient-resolver.c libglusterfsclient-xlator.c

libglusterfsclient_la_CFLAGS =  -fPIC -Wall
libglusterfsclient_la_LIBADD = $(top_builddir)/libglusterfs/src/libglusterfs.la
libglusterfsclient_la_CPPFLAGS = -D_FILE_OFFSET_BITS=64 -D$(GF_HOST_OS) -D__USE_FILE_OFFSET64 -D_GNU_SOURCE -I$(top_srcdir)/libglusterfs/src -DDATADIR=\"$(localstatedir)\" -DCONFDIR=\"$(sysconfdir)/glusterfs\" $(GF_CFLAGS)
libglusterfsclient_la_LDFLAGS = -shared -nostartfiles

noinst_PROGRAMS = glfstest
glfstest_SOURCES = glfstest.c
glfstest_LDADD = $(top_builddir)/libglusterfsclient/src/libglusterfsclient.la
CLEANFILES = 

$(top_builddir)/libglusterfs/src/libglusterfs.la:
	$(MAKE) -C $(top_builddir)/libglusterfs/src/ all

