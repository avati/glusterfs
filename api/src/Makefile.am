lib_LTLIBRARIES = libgfapi.la
noinst_HEADERS = glfs-mem-types.h glfs-internal.h
libgfapi_HEADERS = glfs.h
libgfapidir = $(includedir)/glusterfs/api

libgfapi_la_SOURCES = glfs.c glfs-mgmt.c glfs-fops.c glfs-resolve.c
libgfapi_la_LIBADD = $(top_builddir)/libglusterfs/src/libglusterfs.la \
	$(top_builddir)/rpc/rpc-lib/src/libgfrpc.la \
	$(top_builddir)/rpc/xdr/src/libgfxdr.la \
	$(GF_LDADD)

libgfapi_la_CPPFLAGS = $(GF_CPPFLAGS) -D__USE_FILE_OFFSET64 \
	-I$(top_srcdir)/libglusterfs/src \
	-I$(top_srcdir)/rpc/rpc-lib/src \
	-I$(top_srcdir)/rpc/xdr/src


xlator_LTLIBRARIES = api.la
xlatordir = $(libdir)/glusterfs/$(PACKAGE_VERSION)/xlator/mount
# workaround for broken parallel install support in automake with LTLIBRARIES
# http://debbugs.gnu.org/cgi/bugreport.cgi?bug=7328
install_xlatorLTLIBRARIES = install-xlatorLTLIBRARIES
$(install_xlatorLTLIBRARIES): install-libLTLIBRARIES

api_la_SOURCES = glfs-master.c
api_la_DEPENDENCIES = libgfapi.la
api_la_LDFLAGS = -module -avoid-version
api_la_LIBADD = $(top_builddir)/libglusterfs/src/libglusterfs.la \
	$(top_builddir)/rpc/rpc-lib/src/libgfrpc.la \
	$(top_builddir)/rpc/xdr/src/libgfxdr.la \
	$(top_builddir)/api/src/libgfapi.la

AM_CPPFLAGS = $(GF_CPPFLAGS) -I$(top_srcdir)/libglusterfs/src


clean-local: javaclean
.PHONY: javaclean
javaclean:
	rm -rf $(top_builddir)/api/src/java-build

$(top_builddir)/api/src/java-build/glfs_wrap.c:
	rm -rf $(top_builddir)/api/src/java-build
	mkdir -p $(top_builddir)/api/src/java-build
	$(top_srcdir)/api/src/generate-interface.sh $(top_srcdir)/api/src/glfs.h glfs $(top_builddir)/api/src/java-build/glfs.i
	$(top_srcdir)/api/src/generate-interface.sh $(top_srcdir)/api/src/java/glfs-java.h glfs_java $(top_builddir)/api/src/java-build/glfs_java.i
	cd $(top_builddir)/api/src/java-build && \
		swig -D__BEGIN_DECLS -D__END_DECLS -java -o glfs_wrap.c -outdir . -module glfs glfs.i
	cd $(top_builddir)/api/src/java-build && \
		swig -D__BEGIN_DECLS -D__END_DECLS -java -o glfs_java_wrap.c -outdir . -module glfs_java glfs_java.i

bindings_LTLIBRARIES = libgfapi-java.la
bindingsdir = $(libdir)/glusterfs/$(PACKAGE_VERSION)/api-bindings
noinst_HEADERS += java/glfs-java.h

libgfapi_java_la_SOURCES = \
	$(top_builddir)/api/src/java-build/glfs_wrap.c \
	$(top_builddir)/api/src/java-build/glfs_java_wrap.c \
	$(top_builddir)/api/src/java/glfs-java.c

libgfapi_java_la_CPPFLAGS = $(JAVA_CPPFLAGS) -fno-strict-aliasing  $(GF_CPPFLAGS) -D__USE_FILE_OFFSET64 \
	-I$(top_srcdir)/libglusterfs/src \
	-I$(top_srcdir)/rpc/rpc-lib/src \
	-I$(top_srcdir)/rpc/xdr/src \
	-I$(top_srcdir)/api/src/java

libgfapi_java_la_DEPENDENCIES = libgfapi.la
libgfapi_java_la_LIBADD = $(top_builddir)/api/src/libgfapi.la \
	$(top_builddir)/libglusterfs/src/libglusterfs.la \
	$(top_builddir)/rpc/rpc-lib/src/libgfrpc.la \
	$(top_builddir)/rpc/xdr/src/libgfxdr.la

EXTRA_DIST = generate-interface.sh \
	java/GlusterFileSystem.java \
	java/GlusterFileStream.java

AM_CFLAGS = -Wall $(GF_CFLAGS)
