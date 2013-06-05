import java.io.*;

class GlusterFileInputStream extends InputStream {
    private long fd;

    public GlusterFileInputStream(GlusterFile file) {
	fd = 0;
    }

    public int read(byte [] buf, int size) {
	return glfs_javaJNI.glfs_java_read (fd, buf, size);
    }

    public int read() {
	byte[] buf = new byte[1];

	glfs_javaJNI.glfs_java_read (fd, buf, 1);

	return buf[0];
    }
}