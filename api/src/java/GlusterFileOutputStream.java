import java.io.*;

class GlusterFileOutputStream extends OutputStream {
    private long fd;

    public GlusterFileOutputStream (GlusterFile file) {
	fd = 0;
    }

    public void write(byte [] buf) {
	glfs_javaJNI.glfs_java_write (fd, buf, buf.length);
	return;
    }

    public void write(int b) {
	byte[] buf = new byte[1];
	buf[0] = (byte)b;
	glfs_javaJNI.glfs_java_write (fd, buf, 1);
	return;
    }
}