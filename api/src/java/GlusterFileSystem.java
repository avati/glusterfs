class GlusterFileSystem {
    static {
	System.loadLibrary("gfapi-java");
    }

    private String volname = null;
    private long fs;

    public GlusterFileSystem(String volume_name) {
	fs = glfsJNI.glfs_new(volume_name);
	volname = volume_name;
    }

    public long internal_handle() {
	return fs;
    }

    public String getVolumeName() {
	return volname;
    }

    public int setLogging(String LogFile, int LogLevel) {
	int ret;
	ret = glfsJNI.glfs_set_logging(fs, LogFile, LogLevel);
	return ret;
    }

    public int setVolFile(String VolFile) {
	int ret;
	ret = glfsJNI.glfs_set_volfile(fs, VolFile);
	return ret;
    }

    public int setVolFileServer(String Transport, String Host, int Port) {
	int ret;
	ret = glfsJNI.glfs_set_volfile_server(fs, Transport, Host, Port);
	return ret;
    }
}
