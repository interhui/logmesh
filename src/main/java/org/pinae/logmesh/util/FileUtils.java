package org.pinae.logmesh.util;

import java.io.File;

public class FileUtils {
	
	public static File getFile(String path, String filename) {
		File file = new File(filename);
		if (file.exists() && file.isFile()) {
			return file;
		}
		file = new File(path + filename);
		if (file.exists() && file.isFile()) {
			return file;
		}
		path = ClassLoaderUtils.getResourcePath("");
		file = new File(path + filename);
		if (file.exists() && file.isFile()) {
			return file;
		}
		return null;
	}
}
