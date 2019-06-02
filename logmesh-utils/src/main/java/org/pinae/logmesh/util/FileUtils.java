package org.pinae.logmesh.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

public class FileUtils {
	
	public static File getFile(String filename) {
		return getFile(null, filename);
	}
	
	public static File getFile(String path, String filename) {
		File file = new File(filename);
		if (file.exists() && file.isFile()) {
			return file;
		}
		file = new File(path + File.separator + filename);
		if (file.exists() && file.isFile()) {
			return file;
		}
		file = new File(ClassLoaderUtils.getResourcePath("") + filename);
		if (file.exists() && file.isFile()) {
			return file;
		}
		
		file = new File(ClassLoaderUtils.getResourcePath("") + path + File.separator + filename);
		if (file.exists() && file.isFile()) {
			return file;
		}
		
		return null;
	}
	
	public static String readFileWithCreate(File file) throws IOException {
		if (! file.exists() || !file.isFile()) {
			file.createNewFile();
		}
		return readFile(file);
	}
	
	public static String readFile(File file) throws IOException {
		StringBuffer content = new StringBuffer();
		
		FileInputStream fileStream = new FileInputStream(file);
		InputStreamReader fileReader = new InputStreamReader(fileStream);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		
		String line = null;
		while((line = bufferedReader.readLine()) != null) {
			content.append(line);
		}
		
		bufferedReader.close();
		fileReader.close();
		fileStream.close();
		
		return content.toString();
	}
	
	public static void writeFile(File file, String content) throws IOException {
		writeFile(file, content, "UTF-8");
	}
	
	public static void writeFile(File file, String content, String codec) throws IOException {
		FileOutputStream fileStream = new FileOutputStream(file);
		OutputStreamWriter out = new OutputStreamWriter(fileStream, codec);
        out.write(content);
        out.flush();
        out.close();
	}
}
