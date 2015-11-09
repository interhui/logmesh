package org.pinae.logmesh.util;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.zip.Zip64Mode;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

/**
 * ZIP工具类
 * 
 * @author Huiyugeng
 */
public class ZipUtils {
	private final static Logger log = Logger.getLogger(ZipUtils.class);

	/**
	 * 清理并创建目录
	 * 
	 * @param name 目录名称
	 * @return 创建结果
	 * 
	 * @throws IOException 异常处理
	 */
	public static File mkdir(String name) throws IOException {
		File file = new File(name);
		if (file.exists() && file.isDirectory()) {
			FileUtils.deleteDirectory(file);
		}
		file.mkdir();
		return file;
	}

	/**
	 * 获取目录中文件列表
	 * 
	 * @param dirPath 文件目录
	 * 
	 * @return 目录中文件列表
	 */
	public static List<File> getFileList(String dirPath) {
		List<File> fileList = new ArrayList<File>();
		File dir = new File(dirPath);
		File[] files = dir.listFiles();
		if (files != null)
			for (int i = 0; i < files.length; i++) {
				if (files[i].isDirectory()) {
					fileList.addAll(getFileList(files[i].getAbsolutePath()));
				} else {
					fileList.add(new File(files[i].getAbsolutePath()));
				}
			}
		return fileList;
	}

	/**
	 * 解压目录中的文件
	 * 
	 * @param zipfile ZIP文件
	 * @param outDir 解压目标目录
	 * 
	 * @throws IOException 异常处理
	 */
	public static void unzip(String zipfile, String outDir) {

		try {
			mkdir(outDir);
		} catch (IOException e) {
			log.error(String.format("Mkdir %s Fail: exception=%s", outDir, e.getMessage()));
		}

		File inputFile = new File(zipfile);
		InputStream is = null;

		try {
			is = new FileInputStream(inputFile);
			ArchiveInputStream zipFileStream = new ArchiveStreamFactory().createArchiveInputStream("zip", is);

			ZipArchiveEntry zipEntry = null;
			while ((zipEntry = (ZipArchiveEntry) zipFileStream.getNextEntry()) != null) {
				File unzipFile = new File(outDir, zipEntry.getName());

				unzipFile.createNewFile();
				OutputStream unzipFileStream = new FileOutputStream(unzipFile);
				try {
					IOUtils.copy(zipFileStream, unzipFileStream);
				} finally {
					unzipFileStream.close();
				}
			}
			zipFileStream.close();
		} catch (IOException e) {
			log.error(String.format("IO Exception: exception=%s", e.getMessage()));
		} catch (ArchiveException e) {
			log.error(String.format("Unzip Exception: exception=%s", e.getMessage()));
		} finally {
			org.apache.commons.io.IOUtils.closeQuietly(is);
			log.debug(String.format("unzip success source %s to %s", inputFile.getAbsolutePath(), outDir));
		}
	}

	/**
	 * 压缩目录中的文件
	 * 
	 * @param zipfile 目标ZIP文件
	 * @param inDir 需要压缩的目录
	 * 
	 * @throws IOException 异常处理
	 */
	public static void zip(String zipfile, String inDir) {
		List<File> fileList = getFileList(inDir);

		if (fileList != null && fileList.size() > 0 && zipfile.endsWith("zip")) {
			ZipArchiveOutputStream zipStream = null;
			try {
				File zipFile = new File(zipfile);
				zipStream = new ZipArchiveOutputStream(zipFile);
				zipStream.setUseZip64(Zip64Mode.AsNeeded);

				for (File file : fileList) {
					if (file != null) {
						ZipArchiveEntry zipArchiveEntry = new ZipArchiveEntry(file, file.getName());
						zipStream.putArchiveEntry(zipArchiveEntry);
						InputStream is = null;
						try {
							is = new BufferedInputStream(new FileInputStream(file));
							byte[] buffer = new byte[1024 * 5];
							int len = -1;
							while ((len = is.read(buffer)) != -1) {
								zipStream.write(buffer, 0, len);
							}
							zipStream.closeArchiveEntry();
						} finally {
							org.apache.commons.io.IOUtils.closeQuietly(is);
						}

					}
				}
				zipStream.finish();
			} catch (Exception e) {
				log.error(String.format("Zip Exception: exception=%s", e.getMessage()));
			} finally {
				org.apache.commons.io.IOUtils.closeQuietly(zipStream);
			}

		}

	}

}
