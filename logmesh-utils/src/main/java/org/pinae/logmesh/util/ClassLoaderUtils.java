package org.pinae.logmesh.util;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

/**
 * ClassLoader工具函数库
 * 
 * 
 */
public class ClassLoaderUtils {
	private static ClassLoader systemClassLoader;

	static {
		systemClassLoader = ClassLoader.getSystemClassLoader();
	}

	/*
	 * 获取ClassLoader集合
	 */
	private static ClassLoader[] getClassLoaders(ClassLoader classLoader) {
		return new ClassLoader[] { classLoader, Thread.currentThread().getContextClassLoader(),
				ClassLoaderUtils.class.getClassLoader(), systemClassLoader };
	}

	/**
	 * 获取资源路径(classes根路径)
	 * 
	 * @param resource 资源名称
	 * @return 资源路径
	 */
	public static String getResourcePath(String resource) {
		String path = null;
		ClassLoader[] loaders = getClassLoaders(null);
		for (int i = 0; i < loaders.length; i++) {
			if (loaders[i] != null) {
				try {
					path = loaders[i].getResource(resource).getPath();

				} catch (NullPointerException e) {

				}
			}
		}
		return path;
	}
	
	/**
	 * 加载指定的Jar目录中的jar文件和类
	 * 
	 * @param jarDir Jar文件目录
	 * @param className Class名称
	 * 
	 * @return 加载的Class类
	 * 
	 * @throws ClassNotFoundException 类无法找到异常
	 * @throws IOException IO异常
	 */
	public static Class<?> loadClass(String jarDir, String className) throws ClassNotFoundException, IOException {
		if (jarDir == null) {
			return null;
		}
		File dir = new File(jarDir);
		if (dir.exists() && dir.isDirectory()) {
			String filenames[] = dir.list();
			return loadClass(filenames, className);
		}
		return null;
	}
	
	/**
	 * 加载指定的Jar文件和类
	 * 
	 * @param jarFilenames Jar文件路径
	 * @param className Class名称
	 * 
	 * @return 加载的Class类
	 * 
	 * @throws ClassNotFoundException 类无法找到异常
	 * @throws IOException IO异常
	 */
	public static Class<?> loadClass(String jarFilenames[], String className) throws ClassNotFoundException, IOException {
		
		if (StringUtils.isEmpty(className)) {
			return null;
		}
		
		if (jarFilenames != null && jarFilenames.length > 0) {

			List<URL> urlList = new ArrayList<URL>();
			
			for (String jarFilename : jarFilenames) {
				if (StringUtils.isEmpty(jarFilename)) {
					continue;
				}
				File jarFile = new File(jarFilename);
				if (!jarFile.exists() || !jarFile.isFile()) {
					continue;
				}
				
				urlList.add(new URL(jarFilename));
			}
			
			URL urls[] = new URL[urlList.size()];
			for (int i = 0 ; i < urlList.size() ; i++) {
				urls[i] = urlList.get(i);
			}
			
			URLClassLoader classLoader = new URLClassLoader(urls, Thread.currentThread().getContextClassLoader());
			Class<?> clazz = classLoader.loadClass(className);
			classLoader.close();
			return clazz;
		}
		
		return null;
	}
}
