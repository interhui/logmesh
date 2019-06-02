package org.pinae.logmesh.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * 处理.properties文件的函数库
 * 
 */
public class PropertiesFileUtils {
	private Properties properties = new Properties();

	/**
	 * 载入properties文件
	 * 
	 * @param filename properties文件
	 */
	public void load(String filename) {
		try {
			properties.load(new FileInputStream(filename));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 根据关键字查找属性值
	 * 
	 * @param key 关键字
	 * @return 属性值
	 */
	public String getProperty(String key) {
		return properties.getProperty(key);
	}
}
