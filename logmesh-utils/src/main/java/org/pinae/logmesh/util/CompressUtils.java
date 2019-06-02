package org.pinae.logmesh.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * 压缩工具类
 * 
 * @author Huiyugeng
 * 
 * 
 */
public class CompressUtils {
	/**
	 * 压缩数据
	 * 
	 * @param data 需要压缩的数据
	 * @return 压缩后的数据
	 */
	public static byte[] compress(byte[] data) {
		byte[] output = null;
		try {
			// 建立字节数组输出流
			ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
			// 建立gzip压缩输出流
			GZIPOutputStream gzipStream = new GZIPOutputStream(byteArrayStream);
			// 建立对象序列化输出流
			ObjectOutputStream objectStream = new ObjectOutputStream(gzipStream);
			objectStream.writeObject(data);

			objectStream.flush();
			objectStream.close();
			gzipStream.close();
			// 返回压缩字节流
			output = byteArrayStream.toByteArray();
			byteArrayStream.close();
		} catch (IOException e) {

		}
		return output;
	}

	/**
	 * 解压数据
	 * 
	 * @param data 需要解压的数据
	 * @return 解压后的数据
	 */
	public static byte[] uncompress(byte[] data) {
		byte[] output = null;
		try {
			// 建立字节数组输入流
			ByteArrayInputStream byteArrayStream = new ByteArrayInputStream(data);
			// 建立gzip解压输入流
			GZIPInputStream gzipStream = new GZIPInputStream(byteArrayStream);
			// 建立对象序列化输入流
			ObjectInputStream objectStream = new ObjectInputStream(gzipStream);
			output = (byte[]) objectStream.readObject();

			objectStream.close();
			gzipStream.close();
			byteArrayStream.close();
		} catch (IOException e) {

		} catch (ClassNotFoundException e) {

		}
		return output;
	}
}
