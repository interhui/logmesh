package org.pinae.logmesh.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * 对象工具集
 * 
 * @author Huiyugeng
 * 
 */
public class ObjectUtils {

	/**
	 * 从字节数组转变为对象
	 * 
	 * @param objBytes 字节数组
	 * @return 对象
	 * @throws Exception 异常处理
	 */
	public static Object getObjectFromBytes(byte[] objBytes) throws Exception {
		if (objBytes == null || objBytes.length == 0) {
			return null;
		}
		ByteArrayInputStream bi = new ByteArrayInputStream(objBytes);
		ObjectInputStream oi = new ObjectInputStream(bi);
		return oi.readObject();
	}

	/**
	 * 从对象转变为字节数组
	 * 
	 * @param obj 对象
	 * @return 字节数组
	 * @throws IOException 异常处理
	 */
	public static byte[] getBytes(Object obj) throws IOException {
		ByteArrayOutputStream buf = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(buf);
		oos.writeObject(obj);
		oos.flush();
		oos.close();
		buf.close();
		byte[] data = buf.toByteArray();
		return data;
	}

	/**
	 * 计算对象长度
	 * 
	 * @param obj 需要计算长度的对象
	 * @return 对象长度
	 */
	public static int size(Object obj) {
		if (obj == null) {
			return 0;
		}
		ByteArrayOutputStream buf = new ByteArrayOutputStream(4096);
		try {
			ObjectOutputStream out = new ObjectOutputStream(buf);
			out.writeObject(obj);
			out.flush();
			buf.close();
		} catch (IOException e) {
			return 0;
		}

		return buf.size();
	}
}
