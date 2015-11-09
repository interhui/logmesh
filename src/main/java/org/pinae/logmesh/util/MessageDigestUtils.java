package org.pinae.logmesh.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MessageDigestUtils {
	public static String MD5(String str) {
		return encrypt(str, "MD5");
	}

	public static String SHA1(String str) {
		return encrypt(str, "SHA1");
	}

	public static String encrypt(String str, String enc) {
		MessageDigest md = null;
		String result = null;
		byte[] bt = str.getBytes();
		try {
			md = MessageDigest.getInstance(enc);
			md.update(bt);
			result = bytes2Hex(md.digest());
		} catch (NoSuchAlgorithmException e) {
			System.out.println("Invalid algorithm.\n" + e.getMessage());
			return null;
		}
		return result;
	}

	// 将字节数组转换成16进制的字符串
	private static String bytes2Hex(byte[] bts) {
		String des = "";
		String tmp = null;

		for (int i = 0; i < bts.length; i++) {
			tmp = (Integer.toHexString(bts[i] & 0xFF));
			if (tmp.length() == 1) {
				des += "0";
			}
			des += tmp;
		}
		return des;
	}
}
