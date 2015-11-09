package org.pinae.logmesh.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.pinae.logmesh.util.CompressUtils;

/**
 * 消息压缩工具类测试
 * 
 * @author Huiyugeng
 * 
 * 
 */
public class CompressUtilsTest {

	@Test
	public void testCompressUtils() {

		for (int j = 0; j < 10000; j++) {
			String word = "";

			for (int i = 0; i < 100; i++) {
				word += "Hello ";
			}

			byte data[] = CompressUtils.compress(word.getBytes());

			String newword = new String(CompressUtils.uncompress(data));

			assertEquals(word, newword);
		}
	}
}
