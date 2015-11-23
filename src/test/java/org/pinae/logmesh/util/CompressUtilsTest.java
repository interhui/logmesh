package org.pinae.logmesh.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.text.DecimalFormat;

import org.apache.log4j.Logger;
import org.codehaus.stax2.ri.typed.ValueDecoderFactory.DecimalDecoder;
import org.junit.Test;

/**
 * 消息压缩工具类测试
 * 
 * @author Huiyugeng
 * 
 * 
 */
public class CompressUtilsTest {
	private static Logger logger = Logger.getLogger(CompressUtilsTest.class);

	@Test
	public void testCompressUtils() {

		String data = "";
		for (int i = 0; i < 100; i++) {
			data += "Hello ";
		}

		int originalDataLength = data.getBytes().length;
		byte compressData[] = CompressUtils.compress(data.getBytes());
		int compressDataLength = compressData.length;
		String uncompressData = new String(CompressUtils.uncompress(compressData));

		assertEquals(data, uncompressData);
		assertTrue(compressDataLength < originalDataLength);
		logger.info("Compress Rate: " + new DecimalFormat("#.00")
				.format(((double) compressDataLength / (double) originalDataLength) * 100) + "%");
	}
}
