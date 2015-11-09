package org.pinae.logmesh.util;

import org.junit.Test;
import org.pinae.logmesh.util.ZipUtils;

/**
 * ZIP工具类测试
 * 
 * @author Huiyugeng
 * 
 * 
 */
public class ZipUtilsTest {
	@Test
	public void testZip() {
		String inDir = "D:\\TestData\\simple";
		String zipfile = "D:\\TestData\\simple.zip";

		ZipUtils.zip(zipfile, inDir);
	}

	@Test
	public void testUnzip() {
		String outDir = "D:\\TestData\\simple1";
		String zipfile = "D:\\TestData\\simple.zip";

		ZipUtils.unzip(zipfile, outDir);
	}
}
