package org.pinae.logmesh.component.custom.mapper;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Map;

import org.junit.Test;
import org.pinae.logmesh.util.FileUtils;

public class RegexMapperTest {

	@Test
	public void testMap() {

		File mapperFile = FileUtils.getFile("mapper/regex_mapper.xml");
		RegexMapper regexMap = new RegexMapper(mapperFile);

		String message = "FW log:name=ASA5505;time=2013-07-10;action=permit;value=45";
		Map<String, String> resultMap = regexMap.map(message);
		assertEquals(resultMap.size(), 6);

		String formattedMsg = regexMap.format(message);
		assertEquals(formattedMsg, "FW log:name=ASA5505");

		message = "FW log2:name=PIX803;time=2013-09-10;action=permit;value=32";
		resultMap = regexMap.map(message);

		assertEquals(resultMap.size(), 6);
		
		message = "FW log3:PIX803 2013-09-10 permit 32";
		resultMap = regexMap.map(message);
		assertEquals(resultMap.size(), 6);
	}
}
