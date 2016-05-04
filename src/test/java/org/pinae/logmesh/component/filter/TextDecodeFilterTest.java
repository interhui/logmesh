package org.pinae.logmesh.component.filter;

import static org.junit.Assert.assertEquals;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.pinae.logmesh.message.Message;

public class TextDecodeFilterTest {
	
	@Test
	public void testFilter() throws UnsupportedEncodingException {
		Map<String, Object> parameters = new HashMap<String, Object>();
		parameters.put("file", "filter/text_decode_filter.xml");
		
		TextDecodeFilter filter = new TextDecodeFilter();
		filter.setParameters(parameters);
		filter.initialize();
		
		Message message = null;

		message = new Message("192.168.0.107", "Hui", 
				"防火墙日志: 192.168.78.32 数据流入".getBytes("GBK"));
		message = filter.filter(message);
		assertEquals(message.getMessage().toString(), "防火墙日志: 192.168.78.32 数据流入");
		
		message = new Message("192.168.0.104", "Hui", 
				new String("ASA日志: 心跳检查100ms".getBytes("GBK"), "GB2312").getBytes("GB2312"));
		message = filter.filter(message);
		assertEquals(message.getMessage().toString(), "ASA日志: 心跳检查100ms");
		
	}
}
