package org.pinae.logmesh.component.filter;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.pinae.logmesh.component.filter.ClassifyFilter;
import org.pinae.logmesh.message.Message;

public class ClassifyFilterTest {
	
	@Test
	public void testFilter(){
		
		Map<String, Object> parameters = new HashMap<String, Object>();
		parameters.put("file", "filter/classify_filter.xml");
		
		ClassifyFilter filter = new ClassifyFilter();
		filter.setParameters(parameters);
		filter.initialize();
		
		Message message = null;
		
		message = new Message("192.168.0.107", "Hui", "Firewall Log: 192.168.78.32 inbound stream");
		message = filter.filter(message);
		assertEquals(message.getType(), "NOC|WorkTime|Huiyugeng|Firewall");
		
		message = new Message("192.168.0.106", "Liu", "Database Log: 192.168.33.12(PL/SQL) used SYSTEM connect Test-DB(Oracle 11.0.2.0)");
		message = filter.filter(message);
		assertEquals(message.getType(), "NOC|WorkTime");
		
		message = new Message("192.168.0.105", "Liu", "Host Log: 192.168.33.12(SSH) used root connect Transfer-FS(Ubuntu 14.04.03)");
		message = filter.filter(message);
		assertEquals(message.getType(), "WorkTime");
		
		message = new Message("192.168.0.104", "Liu", "ASA Log: keep alived used 100ms");
		message = filter.filter(message);
		assertEquals(message.getType(), "WorkTime|Firewall");
		
		message = new Message("192.168.0.104", "Liu", "PIX Log: 192.168.12.21 deny access 10.3.0.12");
		message = filter.filter(message);
		assertEquals(message.getType(), "WorkTime|Firewall");
		
	}
}
