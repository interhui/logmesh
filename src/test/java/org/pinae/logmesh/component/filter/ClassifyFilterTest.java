package org.pinae.logmesh.component.filter;

import static org.junit.Assert.assertEquals;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.pinae.logmesh.component.filter.ClassifyFilter;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.util.MatchUtils;

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
		if (isWorkTime()) {
			assertEquals(message.getType(), "NOC|WorkTime|Huiyugeng|Firewall");
		} else {
			assertEquals(message.getType(), "NOC|Huiyugeng|Firewall");
		}
		
		message = new Message("192.168.0.106", "Liu", "Database Log: 192.168.33.12(PL/SQL) used SYSTEM connect Test-DB(Oracle 11.0.2.0)");
		message = filter.filter(message);
		if (isWorkTime()) {
			assertEquals(message.getType(), "NOC|WorkTime");
		} else {
			assertEquals(message.getType(), "NOC");
		}
		
		message = new Message("192.168.0.105", "Liu", "Host Log: 192.168.33.12(SSH) used root connect Transfer-FS(Ubuntu 14.04.03)");
		message = filter.filter(message);
		if (isWorkTime()) {
			assertEquals(message.getType(), "WorkTime");
		} else {
			assertEquals(message.getType(), "unknown");
		}
		
		message = new Message("192.168.0.104", "Liu", "ASA Log: keep alived used 100ms");
		message = filter.filter(message);
		if (isWorkTime()) {
			assertEquals(message.getType(), "WorkTime|Firewall");
		} else {
			assertEquals(message.getType(), "Firewall");
		}
		
		message = new Message("192.168.0.104", "Liu", "PIX Log: 192.168.12.21 deny access 10.3.0.12");
		message = filter.filter(message);
		if (isWorkTime()) {
			assertEquals(message.getType(), "WorkTime|Firewall");
		} else {
			assertEquals(message.getType(), "Firewall");
		}
		
	}
	
	private boolean isWorkTime() {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String time = dateFormat.format(System.currentTimeMillis());
		return MatchUtils.matchTime("10:00:00 - 23:00:00", time);
	}
}
