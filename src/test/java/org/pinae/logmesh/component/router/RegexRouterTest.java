package org.pinae.logmesh.component.router;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.pinae.logmesh.message.Message;

public class RegexRouterTest {
	
	@Test
	public void testRoute() {
		Map<String, Object> parameters = new HashMap<String, Object>();
		parameters.put("file", "router/regex_router.xml");
		
		RegexRouter router = new RegexRouter();
		router.setParameters(parameters);
		router.init();
		
		Message message = null;
		
		message = new Message("192.168.0.105", "Hui", "Firewall Log: 192.168.78.32 inbound stream");
		router.porcess(message);
		assertEquals(message.getMessage(), "192.168.0.105 Hui Firewall Log: 192.168.78.32 inbound stream");
		
		message = new Message("192.168.0.106", "Liu", "Database Log: 192.168.33.12(PL/SQL) used SYSTEM connect Test-DB(Oracle 11.0.2.0)");
		router.porcess(message);
		assertEquals(message.getMessage(), "Database Log: 192.168.33.12(PL/SQL) used SYSTEM connect Test-DB(Oracle 11.0.2.0)");
		
		message = new Message("192.168.0.105", "Liu", "Host Log: 192.168.33.12(SSH) used root connect Transfer-FS(Ubuntu 14.04.03)");
		router.porcess(message);
		assertEquals(message.getMessage(), "Host Log: 192.168.33.12(SSH) used root connect Transfer-FS(Ubuntu 14.04.03)");
		
		message = new Message("192.168.0.104", "Liu", "ASA Log: keep alived used 100ms");
		router.porcess(message);
		assertEquals(message.getMessage(), "192.168.0.104 Liu ASA Log: keep alived used 100ms");
		
		message = new Message("192.168.0.103", "Liu", "PIX Log: 192.168.12.21 deny access 10.3.0.12");
		router.porcess(message);
		assertEquals(message.getMessage(), "192.168.0.103 Liu PIX Log: 192.168.12.21 deny access 10.3.0.12");
	}
}
