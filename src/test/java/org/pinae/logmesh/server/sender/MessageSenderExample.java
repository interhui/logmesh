package org.pinae.logmesh.server.sender;

import java.util.Random;

public class MessageSenderExample {
	
	private String messages[] = {
		"Firewall Log: 192.168.78.32 inbound stream",
		"Database Log: 192.168.33.12(PL/SQL) used SYSTEM connect Test-DB(Oracle 11.0.2.0)",
		"Host Log: 192.168.33.12(SSH) used root connect Transfer-FS(Ubuntu 14.04.03)",
		"ASA Log: keep alived used 100ms",
		"PIX Log: 192.168.12.21 deny access 10.3.0.12"
	};
	
	public String getMessage() {
		Random random = new Random();
		int index = random.nextInt(messages.length);
		if (index < messages.length) {
			return messages[index];
		} else {
			return messages[0];
		}
	}
}
