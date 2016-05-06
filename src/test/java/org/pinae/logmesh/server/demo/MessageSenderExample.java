package org.pinae.logmesh.server.demo;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.pinae.logmesh.output.forward.SendException;
import org.pinae.logmesh.output.forward.Sender;
import org.pinae.logmesh.output.forward.UDPSender;

public class MessageSenderExample implements Runnable {
	
	private static Logger logger = Logger.getLogger(MessageSenderExample.class);
	
	private Sender sender;
	
	private long sleep;
	private boolean displayOriginal;
	
	public MessageSenderExample(long sleep, boolean displayOriginal) {
		this.sleep = sleep;
		this.displayOriginal = displayOriginal;
		
		try {
			this.sender = new UDPSender("127.0.0.1", 514);
			this.sender.connect();
		} catch (SendException e) {
			logger.error(e.getMessage());
		}
	}
	
	public void run() {
		try {
			while(true) {
				String message = getMessage();
				sender.send(message);
				if (displayOriginal) {
					logger.info(message);
				}
				TimeUnit.SECONDS.sleep(sleep);
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}
	
	private String messages[] = {
		"Firewall Log: 192.168.78.32 inbound stream",
		"Database Log: 192.168.33.12(PL/SQL) used SYSTEM connect Test-DB(Oracle 11.0.2.0)",
		"Host Log: 192.168.33.12(SSH) used root connect Transfer-FS(Ubuntu 14.04.03)",
		"ASA Log: keep alived used 100ms",
		"PIX Log: 192.168.12.21 deny access 10.3.0.12"
	};
	
	private String getMessage() {
		Random random = new Random();
		int index = random.nextInt(messages.length);
		if (index < messages.length) {
			return messages[index];
		} else {
			return messages[0];
		}
	}
}
