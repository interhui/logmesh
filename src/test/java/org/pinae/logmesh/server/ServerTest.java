package org.pinae.logmesh.server;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.pinae.logmesh.sender.SendException;
import org.pinae.logmesh.sender.Sender;
import org.pinae.logmesh.sender.UDPSender;
import org.pinae.logmesh.util.ClassLoaderUtils;

public class ServerTest {
	
	private static Logger logger = Logger.getLogger(ServerTest.class);

	public static void main(String[] args) {

		String path = ClassLoaderUtils.getResourcePath("");
		MessageServer server = new MessageServer(path + "server.xml");
		// 启动日志采集
		server.start();
		
		try {
			TimeUnit.SECONDS.sleep(5);
		} catch (InterruptedException e) {
			logger.error(e.getMessage());
		}
		
		// 启动日志发送
		new Thread(new MessageSender()).start();
	}
	
	public static class MessageSender implements Runnable {
		
		private static Logger logger = Logger.getLogger(MessageSender.class);
		
		private Sender sender;
		
		public MessageSender() {
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
					logger.info(message);
					TimeUnit.SECONDS.sleep(3);
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

}
