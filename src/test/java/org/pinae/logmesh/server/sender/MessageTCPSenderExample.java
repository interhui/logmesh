package org.pinae.logmesh.server.sender;

import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.pinae.logmesh.output.forward.SendException;
import org.pinae.logmesh.output.forward.Sender;
import org.pinae.logmesh.output.forward.TCPSender;

public class MessageTCPSenderExample extends MessageSenderExample implements Runnable {
	
	private static Logger logger = Logger.getLogger(MessageTCPSenderExample.class);
	
	private Sender sender;
	
	private long sleep;
	private boolean displayOriginal;
	
	public MessageTCPSenderExample(long sleep, boolean displayOriginal) {
		this.sleep = sleep;
		this.displayOriginal = displayOriginal;
		
		try {
			this.sender = new TCPSender("127.0.0.1", 514);
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

}
