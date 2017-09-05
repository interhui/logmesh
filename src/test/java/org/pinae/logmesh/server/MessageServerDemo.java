package org.pinae.logmesh.server;

import java.util.concurrent.TimeUnit;

import org.pinae.logmesh.output.sender.MessageUDPSenderTest;
import org.pinae.logmesh.server.MessageServer;

public class MessageServerDemo {
	public static void main(String arg[]) throws InterruptedException {
		MessageServer server = new MessageServer("server.xml");
		server.start();
		
		// 5秒后启动消息发送
		TimeUnit.SECONDS.sleep(5);
		new Thread(new MessageUDPSenderTest(1, false), "MessageSender").start();
	}
}
