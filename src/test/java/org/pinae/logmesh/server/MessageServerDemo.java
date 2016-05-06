package org.pinae.logmesh.server;

import java.util.concurrent.TimeUnit;

import org.pinae.logmesh.server.MessageServer;
import org.pinae.logmesh.server.demo.MessageSenderExample;

public class MessageServerDemo {
	public static void main(String arg[]) throws InterruptedException {
		MessageServer server = new MessageServer("server.xml");
		server.start();
		
		// 5秒后启动消息发送
		TimeUnit.SECONDS.sleep(5);
		new Thread(new MessageSenderExample(1, false), "MessageSender").start();
	}
}
