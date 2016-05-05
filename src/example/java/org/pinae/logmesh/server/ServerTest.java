package org.pinae.logmesh.server;

import java.util.concurrent.TimeUnit;

import org.pinae.logmesh.util.ClassLoaderUtils;

public class ServerTest {

	public static void main(String[] args) throws InterruptedException {

		String path = ClassLoaderUtils.getResourcePath("");
		MessageServer server = new MessageServer(path, "server.xml");
		// 启动消息采集
		server.start();

		// 5秒后启动消息发送(UDP方式)
		TimeUnit.SECONDS.sleep(5);
		new Thread(new MessageSenderExample(3, true), "MessageSender").start();
	}

}
