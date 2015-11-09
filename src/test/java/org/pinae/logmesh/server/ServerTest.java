package org.pinae.logmesh.server;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;
import org.pinae.logmesh.server.helper.MessageCounter;
import org.pinae.logmesh.util.ClassLoaderUtils;

public class ServerTest {
	private static Logger logger = Logger.getLogger(ServerTest.class);

	public static void main(String[] args) {

		String path = ClassLoaderUtils.getResourcePath("");
		LogServer server = new LogServer(path + "server.xml");
		// 启动日志采集
		server.start();

		Thread shower = new Thread(new MessageShower(server));
		shower.start();
		
		/*
		// 规则重载
		try {
			Thread.sleep(60000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		ComponentPool.reloadComponent(IPFilter.class);

		try {
			Thread.sleep(60000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		ComponentPool.reloadComponent(IPFilter.class);

		// 采集器停止
		if (server.isStartup()) {
			server.stop();
		}
		
		System.exit(0);
		*/
	}

	public static class MessageShower implements Runnable {
		private static Logger log = Logger.getLogger(MessageShower.class);

		private MessageCounter messageCounter;

		public MessageShower(LogServer server) {
			if (server.getMessageCounter() != null) {
				this.messageCounter = server.getMessageCounter();
			}

		}

		public void run() {
			while (true) {
				if (messageCounter != null) {
					Map<String, Long> counter = messageCounter.getCounter("ip", "127.0.0.1");
					if (counter != null) {
						Set<Entry<String, Long>> ipSet = counter.entrySet();
						for (Entry<String, Long> ip : ipSet) {
							log.info(ip.getKey() + ":" + ip.getValue());
						}
					}
				}
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

		}

	}

}
