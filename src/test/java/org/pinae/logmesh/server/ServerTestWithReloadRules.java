package org.pinae.logmesh.server;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.pinae.logmesh.component.ComponentPool;
import org.pinae.logmesh.component.custom.SystemOutProcessor;
import org.pinae.logmesh.component.filter.IPFilter;
import org.pinae.logmesh.output.WindowOutputor;
import org.pinae.logmesh.sender.SendException;
import org.pinae.logmesh.sender.Sender;
import org.pinae.logmesh.sender.UDPSender;
import org.pinae.logmesh.server.ServerTest.MessageSender;
import org.pinae.logmesh.server.builder.ServerBuilder;

public class ServerTestWithReloadRules {

	public static void main(String[] args) throws InterruptedException {
		
		Map<String, Object> ipFilterParameters = new HashMap<String, Object>();
		ipFilterParameters.put("pass", "true");
		ipFilterParameters.put("filter", "192.168.0.14|192.168.0.15");
		
		ServerBuilder builder = new ServerBuilder();
		builder.addFilter(1, "IPFilter", true, IPFilter.class, ipFilterParameters);
		builder.addReceiver(ServerBuilder.RECEIVER_UDP, true, false, "utf-8", null);
		builder.addProcessor("PrintProcessor", true, SystemOutProcessor.class, null);
		
		Map<String, Object> outputorParameters = new HashMap<String, Object>();
		outputorParameters.put("title", "logmesh --- Free Log Collector");
		outputorParameters.put("width", "900");
		outputorParameters.put("height", "600");
		outputorParameters.put("columns", "80");
		outputorParameters.put("rows", "100");
		outputorParameters.put("background", "#000000");
		outputorParameters.put("foreground", "#00ff00");
		builder.addOutputor("Windows", true, WindowOutputor.class, outputorParameters);

		MessageServer server = new MessageServer(builder.build());
		// 启动日志采集
		server.start();
		
		// 5秒后启动日志发送
		TimeUnit.SECONDS.sleep(5);
		new Thread(new MessageSender()).start();
		
		// 20秒后重新载入IPFilter规则
		TimeUnit.SECONDS.sleep(20);
		List<Object> filters = ComponentPool.getComponent(IPFilter.class);
		ipFilterParameters.put("filter", "192.168.0.14|192.168.0.15|127.0.0.1");
		for (Object filter : filters) {
			if (filter instanceof IPFilter) {
				((IPFilter)filter).setParameters(ipFilterParameters);
			}
		}
		ComponentPool.reloadComponent(IPFilter.class);
		
		// 30秒后停止采集器运行
		TimeUnit.SECONDS.sleep(30);
		server.stop();
		
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
					TimeUnit.SECONDS.sleep(1);
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
