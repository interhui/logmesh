package org.pinae.logmesh.server;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.pinae.logmesh.component.custom.SystemOutProcessor;
import org.pinae.logmesh.output.ScreenOutputor;
import org.pinae.logmesh.server.builder.MessageServerBuilder;
import org.pinae.logmesh.server.helper.MessageCounter;

public class ServerTestWithCounter {
	
	private static Logger logger = Logger.getLogger(ServerTestWithCounter.class);

	public static void main(String[] args) throws InterruptedException {

		MessageServerBuilder builder = new MessageServerBuilder();
		builder.addReceiver(MessageServerBuilder.RECEIVER_UDP, true, false, "utf-8", null);
		
		Map<String, Object> processorParameters = new HashMap<String, Object>();
		processorParameters.put("display", "false");
		builder.addProcessor("PrintProcessor", true, SystemOutProcessor.class, processorParameters);
		
		Map<String, Object> counterParameters = new HashMap<String, Object>();
		counterParameters.put("enable", "true");
		counterParameters.put("counter", "time|owner|ip|type");
		builder.setCounter(counterParameters);
		
		builder.addOutputor("Windows", true, ScreenOutputor.class, null);
		
		// 启动消息采集
		MessageServer server = new MessageServer(builder.build());
		server.start();

		Thread display = new Thread(new CounterDisplay(server));
		display.start();
		
		// 5秒后启动消息发送
		TimeUnit.SECONDS.sleep(5);
		new Thread(new MessageSenderExample(1, false), "MessageSender").start();
	}

	public static class CounterDisplay implements Runnable {
		private static Logger logger = Logger.getLogger(CounterDisplay.class);

		private MessageCounter messageCounter;

		public CounterDisplay(MessageServer server) {
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
							logger.info("MessageCounter: " + ip.getKey() + " - " + ip.getValue());
						}
					}
				}
				try {
					TimeUnit.SECONDS.sleep(10);
				} catch (InterruptedException e) {
					logger.error(e.getMessage());
				}
			}

		}

	}

}
