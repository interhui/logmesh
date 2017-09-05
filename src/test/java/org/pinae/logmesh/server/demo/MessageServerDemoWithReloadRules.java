package org.pinae.logmesh.server.demo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.pinae.logmesh.component.ComponentPool;
import org.pinae.logmesh.component.MessageComponent;
import org.pinae.logmesh.component.custom.DemoCustomProcessor;
import org.pinae.logmesh.component.filter.IPFilter;
import org.pinae.logmesh.output.ScreenOutputor;
import org.pinae.logmesh.server.MessageServer;
import org.pinae.logmesh.server.builder.MessageServerBuilder;
import org.pinae.logmesh.server.sender.MessageUDPSenderExample;

public class MessageServerDemoWithReloadRules {

	public static void main(String[] args) throws InterruptedException {
		
		Map<String, Object> ipFilterParameters = new HashMap<String, Object>();
		ipFilterParameters.put("pass", "true");
		ipFilterParameters.put("filter", "192.168.0.14|192.168.0.15");
		
		MessageServerBuilder builder = new MessageServerBuilder();
		builder.addFilter(1, "IPFilter", true, IPFilter.class, ipFilterParameters);
		builder.addReceiver(MessageServerBuilder.RECEIVER_UDP, true, false, "utf-8", null);
		builder.addProcessor("PrintProcessor", true, DemoCustomProcessor.class, null);
		
		builder.addOutputor("Windows", true, ScreenOutputor.class, null);

		MessageServer server = new MessageServer(builder.build());
		// 启动消息采集
		server.start();
		
		// 5秒后启动消息发送
		TimeUnit.SECONDS.sleep(5);
		new Thread(new MessageUDPSenderExample(1, true), "MessageSender").start();
		
		// 20秒后重新载入IPFilter规则
		TimeUnit.SECONDS.sleep(20);
		List<MessageComponent> filters = ComponentPool.get(IPFilter.class);
		ipFilterParameters.put("filter", "192.168.0.14|192.168.0.15|127.0.0.1");
		for (Object filter : filters) {
			if (filter instanceof IPFilter) {
				((IPFilter)filter).setParameters(ipFilterParameters);
			}
		}
		ComponentPool.reload(IPFilter.class);
		
		// 30秒后停止采集器运行
		TimeUnit.SECONDS.sleep(30);
		server.stop();
		
	}

}
