package org.pinae.logmesh.server;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.pinae.logmesh.component.ComponentPool;
import org.pinae.logmesh.component.custom.SystemOutProcessor;
import org.pinae.logmesh.component.filter.IPFilter;
import org.pinae.logmesh.output.WindowOutputor;
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
		new Thread(new MessageSenderExample(1, false)).start();
		
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

}
