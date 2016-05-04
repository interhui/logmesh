package org.pinae.logmesh.server.builder;

import java.util.HashMap;
import java.util.Map;

import org.pinae.logmesh.server.MessageServer;
import org.pinae.logmesh.server.builder.MessageServerBuilder;

public class ServerBuilderTest {

	private MessageServerBuilder builder = new MessageServerBuilder();

	public void start() {
		setOwner();
		addQueue();
		setThread();
		activeMerger();
		addReceiver();
		setOriginal();
		setCounter();
		addFilter();
		addProcessor();
		addOutput();

		MessageServer server = new MessageServer(builder.build());
		server.start();
	}

	public static void main(String arg[]) {
		new ServerBuilderTest().start();
	}

	public void setOwner() {
		builder.setOwner("logmesh");
	}

	public void addQueue() {
		builder.addQueue("FILE_STORE_QUEUE", 50000);
		builder.addQueue("SOLR_STORE_QUEUE", 50000);
		builder.addQueue("DB_STORE_QUEUE", 50000);
	}

	public void setThread() {
		builder.setThread("filter", 3, 100);
		builder.setThread("router", 3, 100);
		builder.setThread("processor", 3, 100);
	}

	public void activeMerger() {
		builder.activeMerger(false, 1000);
	}

	public void addReceiver() {
		Map<String, Object> parameters = new HashMap<String, Object>();
		parameters.put("port", "514");
		parameters.put("message", "string");
		builder.addReceiver("udp", true, false , "utf8", parameters);
	}

	public void setOriginal() {
		Map<String, Object> parameters = new HashMap<String, Object>();
		parameters.put("msgFormat", "$time : $ip : $message");
		parameters.put("path", "d:\\TestData\\original");
		parameters.put("dir", "yyyy-MM-dd-HH-mm");
		parameters.put("title", "message");
		parameters.put("pattern", "yyyy-MM-dd-HH-mm");
		parameters.put("ext", "log");
		parameters.put("encoding", "utf8");
		parameters.put("cycle", "5000");
		parameters.put("zip", "true");
		builder.setOriginal(parameters);
	}

	public void setCounter() {
		Map<String, Object> parameters = new HashMap<String, Object>();
		parameters.put("enable", "true");
		parameters.put("counter", "time|owner|ip|type");
		builder.setCounter(parameters);
	}

	public void addFilter() {
		Map<String, Object> parameters = new HashMap<String, Object>();
		parameters.put("file", "filter/filter_ip.xml");
		parameters.put("pass", "true");
		builder.addFilter(1, "IPFilter", true, org.pinae.logmesh.component.filter.IPFilter.class, parameters);
	}

	public void addProcessor() {
		builder.addProcessor("SystemOutProcessor", true, org.pinae.logmesh.component.custom.SystemOutProcessor.class, null);
	}

	public void addOutput() {
		Map<String, Object> parameters = new HashMap<String, Object>();
		parameters.put("title", "logmesh --- Free Log Collector");
		parameters.put("width", "900");
		parameters.put("height", "600");
		parameters.put("columns", "80");
		parameters.put("rows", "100");
		parameters.put("background", "#000000");
		parameters.put("foreground", "#00ff00");
		builder.addOutputor("WindowsTest", true, org.pinae.logmesh.output.ScreenOutputor.class, parameters);
	}
}
