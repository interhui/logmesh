package org.pinae.logmesh.server;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.pinae.logmesh.message.MessagePool;
import org.pinae.logmesh.processor.ProcessorFactory;
import org.pinae.logmesh.processor.ProcessorPool;
import org.pinae.logmesh.processor.imp.CustomProcessor;
import org.pinae.logmesh.processor.imp.FilterProcessor;
import org.pinae.logmesh.processor.imp.MergerProcessor;
import org.pinae.logmesh.processor.imp.OutputorProcessor;
import org.pinae.logmesh.processor.imp.RouterProcessor;
import org.pinae.logmesh.receiver.JMSReceiver;
import org.pinae.logmesh.receiver.Receiver;
import org.pinae.logmesh.receiver.TCPReceiver;
import org.pinae.logmesh.receiver.UDPReceiver;
import org.pinae.logmesh.server.helper.MessageCounter;
import org.pinae.logmesh.server.helper.OriginalMessageStore;
import org.pinae.nala.xb.Xml;
import org.pinae.ndb.Statement;

/**
 * 日志采集服务器
 * 
 * @author Huiyugeng
 * 
 */
public class LogServer {

	private static Logger log = Logger.getLogger(LogServer.class);

	private String filename; // 配置文件
	private Map<String, Object> config; // 配置信息
	private boolean startup = false; // 是否启动完成

	private Statement statement = new Statement();
	
	private MessageCounter messageCounter = null; 

	public LogServer(String filename) {
		this.filename = filename;
	}

	public LogServer(Map<String, Object> config) {
		this.config = config;
	}

	/**
	 * 启动Logmesh
	 */
	public void start() {
		long startTime = System.currentTimeMillis();

		// 载入配置
		if (this.config == null) {
			this.config = loadConfig();
		}

		// 载入消息队列
		MessagePool.init(config);

		// 启动消息展示器
		startOutputor(config);

		// 启动自定义处理器
		startCustomProcessor(config);

		// 启动消息路由器
		startRouter(config);

		// 启动归并器
		startMerger(config);

		// 启动过滤器
		startFilter(config);

		// 启动消息计数器
		startMessageCounter(config);

		// 启动原始消息存储器
		startOriginaMessageStorer(config);

		// 启动接收器
		startReceiver(config);

		this.startup = true;

		long startupTime = System.currentTimeMillis() - startTime;
		log.info(String.format("Start Logmesh in %d ms", startupTime));
	}

	/**
	 * 载入Logmesh配置信息
	 * 
	 * @return Logmesh配置信息
	 */
	@SuppressWarnings("unchecked")
	public Map<String, Object> loadConfig() {
		long startTime = System.currentTimeMillis();

		log.info(String.format("Loading Server Config: %s", filename));

		Map<String, Object> serverConfig = new HashMap<String, Object>();

		try {
			serverConfig = (Map<String, Object>) Xml.toMap(new File(filename), "UTF8");
		} catch (Exception e) {
			log.error(String.format("Load Server Config Error: exception=%s", e.getMessage()));
		}

		long startupTime = System.currentTimeMillis() - startTime;
		log.info(String.format("Load Server Config Finished in %d ms", startupTime));

		return serverConfig;
	}

	/**
	 * 启动消息接收器
	 * 
	 * @param config 配置信息
	 */
	@SuppressWarnings("unchecked")
	public void startReceiver(Map<String, Object> config) {

		List<Map<String, Object>> receiverConfigList = (List<Map<String, Object>>) statement.execute(config, "select:receiver->enable:true");

		for (Map<String, Object> receiverConfig : receiverConfigList) {
			Receiver receiver = null;

			String name = "";

			Map<String, String> startupConfig = ProcessorFactory.createParameter(receiverConfig);
			// 进行参数格式转换
			for (Entry<String, Object> entry : receiverConfig.entrySet()) {
				startupConfig.put(entry.getKey(), entry.getValue().toString());
			}
			
			if (receiverConfig.containsKey("type")) {
				String type = (String) receiverConfig.get("type");

				if (type.equalsIgnoreCase("TCP")) {
					receiver = new TCPReceiver();
					name = "TCPReceiver";
				} else if (type.equalsIgnoreCase("UDP")) {
					receiver = new UDPReceiver();
					name = "UDPReceiver";
				} else if (type.equalsIgnoreCase("JMS")) {
					receiver = new JMSReceiver();
					name = "JMSReceiver";
				}

			} else if (receiverConfig.containsKey("kwClass")) {
				String className = (String) receiverConfig.get("kwClass");
				name = receiverConfig.containsKey("name") ? (String) receiverConfig.get("name") : className;

				try {
					if (StringUtils.isNotEmpty(className)) {
						Class<?> clazz = Class.forName(className);
						Object object = clazz.newInstance();

						if (object != null && object instanceof Receiver) {
							receiver = (Receiver) object;
						}
					}
				} catch (Exception e) {
					log.error(String.format("Start Receiver Fail: exception=%s", e.getMessage()));
				}
			}

			if (receiver != null) {
				if (config.containsKey("owner")) {
					receiver.setOwner((String) config.get("owner"));
				}

				receiver.init(startupConfig);
				receiver.start(name);
			}
		}
	}

	/**
	 * 
	 * 启动原始消息存储器
	 * 
	 * @param config 配置信息
	 */
	@SuppressWarnings("unchecked")
	public void startOriginaMessageStorer(Map<String, Object> config) {
		long startTime = System.currentTimeMillis();

		Map<String, Object> originalConfig = (Map<String, Object>) statement.execute(config, "one:original");
		OriginalMessageStore messageStorer = new OriginalMessageStore(ProcessorFactory.createParameter(originalConfig));

		messageStorer.start();

		long startupTime = System.currentTimeMillis() - startTime;
		log.info(String.format("Start origina message storer in %d ms", startupTime));
	}

	/**
	 * 
	 * 启动消息计数器
	 * 
	 * @param config 配置信息
	 */
	@SuppressWarnings("unchecked")
	public void startMessageCounter(Map<String, Object> config) {
		long startTime = System.currentTimeMillis();

		Map<String, Object> counterConfig = (Map<String, Object>) statement.execute(config, "one:counter");
		this.messageCounter = new MessageCounter(ProcessorFactory.createParameter(counterConfig));

		messageCounter.start("MessageCounter");

		long startupTime = System.currentTimeMillis() - startTime;
		log.info(String.format("Start message counter in %d ms", startupTime));
	}
	
	/**
	 * 获取消息计数器
	 * 
	 * @return 消息计数器
	 */
	public MessageCounter getMessageCounter() {
		return messageCounter;
	}

	/**
	 * 启动消息过滤器
	 * 
	 * @param config 配置信息
	 */
	@SuppressWarnings("unchecked")
	public void startFilter(Map<String, Object> config) {
		long startTime = System.currentTimeMillis();

		int filterCount = 1;

		Map<String, Object> filterConfig = (Map<String, Object>) statement.execute(config, "one:thread->filter");
		try {
			filterCount = Integer.parseInt((String) filterConfig.get("count"));
		} catch (NumberFormatException e) {
			filterCount = 1;
		}

		log.info(String.format("Starting message filter, filter count is %d", filterCount));

		for (int i = 0; i < filterCount; i++) {
			new FilterProcessor(config).start("filter-" + Integer.toString(i));
		}

		long startupTime = System.currentTimeMillis() - startTime;
		log.info(String.format("Start message filter in %d ms", startupTime));
	}

	/**
	 * 启动消息路由器
	 * 
	 * @param config 配置信息
	 */
	@SuppressWarnings("unchecked")
	public void startRouter(Map<String, Object> config) {
		long startTime = System.currentTimeMillis();

		int routerCount = 1;

		Map<String, Object> routerConfig = (Map<String, Object>) statement.execute(config, "one:thread->router");
		try {
			routerCount = Integer.parseInt((String) routerConfig.get("count"));
		} catch (NumberFormatException e) {
			routerCount = 1;
		}

		log.info(String.format("Starting message router, router count is %d", routerCount));

		for (int i = 0; i < routerCount; i++) {
			new RouterProcessor(config).start("router-" + Integer.toString(i));
		}

		long startupTime = System.currentTimeMillis() - startTime;
		log.info(String.format("Start message router in %d ms", startupTime));
	}

	/**
	 * 启动消息归并器
	 * 
	 * @param config 配置信息
	 */
	@SuppressWarnings("unchecked")
	public void startMerger(Map<String, Object> config) {
		long startTime = System.currentTimeMillis();

		Map<String, Object> mergerConfig = (Map<String, Object>) statement.execute(config, "one:merger");
		
		boolean enable = false;
		
		try{
			enable = Boolean.parseBoolean((String) mergerConfig.get("enable"));
		} catch(Exception e) {
			enable = false;
		}
		
		if (enable == true) {

			new MergerProcessor(mergerConfig).start("merger");

			long startupTime = System.currentTimeMillis() - startTime;
			log.info(String.format("Start message merger in %d ms", startupTime));
		} else {
			log.info("Merger Processor is Disable");
		}

	}

	/**
	 * 启动消息自定义处理器
	 * 
	 * @param config 配置信息
	 */
	@SuppressWarnings("unchecked")
	public void startCustomProcessor(Map<String, Object> config) {
		long startTime = System.currentTimeMillis();

		int processorCount = 1;

		Map<String, Object> processorConfig = (Map<String, Object>) statement.execute(config, "one:thread->processor");

		try {
			processorCount = Integer.parseInt((String) processorConfig.get("count"));
		} catch (NumberFormatException e) {
			processorCount = 1;
		}

		log.info(String.format("Starting customer message processor, processor count is %d", processorCount));

		for (int i = 0; i < processorCount; i++) {
			new CustomProcessor(config).start("processor-" + Integer.toString(i));
		}

		long startupTime = System.currentTimeMillis() - startTime;
		log.info(String.format("Start customer message processor in %d ms", startupTime));
	}

	/**
	 * 启动消息展示器
	 * 
	 * @param config 配置信息
	 */
	public void startOutputor(Map<String, Object> config) {
		long startTime = System.currentTimeMillis();

		new OutputorProcessor(config).start("outputor");

		long startupTime = System.currentTimeMillis() - startTime;
		log.info(String.format("Start outputor in %d ms", startupTime));
	}

	/**
	 * 停止Logmesh
	 */
	public void stop() {
		ProcessorPool.stopAll();

		log.info("Logmesh STOP");
	}

	/**
	 * Logmesh是否启动完成
	 */
	public boolean isStartup() {
		return this.startup;
	}
}
