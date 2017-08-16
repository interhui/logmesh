package org.pinae.logmesh.server;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.pinae.logmesh.component.ComponentFactory;
import org.pinae.logmesh.message.MessagePool;
import org.pinae.logmesh.processor.ProcessorPool;
import org.pinae.logmesh.processor.imp.CustomProcessor;
import org.pinae.logmesh.processor.imp.FilterProcessor;
import org.pinae.logmesh.processor.imp.OutputorProcessor;
import org.pinae.logmesh.processor.imp.RouterProcessor;
import org.pinae.logmesh.receiver.AbstractReceiver;
import org.pinae.logmesh.receiver.event.KafkaReceiver;
import org.pinae.logmesh.receiver.event.TCPReceiver;
import org.pinae.logmesh.receiver.event.UDPReceiver;
import org.pinae.logmesh.server.helper.MessageCounter;
import org.pinae.logmesh.server.helper.OriginalMessageStorer;
import org.pinae.logmesh.util.ClassLoaderUtils;
import org.pinae.logmesh.util.FileUtils;
import org.pinae.nala.xb.Xml;
import org.pinae.ndb.Ndb;
import org.pinae.ndb.common.MapHelper;

/**
 * 消息采集服务器
 * 
 * @author Huiyugeng
 * 
 */
public class MessageServer {

	private static Logger logger = Logger.getLogger(MessageServer.class);
	/* 配置文件路径 */
	private String path;
	/* 配置文件名 */
	private String filename;
	/* 配置信息 */
	private Map<String, Object> config;
	/* 是否启动完成 */
	private boolean startup = false;
	
	private MessageCounter messageCounter = null; 

	public MessageServer() {
		
	}
	
	public MessageServer(String filename) {
		this(null, filename);
	}
	
	public MessageServer(String path, String filename) {
		if (path == null) {
			this.path = ClassLoaderUtils.getResourcePath("");
		} else {
			this.path = path;
		}
		this.filename = filename;
	}

	public MessageServer(Map<String, Object> config) {
		this.config = config;
	}

	/**
	 * 启动Logmesh
	 */
	public void start() {
		long startTime = System.currentTimeMillis();

		// 载入配置
		if (this.config == null) {
			if (StringUtils.isNotEmpty(this.filename)) {
				File configFile = FileUtils.getFile(this.path, this.filename);
				if (configFile != null) {
					this.config = loadConfig(configFile);
				} else {
					logger.error(String.format("Load server config %s%s FAIL", this.path, this.filename));
				}
			} else {
				logger.error("Server config filename is NULL");
			}
		}
		
		if (this.config != null) {
			// 载入消息队列
			MessagePool.initialize(config);
	
			// 启动消息展示器
			startOutputor(config);
	
			// 启动自定义处理器
			startCustomProcessor(config);
	
			// 启动消息路由器
			startRouter(config);
	
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
			logger.info(String.format("Start Logmesh in %d ms", startupTime));
		} else {
			logger.error("Server configurtion is NULL and start FAIL");
		}
	}

	/**
	 * 载入Logmesh配置信息
	 * 
	 * @return Logmesh配置信息
	 */
	@SuppressWarnings("unchecked")
	public Map<String, Object> loadConfig(File file) {
		long startTime = System.currentTimeMillis();

		logger.info(String.format("Loading server config: %s", file.getAbsolutePath()));

		Map<String, Object> serverConfig = new HashMap<String, Object>();

		try {
			serverConfig = (Map<String, Object>) Xml.toMap(file, "UTF8");
		} catch (Exception e) {
			logger.error(String.format("Load server config Error: exception=%s", e.getMessage()));
		}

		long startupTime = System.currentTimeMillis() - startTime;
		logger.info(String.format("Load server config Finished in %d ms", startupTime));

		List<String> importFilenameList = (List<String>)Ndb.execute(serverConfig, "select:import->file");
		for (String importFilename : importFilenameList) {
			File importFile = FileUtils.getFile(this.path, importFilename);
			if (importFile != null) {
				Map<String, Object> importConfig = loadConfig(importFile);
				
				MapHelper.join(serverConfig, importConfig);
			}
		}

		return serverConfig;
	}

	/**
	 * 启动消息接收器
	 * 
	 * @param config 配置信息
	 */
	@SuppressWarnings("unchecked")
	public void startReceiver(Map<String, Object> config) {

		List<Map<String, Object>> receiverConfigList = (List<Map<String, Object>>) Ndb.execute(config, "select:receiver->enable:true");

		for (Map<String, Object> receiverConfig : receiverConfigList) {
			AbstractReceiver receiver = null;

			String name = "";

			Map<String, Object> startupConfig = ComponentFactory.createParameter(receiverConfig);
			// 进行参数格式转换
			for (Entry<String, Object> entry : receiverConfig.entrySet()) {
				startupConfig.put(entry.getKey(), entry.getValue().toString());
			}
			
			if (receiverConfig.containsKey("type")) {
				// 使用预置接收器
				String type = (String) receiverConfig.get("type");

				if (type.equalsIgnoreCase("TCP")) {
					receiver = new TCPReceiver();
					name = "TCPReceiver";
				} else if (type.equalsIgnoreCase("UDP")) {
					receiver = new UDPReceiver();
					name = "UDPReceiver";
				} else if (type.equalsIgnoreCase("Kafka")) {
					receiver = new KafkaReceiver();
					name = "KafkaReceiver";
				}
			
			} else if (receiverConfig.containsKey("kwClass")) {
				// 使用自定义接收器
				String className = (String) receiverConfig.get("kwClass");
				name = receiverConfig.containsKey("name") ? (String) receiverConfig.get("name") : className;

				try {
					if (StringUtils.isNotEmpty(className)) {
						Class<?> clazz = Class.forName(className);
						Object object = clazz.newInstance();

						if (object != null && object instanceof AbstractReceiver) {
							receiver = (AbstractReceiver) object;
						}
					}
				} catch (Exception e) {
					logger.error(String.format("Start Receiver Errork: exception=%s", e.getMessage()));
				}
			}

			if (receiver != null) {
				if (config.containsKey("owner")) {
					receiver.setOwner((String) config.get("owner"));
				}

				receiver.initialize(startupConfig);
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

		Map<String, Object> originalConfig = (Map<String, Object>) Ndb.execute(config, "one:original");
		
		OriginalMessageStorer messageStorer = new OriginalMessageStorer(ComponentFactory.createParameter(originalConfig));
		messageStorer.start();

		long startupTime = System.currentTimeMillis() - startTime;
		logger.info(String.format("Start origina message storer in %d ms", startupTime));
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

		Map<String, Object> counterConfig = (Map<String, Object>) Ndb.execute(config, "one:counter");
		this.messageCounter = new MessageCounter(ComponentFactory.createParameter(counterConfig));

		messageCounter.start("MessageCounter");

		long startupTime = System.currentTimeMillis() - startTime;
		logger.info(String.format("Start message counter in %d ms", startupTime));
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

		Map<String, Object> filterConfig = (Map<String, Object>) Ndb.execute(config, "one:thread->filter");
		
		int filterCounter = 1;
		if (filterConfig != null && filterConfig.containsKey("count")) {
			try {
				filterCounter = Integer.parseInt((String) filterConfig.get("count"));
			} catch (NumberFormatException e) {
				filterCounter = 1;
			}
		}
		// 过滤器最小数量为1
		if (filterCounter < 1) {
			filterCounter = 1;
		}

		logger.info(String.format("Starting message filter, filter count is %d", filterCounter));

		for (int i = 0; i < filterCounter; i++) {
			new FilterProcessor(config).start("filter-" + Integer.toString(i));
		}

		long startupTime = System.currentTimeMillis() - startTime;
		logger.info(String.format("Start message filter in %d ms", startupTime));
	}

	/**
	 * 启动消息路由器
	 * 
	 * @param config 配置信息
	 */
	@SuppressWarnings("unchecked")
	public void startRouter(Map<String, Object> config) {
		long startTime = System.currentTimeMillis();

		Map<String, Object> routerConfig = (Map<String, Object>) Ndb.execute(config, "one:thread->router");
		
		int routerCounter = 1;
		if (routerConfig != null && routerConfig.containsKey("count")) {
			try {
				routerCounter = Integer.parseInt((String) routerConfig.get("count"));
			} catch (NumberFormatException e) {
				routerCounter = 1;
			}
		}
		
		// 路由最小数量为1
		if (routerCounter < 1) {
			routerCounter = 1;
		}

		logger.info(String.format("Starting message router, router count is %d", routerCounter));

		for (int i = 0; i < routerCounter; i++) {
			new RouterProcessor(config).start("router-" + Integer.toString(i));
		}

		long startupTime = System.currentTimeMillis() - startTime;
		logger.info(String.format("Start message router in %d ms", startupTime));
	}

	/**
	 * 启动消息归并器
	 * 
	 * @param config 配置信息
	 */

	/**
	 * 启动消息自定义处理器
	 * 
	 * @param config 配置信息
	 */
	@SuppressWarnings("unchecked")
	public void startCustomProcessor(Map<String, Object> config) {
		long startTime = System.currentTimeMillis();

		Map<String, Object> processorConfig = (Map<String, Object>) Ndb.execute(config, "one:thread->processor");
		
		int processorCounter = 1;
		if (processorConfig != null && processorConfig.containsKey("count")) {
			try {
				processorCounter = Integer.parseInt((String) processorConfig.get("count"));
			} catch (NumberFormatException e) {
				processorCounter = 1;
			}
		}
		
		// 过滤器最小数量为1
		if (processorCounter < 1) {
			processorCounter = 1;
		}

		logger.info(String.format("Starting customer message processor, processor count is %d", processorCounter));

		for (int i = 0; i < processorCounter; i++) {
			new CustomProcessor(config).start("processor-" + Integer.toString(i));
		}

		long startupTime = System.currentTimeMillis() - startTime;
		logger.info(String.format("Start customer message processor in %d ms", startupTime));
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
		logger.info(String.format("Start outputor in %d ms", startupTime));
	}

	/**
	 * 停止Logmesh
	 */
	public void stop() {
		ProcessorPool.stopAll();

		logger.info("Logmesh STOP");
	}

	/**
	 * Logmesh是否启动完成
	 */
	public boolean isStartup() {
		return this.startup;
	}
}
