package org.pinae.logmesh.server;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.pinae.logmesh.component.ComponentFactory;
import org.pinae.logmesh.component.custom.MessageProcessorFactory;
import org.pinae.logmesh.component.filter.MessageFilterFactory;
import org.pinae.logmesh.component.router.MessageRouterFactory;
import org.pinae.logmesh.message.MessagePool;
import org.pinae.logmesh.output.MessageOutputorFactory;
import org.pinae.logmesh.processor.ProcessorPool;
import org.pinae.logmesh.processor.imp.CustomProcessor;
import org.pinae.logmesh.processor.imp.FilterProcessor;
import org.pinae.logmesh.processor.imp.OutputorProcessor;
import org.pinae.logmesh.processor.imp.RouterProcessor;
import org.pinae.logmesh.receiver.AbstractReceiver;
import org.pinae.logmesh.receiver.event.kafka.KafkaReceiver;
import org.pinae.logmesh.receiver.event.netty.TCPReceiver;
import org.pinae.logmesh.receiver.event.netty.UDPReceiver;
import org.pinae.logmesh.receiver.pollable.redis.RedisWatcher;
import org.pinae.logmesh.receiver.pollable.txtfile.FileWatcher;
import org.pinae.logmesh.server.helper.MessageCounter;
import org.pinae.logmesh.server.helper.OriginalMessageStorer;
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
	/* 配置文件 */
	private File serverFile;

	/* 配置信息 */
	private Map<String, Object> config;
	/* 是否启动完成 */
	private boolean startup = false;
	
	private MessageCounter messageCounter = null; 
	
	private SimpleDateFormat dateFmt = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

	public MessageServer() {
		
	}
	
	public MessageServer(String serverFilename) {
		this(FileUtils.getFile(serverFilename));
	}
	
	public MessageServer(File serverFile) {
		this.serverFile = serverFile;
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
			if (this.serverFile != null && this.serverFile.exists() && this.serverFile.isFile()) {
				if (this.serverFile != null) {
					this.config = loadConfig(this.serverFile);
				} else {
					logger.error(String.format("Server config %s loaded FAIL", this.serverFile.getPath()));
				}
			} else {
				logger.error("Server Config is NULL");
			}
		}
		
		if (this.config != null) {
			// 载入消息队列
			MessagePool.initialize(this.config);
	
			// 启动消息展示器
			startOutputor(this.config);
	
			// 启动自定义处理器
			startCustomProcessor(this.config);
	
			// 启动消息路由器
			startRouter(this.config);
	
			// 启动过滤器
			startFilter(this.config);
	
			// 启动消息计数器
			startMessageCounter(this.config);
	
			// 启动原始消息存储器
			startOriginaMessageStorer(this.config);
	
			// 启动接收器
			startReceiver(this.config);
	
			this.startup = true;
	
			long startupTime = System.currentTimeMillis() - startTime;
			
			logger.info(String.format("Start Logmesh at %s in %d ms", dateFmt.format(new Date()), startupTime));
		} else {
			logger.error("Server configurtion is NULL and start FAIL");
		}
	}

	/**
	 * 载入Logmesh配置信息
	 * 
	 * @param file Logmesh配置文件(XML文件)
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
			File importFile = FileUtils.getFile(importFilename);
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
				} else if (type.equalsIgnoreCase("file")) {
					receiver = new FileWatcher();
					name = "FileWatcher";
				} else if (type.equalsIgnoreCase("redis")) {
					receiver = new RedisWatcher();
					name = "RedisWatcher";
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
					logger.error(String.format("Start Receiver Error: exception=%s", e.getMessage()));
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
		
		if (originalConfig != null) {
			OriginalMessageStorer messageStorer = new OriginalMessageStorer(ComponentFactory.createParameter(originalConfig));
			messageStorer.start();
	
			long startupTime = System.currentTimeMillis() - startTime;
			logger.info(String.format("Start Origina Storer in %d ms", startupTime));
		}
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
		
		if (counterConfig != null) {
			this.messageCounter = new MessageCounter(ComponentFactory.createParameter(counterConfig));
	
			messageCounter.start("MessageCounter");
	
			long startupTime = System.currentTimeMillis() - startTime;
			logger.info(String.format("Start Message Counter in %d ms", startupTime));
		}
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
		
		List<Map<String, Object>> filterConfigList = MessageFilterFactory.getFilterConfigList(FilterProcessor.GLOBAL_FILTER, config);
		
		logger.info(String.format("Starting Message Filter, %d Threads", filterCounter));

		boolean enableCounter = this.messageCounter != null ? this.messageCounter.isRunning() : false;
		for (int i = 0; i < filterCounter; i++) {
			new FilterProcessor(filterConfigList, enableCounter).start("filter-" + Integer.toString(i));
		}

		long startupTime = System.currentTimeMillis() - startTime;
		logger.info(String.format("Start Message Filter in %d ms", startupTime));
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
		
		List<Map<String, Object>> routerConfigList = MessageRouterFactory.getRouterConfigList(config);

		logger.info(String.format("Starting Message Router, %d Threads", routerCounter));

		for (int i = 0; i < routerCounter; i++) {
			new RouterProcessor(routerConfigList).start("router-" + Integer.toString(i));
		}

		long startupTime = System.currentTimeMillis() - startTime;
		logger.info(String.format("Start Message Router in %d ms", startupTime));
	}

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
		
		List<Map<String, Object>> processorConfigList = MessageProcessorFactory.getProcessorConfigList(CustomProcessor.GLOBAL_PROCESSOR, config);

		logger.info(String.format("Starting Message Processor, %d Threads", processorCounter));

		for (int i = 0; i < processorCounter; i++) {
			new CustomProcessor(processorConfigList).start("processor-" + Integer.toString(i));
		}

		long startupTime = System.currentTimeMillis() - startTime;
		logger.info(String.format("Start Message Processor in %d ms", startupTime));
	}

	/**
	 * 启动消息展示器
	 * 
	 * @param config 配置信息
	 */
	public void startOutputor(Map<String, Object> config) {
		long startTime = System.currentTimeMillis();
		
		List<Map<String, Object>> outputorConfigList = MessageOutputorFactory.getOutputorConfigList(config);

		new OutputorProcessor(outputorConfigList).start("outputor");

		long startupTime = System.currentTimeMillis() - startTime;
		logger.info(String.format("Start Message Outputor in %d ms", startupTime));
	}

	/**
	 * 停止Logmesh
	 */
	public void stop() {
		ProcessorPool.stopAll();

		logger.info("Logmesh Stopped AT " + dateFmt.format(new Date()));
	}

	/**
	 * Logmesh是否启动完成
	 * 
	 * @return 启动是否完成
	 */
	public boolean isStartup() {
		return this.startup;
	}
}
