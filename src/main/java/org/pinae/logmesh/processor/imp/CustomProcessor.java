package org.pinae.logmesh.processor.imp;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.pinae.logmesh.component.ComponentFactory;
import org.pinae.logmesh.component.ComponentPool;
import org.pinae.logmesh.component.MessageComponent;
import org.pinae.logmesh.component.MessageProcessor;
import org.pinae.logmesh.component.custom.BasicCustomProcessor;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.message.MessagePool;
import org.pinae.logmesh.processor.Processor;
import org.pinae.logmesh.processor.ProcessorFactory;
import org.pinae.ndb.Ndb;

/**
 * 
 * 自定义处理器线程
 * 
 * @author Huiyugeng
 * 
 */
public class CustomProcessor implements Processor {
	private static Logger logger = Logger.getLogger(CustomProcessor.class);

	/* 消息处理器配置信息 */
	private Map<String, Object> config;

	/* 自定义消息处理组件列表 */
	private List<MessageProcessor> processorList = new ArrayList<MessageProcessor>();

	/* 自定义消息处理线程是否停止 */
	private boolean isStop = false;

	public CustomProcessor(Map<String, Object> config) {
		this.config = config;
	}

	/**
	 * 载入消息处理器列表
	 * 
	 * @return 消息处理器列表
	 */
	@SuppressWarnings("unchecked")
	public static List<MessageProcessor> create(Map<String, Object> config) {
		Map<Integer, MessageProcessor> processorMap = new TreeMap<Integer, MessageProcessor>();

		// 选取需要启动的处理器
		List<Map<String, Object>> processorConfigList = (List<Map<String, Object>>) Ndb.execute(config,
				"select:processor->enable:true");

		for (Map<String, Object> processorConfig : processorConfigList) {

			MessageComponent processorComponent = ComponentFactory.create((Map<String, Object>) processorConfig);

			if (processorComponent != null && processorComponent instanceof MessageProcessor) {
				MessageProcessor processor = (MessageProcessor) processorComponent;
				// 处理器初始化
				processor.initialize(); 

				// 判断处理器是否包括启动顺序
				if (processorConfig.containsKey("startup")) {
					String startup = (String) processorConfig.get("startup");
					if (StringUtils.isAlphanumeric(startup)) {
						processorMap.put(Integer.parseInt(startup), processor); 
					}
				} else {
					int index = (int)(Math.random() * Integer.MAX_VALUE);
					while (processorMap.containsKey(index)) {
						index = (int)(Math.random() * Integer.MAX_VALUE);
					}
					processorMap.put(index, processor);
				}
			}
		}

		if (processorMap.isEmpty()) {
			processorMap.put(0, new BasicCustomProcessor());
		}

		return new ArrayList<MessageProcessor>(processorMap.values());
	}

	public void start(String name) {

		this.processorList = create(this.config);

		// 将过滤器在组件池中进行注册
		for (MessageProcessor processor : this.processorList) {
			ComponentPool.registe(processor);
		}
		// 设置线程启动标记
		this.isStop = false;
		// 启动自定义处理器线程
		ProcessorFactory.getThread(name, this).start(); 
	}

	public void stop() {
		// 设置线程停止标记
		this.isStop = true;
		logger.info("Custom Processor STOP");
	}

	public void run() {
		while (!isStop) {

			while (!MessagePool.PROCESSOR_QUEUE.isEmpty()) {
				Message message = MessagePool.PROCESSOR_QUEUE.poll();
				for (MessageProcessor processor : processorList) {
					if (processor != null && message != null) {
						message = processor.porcess(message);
					}
				}
				
				if (message != null) {
					MessagePool.OUTPUT_QUEUE.offer(message);
				}
			}
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				logger.error(String.format("CustomProcessor Exception: exception=%s", e.getMessage()));
			}
		}
	}

}
