package org.pinae.logmesh.component.custom;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.pinae.logmesh.component.ComponentFactory;
import org.pinae.logmesh.component.MessageComponent;
import org.pinae.logmesh.component.custom.impl.BasicCustomProcessor;
import org.pinae.logmesh.processor.imp.CustomProcessor;
import org.pinae.ndb.Ndb;

public class MessageProcessorFactory {
	
	private static Logger logger = Logger.getLogger(MessageProcessorFactory.class);
	
	/**
	 * 载入消息处理器列表
	 * 
	 * @param config 消息处理器配置信息
	 * 
	 * @return 消息处理器列表
	 */
	
	public static List<MessageProcessor> create(List<Map<String, Object>> processorConfigList) {
		Map<Integer, MessageProcessor> processorMap = new TreeMap<Integer, MessageProcessor>();

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

	@SuppressWarnings("unchecked")
	public static List<Map<String, Object>> getProcessorConfigList(String processorStage, Map<String, Object> config) {
		// 选取需要启动的处理器
		List<Map<String, Object>> processorConfigList = (List<Map<String, Object>>) Ndb.execute(config,
				"select:processor->enable:true");
		
		/* ROUTER获取过滤器时不进行过滤器日志输出 */
		if (CustomProcessor.GLOBAL_PROCESSOR.equals(processorStage)) {
			for (Map<String, Object> processorConfig : processorConfigList) {
				String name = (String)processorConfig.get("name");
				String className = (String)processorConfig.get("kwClass");
				if (StringUtils.isNotEmpty(name) && StringUtils.isNotEmpty(className)) {
					logger.info(String.format("Start Processor %s (%s)", name, className));
				}
			}
		}
		
		return processorConfigList;
	}
	
}
