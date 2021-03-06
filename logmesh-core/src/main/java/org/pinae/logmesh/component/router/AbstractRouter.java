package org.pinae.logmesh.component.router;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.pinae.logmesh.component.ComponentInfo;
import org.pinae.logmesh.component.custom.MessageProcessor;
import org.pinae.logmesh.component.custom.MessageProcessorFactory;
import org.pinae.logmesh.component.filter.MessageFilter;
import org.pinae.logmesh.component.filter.MessageFilterFactory;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.output.MessageOutputor;
import org.pinae.logmesh.output.MessageOutputorFactory;
import org.pinae.logmesh.processor.imp.CustomProcessor;
import org.pinae.logmesh.processor.imp.FilterProcessor;
import org.pinae.logmesh.util.FileUtils;
import org.pinae.nala.xb.Xml;
import org.pinae.ndb.Ndb;

/**
 * 抽象消息路由器
 * 
 * @author Huiyugeng
 * 
 */
public abstract class AbstractRouter extends ComponentInfo implements MessageRouter {
	
	private static Logger logger = Logger.getLogger(AbstractRouter.class);

	/* 消息过滤器列表, (路由名称, 消息过滤器) */
	private Map<String, List<MessageFilter>> filterMap = new HashMap<String, List<MessageFilter>>();
	
	/* 消息处理器列表, (路由名称, 消息处理器) */
	private Map<String, List<MessageProcessor>> processorMap = new HashMap<String, List<MessageProcessor>>();
	
	/* 消息转发器列表, (路由名称, 消息转发器) */
	private Map<String, List<MessageOutputor>> outputorMap = new HashMap<String, List<MessageOutputor>>();

	/* 消息路由条件 */
	protected Map<String, Map<String, Object>> ruleMap = new HashMap<String, Map<String, Object>>(); 

	public void initialize() {
		String routerFilename = getStringValue("file", "router.xml");
		File routerFile = FileUtils.getFile(routerFilename);
		
		load(routerFile);
	}
	
	@SuppressWarnings("unchecked")
	private void load(File routerFile) {
		
		Map<String, Object> routerConfig = null;
		try {
			if (routerFile != null) {
				routerConfig = (Map<String, Object>) Xml.toMap(routerFile, "UTF8");
				if (routerConfig != null && routerConfig.containsKey("import")) {
					List<String> importFilenameList = (List<String>)Ndb.execute(routerConfig, "select:import->file");
					for (String importFilename : importFilenameList) {
						if (StringUtils.isNotEmpty(importFilename)) {
							File importFile = FileUtils.getFile(routerFile.getParent(), importFilename);
							if (importFile != null) {
								load(importFile);
							} else {
								logger.error(String.format("Router Load Exception: exception=File doesn't extis, source=%s, import=%s/%s", 
										routerFile.getPath(), routerFile.getAbsolutePath(), importFilename));
							}
							
						}
					}
				}

				if (routerConfig != null && routerConfig.containsKey("rule")) {
					
					List<Map<String, Object>> ruleConfigList = (List<Map<String, Object>>) Ndb.execute(routerConfig,
							"select:rule");
					
					for (Map<String, Object> ruleConfig : ruleConfigList) {

						String ruleName = ruleConfig.containsKey("name") ? ruleConfig.get("name").toString() : ruleConfig.toString();
						
						Map<String, Object> rules = (Map<String, Object>) Ndb.execute(ruleConfig, "one:condition");
						
						if (ruleName != null && rules != null && rules.size() > 0) {
							ruleMap.put(ruleName, rules);
							String extend = ruleConfig.containsKey("extend") ? ruleConfig.get("extend").toString() : null;
	
							loadFilter(ruleName, ruleConfig, extend);
							loadProcessor(ruleName, ruleConfig, extend);
							loadOutputor(ruleName, ruleConfig, extend);
						}
					}
				}
			} 
		} catch (Exception e) {
			logger.error(String.format("Router Load Exception: exception=%s", e.getMessage()));
		}	
	}
	
	/*
	 * 载入 消息过滤器
	 * 
	 */
	private void loadFilter(String ruleName, Map<String, Object> ruleConfig, String extend) {
		List<MessageFilter> filterList = new ArrayList<MessageFilter>();
		if (extend != null && this.filterMap.containsKey(extend)) {
			filterList.addAll(this.filterMap.get(extend));
		}
		
		List<Map<String, Object>> routerFilterConfigList = MessageFilterFactory.getFilterConfigList(FilterProcessor.ROUTER_FILTER, ruleConfig);
		
		List<MessageFilter> routerFilterList = MessageFilterFactory.create(routerFilterConfigList);
		if (routerFilterList != null) {
			filterList.addAll(routerFilterList);
		}
		this.filterMap.put(ruleName, filterList);
	}
	
	/*
	 * 载入消息处理器
	 * 
	 */
	private void loadProcessor(String ruleName, Map<String, Object> ruleConfig, String extend) {
		List<MessageProcessor> processorList = new ArrayList<MessageProcessor>();
		if (extend != null && this.processorMap.containsKey(extend)) {
			processorList.addAll(this.processorMap.get(extend));
		}
		
		List<Map<String, Object>> routerProcessorConfigList = MessageProcessorFactory.getProcessorConfigList(CustomProcessor.ROUTER_PROCESSOR, ruleConfig);
		
		List<MessageProcessor> routerProcessorList = MessageProcessorFactory.create(routerProcessorConfigList);
		if (routerProcessorList != null) {
			processorList.addAll(routerProcessorList);
		}
		this.processorMap.put(ruleName, processorList);
	}
	
	/*
	 * 载入消息输出器
	 * 
	 */
	private void loadOutputor(String ruleName, Map<String, Object> ruleConfig, String extend) {
		List<MessageOutputor> outputorList = new ArrayList<MessageOutputor>();
		if (extend != null && this.outputorMap.containsKey(extend)) {
			outputorList.addAll(this.outputorMap.get(extend));
		}
		
		List<Map<String, Object>> routerOutputorConfigList = MessageOutputorFactory.getOutputorConfigList(ruleConfig);
		
		List<MessageOutputor> routerOutputorList = MessageOutputorFactory.create(routerOutputorConfigList);
		if (routerOutputorList != null) {
			outputorList.addAll(routerOutputorList);
		}
		this.outputorMap.put(ruleName, outputorList);
	}

	public void porcess(Message message) {
		String rule = match(message);
		if (rule != null) {
			// 执行消息过滤器
			List<MessageFilter> filters = this.filterMap.get(rule);
			if (filters != null) {
				for (MessageFilter filter : filters) {
					message = filter.filter(message);
					if (message == null) {
						break;
					}
				}
			}
			if (message != null) {
				// 执行自定义处理器
				List<MessageProcessor> processors = this.processorMap.get(rule);
				if (processors != null) {
					for (MessageProcessor processor : processors) {
						processor.porcess(message);
					}
				}
			}
			if (message != null) {
				// 执行消息转发器
				List<MessageOutputor> outputors = this.outputorMap.get(rule);
				if (outputors != null) {
					for (MessageOutputor outputor : outputors) {
						outputor.output(message);
					}
				}
			}
		}
	}

	public abstract String match(Message message);

}
