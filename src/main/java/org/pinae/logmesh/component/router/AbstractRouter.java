package org.pinae.logmesh.component.router;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.pinae.logmesh.component.MessageProcessor;
import org.pinae.logmesh.component.filter.MessageFilter;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.output.MessageOutputor;
import org.pinae.logmesh.processor.ProcessorInfo;
import org.pinae.logmesh.processor.imp.CustomProcessor;
import org.pinae.logmesh.processor.imp.FilterProcessor;
import org.pinae.logmesh.processor.imp.OutputorProcessor;
import org.pinae.logmesh.util.ClassLoaderUtils;
import org.pinae.logmesh.util.FileUtils;
import org.pinae.nala.xb.Xml;
import org.pinae.ndb.Statement;

/**
 * 抽象消息路由器
 * 
 * @author Huiyugeng
 * 
 */
public abstract class AbstractRouter extends ProcessorInfo implements MessageRouter {
	private static Logger logger = Logger.getLogger(AbstractRouter.class);

	protected Statement statement = new Statement();

	/* 消息过滤器列表, (路由器名称, 消息过滤器) */
	private Map<String, List<MessageFilter>> filterMap = new HashMap<String, List<MessageFilter>>(); 
	/* 消息处理器列表, (路由器名称, 消息处理器) */
	private Map<String, List<MessageProcessor>> processorMap = new HashMap<String, List<MessageProcessor>>(); 
	/* 消息转发器列表, (路由器名称, 消息转发器) */
	private Map<String, List<MessageOutputor>> outputorMap = new HashMap<String, List<MessageOutputor>>();

	/* 消息路由条件 */
	protected Map<String, Map<String, Object>> routerRuleMap = new HashMap<String, Map<String, Object>>(); 

	public void initialize() {
		String path = ClassLoaderUtils.getResourcePath("");
		String routerFilename = getStringValue("file", "router.xml");

		File routerFile = FileUtils.getFile(path, routerFilename);
		if (routerFile != null) {
			loadConfig(routerFile);
		} else {
			logger.error(String.format("Router Load Exception: exception=File doesn't extis, file=%s/%s", path, routerFilename));
		}
	}

	@SuppressWarnings("unchecked")
	private void loadConfig(File routerFile) {
		Map<String, Object> routerConfig = null;
		try {
			if (routerFile != null) {
				routerConfig = (Map<String, Object>) Xml.toMap(routerFile, "UTF8");
				if (routerConfig != null && routerConfig.containsKey("import")) {
					List<String> importFilenameList = (List<String>) statement.execute(routerConfig, "select:import->file");
					for (String importFilename : importFilenameList) {
						if (StringUtils.isNotEmpty(importFilename)) {
							File importFile = FileUtils.getFile(routerFile.getParent(), importFilename);
							if (importFile != null) {
								loadConfig(importFile);
							} else {
								logger.error(String.format("Router Load Exception: exception=File doesn't extis, source=%s, import=%s/%s", 
										routerFile.getPath(), routerFile.getAbsolutePath(), importFilename));
							}
							
						}
					}
				}

				if (routerConfig != null && routerConfig.containsKey("rule")) {
					List<Map<String, Object>> ruleConfigList = (List<Map<String, Object>>) statement.execute(routerConfig,
							"select:rule");
					for (Map<String, Object> ruleConfig : ruleConfigList) {

						String name = ruleConfig.containsKey("name") ? ruleConfig.get("name").toString() : ruleConfig
								.toString();
						
						this.routerRuleMap.put(name, (Map<String, Object>) statement.execute(ruleConfig, "one:condition"));

						List<MessageFilter> filterList = new ArrayList<MessageFilter>();
						List<MessageProcessor> processorList = new ArrayList<MessageProcessor>();
						List<MessageOutputor> outputorList = new ArrayList<MessageOutputor>();

						String extend = ruleConfig.containsKey("extend") ? ruleConfig.get("extend").toString() : null;

						if (extend != null) {
							filterList.addAll(this.filterMap.get(extend));
							processorList.addAll(this.processorMap.get(extend));
							outputorList.addAll(this.outputorMap.get(extend));
						}

						filterList.addAll(FilterProcessor.load(ruleConfig));
						processorList.addAll(CustomProcessor.load(ruleConfig));
						outputorList.addAll(OutputorProcessor.load(ruleConfig));

						this.filterMap.put(name, filterList);
						this.processorMap.put(name, processorList);
						this.outputorMap.put(name, outputorList);
					}
				}
			} 
		} catch (Exception e) {
			logger.error(String.format("Router Load Exception: exception=%s", e.getMessage()));
		} 


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
						outputor.showMessage(message);
					}
				}
			}
		}
	}

	public abstract String match(Message message);

}
