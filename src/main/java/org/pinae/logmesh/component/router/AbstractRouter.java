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
import org.pinae.nala.xb.Xml;
import org.pinae.ndb.Statement;

/**
 * 抽象消息路由器
 * 
 * @author Huiyugeng
 * 
 * 
 */
public abstract class AbstractRouter extends ProcessorInfo implements MessageRouter {
	private static Logger log = Logger.getLogger(AbstractRouter.class);

	protected Statement statement = new Statement();

	private Map<String, Object> routerConfig; // 消息路由器配置

	private Map<String, List<MessageFilter>> filterMap = new HashMap<String, List<MessageFilter>>(); // 消息过滤器列表
	private Map<String, List<MessageProcessor>> processorMap = new HashMap<String, List<MessageProcessor>>(); // 消息处理器列表
	private Map<String, List<MessageOutputor>> outputorMap = new HashMap<String, List<MessageOutputor>>(); // 消息转发器列表

	protected Map<String, Map<String, Object>> routerRuleMap = new HashMap<String, Map<String, Object>>(); // 消息路由条件

	public void init() {
		String path = ClassLoaderUtils.getResourcePath("");
		String routerFile = getParameter("file");

		loadConfig(path, routerFile);
	}

	@SuppressWarnings("unchecked")
	private void loadConfig(String path, String filename) {

		try {
			this.routerConfig = (Map<String, Object>) Xml.toMap(new File(path + filename), "UTF8");
		} catch (Exception e) {
			log.error(String.format("Router Load Exception: exception=%s", e.getMessage()));
		} 

		if (this.routerConfig != null && this.routerConfig.containsKey("import")) {
			List<String> importList = (List<String>) statement.execute(this.routerConfig, "select:import->file");
			for (String file : importList) {
				if (StringUtils.isNotEmpty(file)) {
					loadConfig(path, file);
				}
			}
		}

		if (this.routerConfig != null && this.routerConfig.containsKey("rule")) {
			List<Map<String, Object>> ruleConfigList = (List<Map<String, Object>>) statement.execute(this.routerConfig,
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

	/**
	 * 获取消息路由器配置信息
	 * 
	 * @return 消息路由配置信息
	 */
	public Map<String, Object> getRouteConfig() {
		return this.routerConfig;
	}

	public void porcess(Message message) {

		String rule = match(message);

		if (rule != null) {

			// 执行消息过滤器
			List<MessageFilter> filterList = this.filterMap.get(rule);
			for (MessageFilter filter : filterList) {
				message = filter.filter(message);

				if (message == null) {
					break;
				}
			}

			if (message != null) {
				// 执行自定义处理器
				List<MessageProcessor> processorList = this.processorMap.get(rule);
				for (MessageProcessor processor : processorList) {
					processor.porcess(message);
				}
			}
			
			if (message != null) {
				// 执行消息转发器
				List<MessageOutputor> outputorList = this.outputorMap.get(rule);
				for (MessageOutputor outputor : outputorList) {
					outputor.showMessage(message);
				}
			}
		}
	}

	public abstract String match(Message message);

}
