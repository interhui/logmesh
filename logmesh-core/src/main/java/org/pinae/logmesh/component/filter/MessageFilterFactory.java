package org.pinae.logmesh.component.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.pinae.logmesh.component.ComponentFactory;
import org.pinae.logmesh.component.MessageComponent;
import org.pinae.logmesh.component.filter.impl.BasicFilter;
import org.pinae.logmesh.processor.imp.FilterProcessor;
import org.pinae.ndb.Ndb;

public class MessageFilterFactory {
	
	private static Logger logger = Logger.getLogger(MessageFilterFactory.class);
	
	/**
	 * 载入消息过滤器列表
	 * 
	 * @param config 消息过滤器配置信息
	 * 
	 * @return 消息过滤器列表
	 */
	public static List<MessageFilter> create(List<Map<String, Object>> filterConfigList) {
		Map<Integer, MessageFilter> filterMap = new TreeMap<Integer, MessageFilter>();
		
		for (Map<String, Object> filterConfig : filterConfigList) {

			MessageComponent filterComponent = ComponentFactory.create(filterConfig);

			if (filterComponent != null && filterComponent instanceof MessageFilter) {
				MessageFilter filter = (MessageFilter) filterComponent;
				// 调用过滤器初始化
				filter.initialize();

				// 判断是否包含过滤器顺序
				if (filterConfig.containsKey("startup")) {
					String startup = (String) filterConfig.get("startup");
					if (StringUtils.isAlphanumeric(startup)) {
						filterMap.put(Integer.parseInt(startup), filter);
					}
				} else {
					int index = (int)(Math.random() * Integer.MAX_VALUE);
					while (filterMap.containsKey(index)) {
						index = (int)(Math.random() * Integer.MAX_VALUE);
					}
					filterMap.put(index, filter);
				}
			}
		}

		// 如果处理器队列中不含任何过滤器则启动默认过滤器
		if (filterMap.isEmpty()) {
			filterMap.put(0, new BasicFilter());
		}

		return new ArrayList<MessageFilter>(filterMap.values());
	}
	
	/**
	 * 获取过滤器配置列表
	 * 
	 * @param filterStage 过滤器阶段, global 全局过滤器, router 路由过滤器
	 * @param config 服务配置信息
	 * 
	 * @return 过滤器配置列表
	 */
	@SuppressWarnings("unchecked")
	public static List<Map<String, Object>> getFilterConfigList(String filterStage, Map<String, Object> config) {
		List<Map<String, Object>> filterConfigList = (List<Map<String, Object>>)Ndb.execute(config,
				"select:filter->enable:true");
		
		/* ROUTER获取过滤器时不进行过滤器日志输出 */
		if (FilterProcessor.GLOBAL_FILTER.equals(filterStage)) {
			for (Map<String, Object> fileterConfig : filterConfigList) {
				String name = (String)fileterConfig.get("name");
				String className = (String)fileterConfig.get("kwClass");
				if (StringUtils.isNotEmpty(name) && StringUtils.isNotEmpty(className)) {
					logger.info(String.format("Start Message Filter %s (%s)", name, className));
				}
			}
		}
		
		return filterConfigList;
	}
}
