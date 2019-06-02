package org.pinae.logmesh.component.router;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.pinae.logmesh.component.ComponentFactory;
import org.pinae.logmesh.component.MessageComponent;
import org.pinae.logmesh.component.router.impl.BasicRouter;
import org.pinae.ndb.Ndb;

public class MessageRouterFactory {
	
	private static Logger logger = Logger.getLogger(MessageRouterFactory.class);
	
	/**
	 * 载入消息路由列表
	 * 
	 * @return 消息路由列表
	 */
	public static List<MessageRouter> create(List<Map<String, Object>> routerConfigList) {
		List<MessageRouter> routerList = new ArrayList<MessageRouter>();

		for (Map<String, Object> routerConfig : routerConfigList) {

			MessageComponent routerComponent = ComponentFactory.create(routerConfig);

			if (routerComponent != null && routerComponent instanceof MessageRouter) {
				MessageRouter router = (MessageRouter) routerComponent;
				// 调用路由器初始化
				router.initialize();
				
				// 加入路由队列
				if (router != null) {
					routerList.add(router);
				}
			}
		}

		if (routerList.size() == 0) {
			routerList.add(new BasicRouter());
		}

		return routerList;
	}
	
	@SuppressWarnings("unchecked")
	public static List<Map<String, Object>> getRouterConfigList(Map<String, Object> config) {
		List<Map<String, Object>> routerConfigList = (List<Map<String, Object>>)Ndb.execute(config,
				"select:router->enable:true");
		
		for (Map<String, Object> routerConfig : routerConfigList) {
			String name = (String)routerConfig.get("name");
			String className = (String)routerConfig.get("kwClass");
			if (StringUtils.isNotEmpty(name) && StringUtils.isNotEmpty(className)) {
				logger.info(String.format("Start Message Router %s (%s)", name, className));
			}
		}
		
		return routerConfigList;
	}
}
