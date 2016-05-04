package org.pinae.logmesh.processor.imp;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.pinae.logmesh.component.ComponentPool;
import org.pinae.logmesh.component.router.BasicRouter;
import org.pinae.logmesh.component.router.MessageRouter;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.message.MessagePool;
import org.pinae.logmesh.processor.Processor;
import org.pinae.logmesh.processor.ProcessorFactory;
import org.pinae.ndb.Statement;

/**
 * 
 * 消息路由器线程
 * 
 * @author Huiyugeng
 *
 */
public class RouterProcessor implements Processor {

	private static Logger logger = Logger.getLogger(RouterProcessor.class);

	/* 消息路由器配置信息 */
	private Map<String, Object> config;
	
	/* 消息路由处理组件列表  */
	private List<MessageRouter> routerList = new ArrayList<MessageRouter>();

	private Statement statement = new Statement();

	/* 消息路由线程是否停止 */
	private boolean isStop = false; // 

	public RouterProcessor(Map<String, Object> config) {
		this.config = config;
	}

	/**
	 * 载入消息路由列表
	 * 
	 * @return 消息路由列表
	 */
	@SuppressWarnings("unchecked")
	public List<MessageRouter> load() {
		List<MessageRouter> routerList = new ArrayList<MessageRouter>();

		// 选取需要启动的路由器
		List<Map<String, Object>> routerConfigList = (List<Map<String, Object>>) statement.execute(config,
				"select:router->enable:true");

		for (Map<String, Object> routerConfig : routerConfigList) {

			Object routerObject = ProcessorFactory.create(routerConfig);

			if (routerObject != null && routerObject instanceof MessageRouter) {
				MessageRouter router = (MessageRouter) routerObject;
				// 调用路由器初始化
				router.initialize(); 
				// 加入路由队列
				routerList.add(router); 
			}
		}

		if (routerList.size() == 0) {
			routerList.add(new BasicRouter());
		}

		return routerList;

	}

	public void start(String name) {

		this.routerList = load();

		// 将路由在组件池中进行注册
		for (MessageRouter router : this.routerList) {
			ComponentPool.registeComponent(router);
		}

		this.isStop = false; // 设置线程启动标记
		ProcessorFactory.getThread(name, this).start(); // 启动消息路由线程
	}

	public void stop() {
		this.isStop = true; // 设置线程停止标记

		logger.info("Message ROUTE STOP");

	}

	public void run() {
		while (!isStop) {
			while (!MessagePool.ROUTE_QUEUE.isEmpty()) {
				Message message = MessagePool.ROUTE_QUEUE.poll();

				if (message != null) {
					for (MessageRouter route : routerList) {
						if (route != null) {
							try {
								route.porcess(message.clone());
							} catch (CloneNotSupportedException e) {
								logger.error("RouterProcessor Message Clone Exception: exception=" + e.getMessage());
							}
						}
					}
				}

			}
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				logger.error("RouterProcessor InterruptedException: exception=" + e.getMessage());
			}
		}
	}

}
