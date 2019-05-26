package org.pinae.logmesh.processor.imp;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.pinae.logmesh.component.ComponentPool;
import org.pinae.logmesh.component.router.MessageRouter;
import org.pinae.logmesh.component.router.MessageRouterFactory;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.message.MessagePool;
import org.pinae.logmesh.processor.Processor;
import org.pinae.logmesh.processor.ProcessorFactory;

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
	private List<Map<String, Object>> routerConfigList;
	
	/* 消息路由处理组件列表  */
	private List<MessageRouter> routerList = new ArrayList<MessageRouter>();

	/* 消息路由线程是否停止 */
	private boolean isStop = false; // 

	public RouterProcessor(List<Map<String, Object>> routerConfigList) {
		this.routerConfigList = routerConfigList;
	}

	public void start(String name) {

		this.routerList = MessageRouterFactory.create(this.routerConfigList);

		// 将路由在组件池中进行注册
		for (MessageRouter router : this.routerList) {
			ComponentPool.registe(router);
		}

		this.isStop = false; // 设置线程启动标记
		ProcessorFactory.getThread(name, this).start(); // 启动消息路由线程
	}

	public void stop() {
		this.isStop = true; // 设置线程停止标记

		logger.info("Message ROUTE STOP");

	}
	
	public boolean isRunning() {
		return !this.isStop;
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
