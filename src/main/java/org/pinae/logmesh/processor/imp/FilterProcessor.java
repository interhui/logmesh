package org.pinae.logmesh.processor.imp;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.pinae.logmesh.component.ComponentPool;
import org.pinae.logmesh.component.filter.MessageFilterFactory;
import org.pinae.logmesh.component.filter.MessageFilter;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.message.MessagePool;
import org.pinae.logmesh.processor.Processor;
import org.pinae.logmesh.processor.ProcessorFactory;

/**
 * 
 * 消息过滤器线程
 * 
 * @author Huiyugeng
 * 
 */
public class FilterProcessor implements Processor {
	
	private static Logger logger = Logger.getLogger(FilterProcessor.class);
	
	public static final String GLOBAL_FILTER = "global";
	
	public static final String ROUTER_FILTER = "router";

	/* 消息过滤器配置信息 */
	private List<Map<String, Object>> filterConfigList;

	/* 消息过滤组件列表 */
	private List<MessageFilter> filterList = new ArrayList<MessageFilter>();

	/* 消息过滤线程是否停止 */
	private boolean isStop = false;
	
	private boolean enableCounter = false;

	public FilterProcessor(List<Map<String, Object>> filterConfigList, boolean enableCounter) {
		this.filterConfigList = filterConfigList;
		this.enableCounter = enableCounter;
	}

	public void start(String name) {
		this.filterList = MessageFilterFactory.create(this.filterConfigList);
		// 将过滤器在组件池中进行注册
		for (MessageFilter filter : this.filterList) {
			ComponentPool.registe(filter);
		}

		// 设置线程启动标记
		this.isStop = false;
		// 启动消息过滤线程
		ProcessorFactory.getThread(name, this).start();
	}

	public void stop() {
		// 设置线程停止标记
		this.isStop = true;
		logger.info("Message Filter STOP");
	}
	
	public boolean isRunning() {
		return !this.isStop;
	}

	public void run() {
		while (!isStop) {

			while (!MessagePool.FILTER_QUEUE.isEmpty()) {
				Message message = MessagePool.FILTER_QUEUE.poll();

				if (message != null) {
					for (MessageFilter filter : filterList) {
						if (filter != null) {
							message = filter.filter(message);
							if (message == null) {
								break;
							}
						}
					}

					if (message != null && message.getMessage() != null) {
						MessagePool.ROUTE_QUEUE.offer(message); // 将消息填入路由队列中
						MessagePool.PROCESSOR_QUEUE.offer(message); // 将消息填入自定义处理器队列中
						
						if (this.enableCounter) {
							MessagePool.COUNTER_QUEUE.offer(message); // 将消息填入统计队列中
						}
					}
				}
			}
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				logger.error(String.format("FilterProcessor Exception: exception=%s", e.getMessage()));
			}
		}

	}

}
