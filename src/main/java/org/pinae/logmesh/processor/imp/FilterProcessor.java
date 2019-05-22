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
import org.pinae.logmesh.component.filter.BasicFilter;
import org.pinae.logmesh.component.filter.MessageFilter;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.message.MessagePool;
import org.pinae.logmesh.processor.Processor;
import org.pinae.logmesh.processor.ProcessorFactory;
import org.pinae.ndb.Ndb;

/**
 * 
 * 消息过滤器线程
 * 
 * @author Huiyugeng
 * 
 */
public class FilterProcessor implements Processor {
	private static Logger logger = Logger.getLogger(FilterProcessor.class);

	/* 消息过滤器配置信息 */
	private Map<String, Object> config;

	/* 消息过滤组件列表 */
	private List<MessageFilter> filterList = new ArrayList<MessageFilter>();

	/* 消息过滤线程是否停止 */
	private boolean isStop = false;
	
	private boolean enableCounter = false;

	public FilterProcessor(Map<String, Object> config, boolean enableCounter) {
		this.config = config;
		this.enableCounter = enableCounter;
	}

	/**
	 * 载入消息过滤器列表
	 * 
	 * @param config 消息过滤器配置信息
	 * 
	 * @return 消息过滤器列表
	 */
	@SuppressWarnings("unchecked")
	public static List<MessageFilter> create(Map<String, Object> config) {
		Map<Integer, MessageFilter> filterMap = new TreeMap<Integer, MessageFilter>();

		// 选取需要启动的过滤器
		List<Map<String, Object>> filterConfigList = (List<Map<String, Object>>)Ndb.execute(config,
				"select:filter->enable:true");
		
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

	public void start(String name) {
		this.filterList = create(this.config);
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
