package org.pinae.logmesh.processor.imp;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.pinae.logmesh.component.ComponentPool;
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

	/* 是否启用归并消息 */
	private boolean merger = false;
	/* 消息过滤线程是否停止 */
	private boolean isStop = false;

	public FilterProcessor(Map<String, Object> config) {
		this.config = config;
	}

	/**
	 * 载入消息过滤器列表
	 * 
	 * @return 消息过滤器列表
	 */
	@SuppressWarnings("unchecked")
	public static List<MessageFilter> create(Map<String, Object> config) {
		List<MessageFilter> filterList = new ArrayList<MessageFilter>();

		// 选取需要启动的过滤器
		List<Map<String, Object>> filterConfigList = (List<Map<String, Object>>)Ndb.execute(config,
				"select:filter->enable:true");
		
		// 顺序过滤器列表
		SortedMap<Integer, MessageFilter> filtersWithStartup = new TreeMap<Integer, MessageFilter>(); 
		// 无序过滤器列表
		List<MessageFilter> filtersWithoutStartup = new ArrayList<MessageFilter>(); 

		for (Map<String, Object> filterConfig : filterConfigList) {

			Object filterObject = ProcessorFactory.create(filterConfig);

			if (filterObject != null && filterObject instanceof MessageFilter) {
				MessageFilter filter = (MessageFilter) filterObject;
				 // 调用过滤器初始化
				filter.initialize();

				if (filterConfig.containsKey("startup")) {
					String startup = (String) filterConfig.get("startup");
					if (StringUtils.isAlphanumeric(startup)) {
						 // 加入顺序过滤器队列
						filtersWithStartup.put(Integer.parseInt(startup), filter);
					}
				} else {
					 // 加入无序过滤器队列
					filtersWithoutStartup.add(filter);
				}
			}
		}

		// 将顺序过滤器和无序过滤器进行合并
		filterList.addAll(filtersWithStartup.values());
		filterList.addAll(filtersWithoutStartup);

		// 如果处理器队列中不含任何过滤器则启动默认过滤器
		if (filterList.size() == 0) {
			filterList.add(new BasicFilter());
		}

		return filterList;
	}

	public void start(String name) {
		this.filterList = create(this.config);
		// 将过滤器在组件池中进行注册
		for (MessageFilter filter : this.filterList) {
			ComponentPool.registe(filter);
		}

		// 是否启动消息归并
		if (((List<?>) Ndb.execute(config, "select:thread->merger->enable:true")).size() > 0) {
			merger = true;
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
						if (merger == true) {
							MessagePool.MERGER_QUEUE.offer(message); // 将消息进行合并
						} else {
							MessagePool.ROUTE_QUEUE.offer(message); // 将消息填入路由队列中
							MessagePool.PROCESSOR_QUEUE.offer(message); // 将消息填入自定义处理器队列中
						}
						MessagePool.COUNTER_QUEUE.offer(message); // 将消息填入统计队列中
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
