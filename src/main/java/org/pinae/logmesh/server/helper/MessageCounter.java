package org.pinae.logmesh.server.helper;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.message.MessagePool;
import org.pinae.logmesh.processor.Processor;
import org.pinae.logmesh.processor.ProcessorFactory;
import org.pinae.logmesh.processor.imp.FilterProcessor;

/**
 * 消息计数器
 * 
 * @author Huiyugeng
 * 
 */
public class MessageCounter implements Processor {
	private static Logger log = Logger.getLogger(FilterProcessor.class);

	private Map<String, Map<String, Long>> counterPool = new ConcurrentHashMap<String, Map<String, Long>>(); // 计数器池

	private String counterTypes[] = { "time", "owner", "ip", "type" }; // 计数器类型
	private String enableCounterTypes; // 启动的消息计数器

	private boolean enable = true; // 是否激活计数器

	private boolean isStop = false; // 是否停止过滤线程

	private Map<String, String> config; // 原始消息存储器配置

	private SimpleDateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd hh:MM");

	public MessageCounter(Map<String, String> config) {
		this.config = config;
	}

	/**
	 * 根据计数器类型获得计数数据
	 * 
	 * @param type 计数器类型
	 * 
	 * @return 计数结果
	 */
	public Map<String, Long> getCounter(String type) {
		return getCounter(type, null);
	}
	
	/**
	 * 根据计数器类型和关键字获得计数数据
	 * 
	 * @param type 计数器类型
	 * @param key 关键字
	 * 
	 * @return 计数结果
	 */
	public Map<String, Long> getCounter(String type, String key) {
		Map<String, Long> counter = counterPool.get(type);
		Set<Entry<String, Long>> itemSet = counter.entrySet();

		Map<String, Long> copyCounter = new HashMap<String, Long>();
		for (Entry<String, Long> item : itemSet) {
			String counterKey = item.getKey();
			if (key == null || counterKey.matches(key)) {
				copyCounter.put(item.getKey(), item.getValue());
			}
		}
		return copyCounter;
	}

	/**
	 * 根据计数器类型重置计数器
	 * 
	 * @param type 计数器类型
	 */
	public void resetCounter(String type) {
		counterPool.get(type).clear();
	}

	public void start(String name) {
		// 初始化计数器池
		for (String counterType : counterTypes) {
			counterPool.put(counterType, new ConcurrentHashMap<String, Long>());
		}

		if (config.containsKey("enable") && config.get("enable").equalsIgnoreCase("true")) {
			this.enable = true;

			if (config.containsKey("counter") && StringUtils.isNotEmpty(config.get("counter"))) {
				this.enableCounterTypes = config.get("counter");
			} else {
				this.enableCounterTypes = StringUtils.join(counterTypes, "|");
			}

			log.info("Message Counter Enable");
		} else {
			log.info("Message Counter Disable");
		}

		this.isStop = false; // 设置线程启动标记
		ProcessorFactory.getThread(name, this).start(); // 启动消息计数器线程
	}

	public void stop() {
		this.isStop = true; // 设置线程停止标记
		log.info("Message Counter STOP");
	}

	public void run() {
		while (!isStop) {

			while (!MessagePool.COUNTER_QUEUE.isEmpty()) {
				Message message = MessagePool.COUNTER_QUEUE.poll();

				// 如果计数器开关关闭，线程也要启动并从队列中提取消息，否则队列会出现满溢的情况
				if (!enable) {
					continue;
				}

				if (message != null) {

					String log[] = { timeFormat.format(new Date(message.getTimestamp())), message.getOwner(),
							message.getIP(), message.getType() };

					for (int i = 0; i < counterTypes.length; i++) {
						String counterType = counterTypes[i];
						// 判断计数器是否被启用
						if (!StringUtils.contains(enableCounterTypes, counterType)) {
							continue;
						}
						Map<String, Long> counter = counterPool.get(counterType);

						if (counter != null) {
							String key = log[i];
							Long count = counter.get(key);
							if (count == null || count == 0) {
								count = new Long(1);
							} else {
								count++;
							}
							counter.put(key, count);
						}
						counterPool.put(counterType, counter);
					}
				}

			}
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				log.error(String.format("FilterProcessor Exception: exception=%s", e.getMessage()));
			}
		}
	}

}
