package org.pinae.logmesh.message;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.pinae.ndb.Ndb;

/**
 * 消息队列池
 * 
 * @author Huiyugeng
 * 
 */
public class MessagePool {
	private static Logger logger = Logger.getLogger(MessagePool.class);

	/* 消息过滤队列 */
	public static final MessageQueue FILTER_QUEUE = buildMessageQueue("FILTER_QUEUE");

	/* 消息路由队列 */
	public static final MessageQueue ROUTE_QUEUE = buildMessageQueue("ROUTE_QUEUE");
	
	/* 自定义消息处理队列 */
	public static final MessageQueue PROCESSOR_QUEUE = buildMessageQueue("PROCESSOR_QUEUE");
	
	/* 消息输出队列 */
	public static final MessageQueue OUTPUT_QUEUE = buildMessageQueue("OUTPUT_QUEUE");
	
	/* 原始消息处理队列 */
	public static final MessageQueue ORIGINAL_QUEUE = buildMessageQueue("ORIGINAL_QUEUE");

	/* 计数器队列 */
	public static final MessageQueue COUNTER_QUEUE = buildMessageQueue("COUNTER_QUEUE");

	/* 自定义消息队列 <消息队列名称, 消息队列> */
	public static Map<String, MessageQueue> CUSTOM_MESSAGE_QUEUE = new ConcurrentHashMap<String, MessageQueue>();
	
	public static MessageQueue buildMessageQueue(String name) {
		return new MemoryMessageQueue(name);
	}
	
	public static MessageQueue buildMessageQueue(String name, int maxSize) {
		return new MemoryMessageQueue(name, maxSize);
	}

	@SuppressWarnings("unchecked")
	public static void initialize(Map<String, Object> config) {

		// 初始化固定消息队列
		CUSTOM_MESSAGE_QUEUE.put("FILTER_QUEUE", FILTER_QUEUE);
		CUSTOM_MESSAGE_QUEUE.put("ROUTE_QUEUE", ROUTE_QUEUE);
		CUSTOM_MESSAGE_QUEUE.put("PROCESSOR_QUEUE", PROCESSOR_QUEUE);
		CUSTOM_MESSAGE_QUEUE.put("OUTPUT_QUEUE", OUTPUT_QUEUE);
		CUSTOM_MESSAGE_QUEUE.put("ORIGINAL_QUEUE", ORIGINAL_QUEUE);
		CUSTOM_MESSAGE_QUEUE.put("COUNTER_QUEUE", COUNTER_QUEUE);

		// 初始化自定义消息队列
		List<?> queueConfigList = (List<?>) Ndb.execute(config, "select:queue");

		for (Object queueConfigObject : queueConfigList) {

			if (queueConfigObject instanceof Map) {
				
				Map<String, Object> queueConfigMap = (Map<String, Object>) queueConfigObject;

				if (queueConfigMap.containsKey("name")) {
					String name = (String) queueConfigMap.get("name");
					int size = Integer.MAX_VALUE;
					
					MessageQueue messageQueue = null;
					if (queueConfigMap.containsKey("size")) {
						try {
							size = Integer.parseInt((String) queueConfigMap.get("size"));
						} catch (NumberFormatException e) {
							size = Integer.MAX_VALUE;
						}
						messageQueue = buildMessageQueue(name, size);
					} else {
						messageQueue = buildMessageQueue(name);
					}

					if (StringUtils.isNotEmpty(name) && messageQueue != null) {
						logger.info(String.format("Add message queue: %s, size: %d", name, size));
						CUSTOM_MESSAGE_QUEUE.put(name, messageQueue);
					}
				}
			}
		}
	}
	
	/**
	 * 新增消息队列
	 * 
	 * @param name 消息队列名称
	 * @param size 消息队列长度
	 */
	public static void addQueue(String name, int size) {
		logger.info(String.format("Add message queue: %s, size: %d", name, size));
		MessageQueue messageQueue = buildMessageQueue(name, size);
		CUSTOM_MESSAGE_QUEUE.put(name, messageQueue);
	}
	
	/**
	 * 删除消息队列
	 * 
	 * @param name 消息队列名称
	 */
	public static void removeQueue(String name) {
		logger.info(String.format("Remove message queue: %s", name));
		MessageQueue messageQueue = CUSTOM_MESSAGE_QUEUE.get(name);
		if (messageQueue != null) {
			synchronized (messageQueue) {
				messageQueue.clear();
				CUSTOM_MESSAGE_QUEUE.remove(name);
			}
		}
	}
	
	/**
	 * 根据名称获取自定义消息队列
	 * 
	 * @param name 消息列表名称
	 * 
	 * @return 自定义消息队列
	 */
	public static MessageQueue getQueue(String name) {
		MessageQueue messageQueue = CUSTOM_MESSAGE_QUEUE.get(name);
		return messageQueue;
	}

	/**
	 * 根据名称判断是否存在对应的消息队列
	 * 
	 * @param name 消息列表名称
	 * 
	 * @return 是否存在消息队列
	 */
	public static boolean hasQueue(String name) {
		return CUSTOM_MESSAGE_QUEUE.containsKey(name);
	}

}
