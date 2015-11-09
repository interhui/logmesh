package org.pinae.logmesh.message;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.pinae.ndb.Statement;

/**
 * 消息队列池
 * 
 * @author Huiyugeng
 * 
 */
public class MessagePool {

	// 原始消息处理队列
	public static final MessageQueue ORIGINAL_QUEUE = new MessageQueue("ORIGINAL_QUEUE");

	// 消息过滤队列
	public static final MessageQueue FILTER_QUEUE = new MessageQueue("FILTER_QUEUE");

	// 消息处理队列
	public static final MessageQueue PROCESSOR_QUEUE = new MessageQueue("PROCESSOR_QUEUE");

	// 消息归并队列
	public static final MessageQueue MERGER_QUEUE = new MessageQueue("MERGER_QUEUE");

	// 消息路由队列
	public static final MessageQueue ROUTE_QUEUE = new MessageQueue("ROUTE_QUEUE");

	// 计数器队列
	public static final MessageQueue COUNTER_QUEUE = new MessageQueue("COUNTER_QUEUE");

	// 自定义消息队列 <消息队列名称, 消息队列>
	public static Map<String, MessageQueue> CUSTOM_MESSAGE_QUEUE = new ConcurrentHashMap<String, MessageQueue>();

	public static void init(Map<String, Object> config) {

		// 初始化固定消息队列
		CUSTOM_MESSAGE_QUEUE.put("FILTER_QUEUE", FILTER_QUEUE);
		CUSTOM_MESSAGE_QUEUE.put("PROCESSOR_QUEUE", PROCESSOR_QUEUE);
		CUSTOM_MESSAGE_QUEUE.put("ROUTE_QUEUE", ROUTE_QUEUE);
		CUSTOM_MESSAGE_QUEUE.put("ORIGINAL_QUEUE", ORIGINAL_QUEUE);
		CUSTOM_MESSAGE_QUEUE.put("COUNTER_QUEUE", COUNTER_QUEUE);

		// 初始化自定义消息队列
		Statement statement = new Statement();

		List<?> queueConfigList = (List<?>) statement.execute(config, "select:queue");

		for (Object queueConfigObject : queueConfigList) {

			if (queueConfigObject instanceof Map) {
				@SuppressWarnings("unchecked")
				Map<String, Object> queueConfigMap = (Map<String, Object>) queueConfigObject;

				if (queueConfigMap.containsKey("name")) {
					String name = (String) queueConfigMap.get("name");

					MessageQueue messageQueue = null;
					if (queueConfigMap.containsKey("size")) {
						int size = Integer.MAX_VALUE;
						try {
							size = Integer.parseInt((String) queueConfigMap.get("size"));
						} catch (NumberFormatException e) {
							size = Integer.MAX_VALUE;
						}
						messageQueue = new MessageQueue(name, size);
					} else {
						messageQueue = new MessageQueue(name);
					}

					if (StringUtils.isNotEmpty(name) && messageQueue != null) {
						CUSTOM_MESSAGE_QUEUE.put(name, messageQueue);
					}
				}
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
	public static MessageQueue getMessageQueue(String name) {
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
	public static boolean hasMessageQueue(String name) {
		return CUSTOM_MESSAGE_QUEUE.containsKey(name);
	}

}
