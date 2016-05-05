package org.pinae.logmesh.server.builder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.pinae.ndb.Ndb;

/**
 * 消息服务配置信息构建器
 * 
 * @author Huiyugeng
 * 
 */
public class MessageServerBuilder {

	private Map<String, Object> serverConfig = new HashMap<String, Object>();
	
	public static final String RECEIVER_UDP = "udp";
	public static final String RECEIVER_TCP = "tcp";
	public static final String RECEIVER_JMS = "jms";
	public static final String RECEIVER_KAFKA = "kafka";
	
	/**
	 * 获得消息服务配置信息
	 * 
	 * @return 消息服务配置信息
	 */
	@SuppressWarnings("unchecked")
	public Map<String, Object> build() {
		if (serverConfig.containsKey("server")) {
			return (Map<String, Object>) serverConfig.get("server");
		}
		return serverConfig;
	}

	/**
	 * 设置消息服务所有者
	 * 
	 * @param owner 服务所有者
	 */
	public void setOwner(String owner) {
		Object result = Ndb.execute(serverConfig, "select:server->owner");
		if (result instanceof List && ((List<?>) result).size() > 0) {
			Ndb.execute(serverConfig, String.format("update:server->owner:%s !! owner=%s", owner, owner));
		} else {
			Ndb.execute(serverConfig, String.format("insert:server !! owner=%s", owner));
		}
	}

	/**
	 * 增加消息队列，当消息队列存在时更新消息队列的长度
	 * 
	 * @param name 消息队列名称
	 * @param size 消息队列长度
	 */
	public void addQueue(String name, long size) {
		Object result = Ndb.execute(serverConfig, String.format("select:server->queue->name:%s", name));
		if (result instanceof List && ((List<?>) result).size() > 0) {
			Ndb.execute(serverConfig, String.format("update:server->queue->name:%s !! size=%s", name, Long.toString(size)));
		} else {
			Ndb.execute(serverConfig, String.format("insert:server->queue !! name=%s, size=%s", name, Long.toString(size)));
		}
	}

	/**
	 * 设置线程管理
	 * 
	 * @param type 线程管理类型：filter 过滤器，router 路由， processor 自定义处理器
	 * @param count 线程数量
	 * @param delay 线程延迟时间(ms)，通常为100
	 */
	public void setThread(String type, int count, long delay) {
		Object result = Ndb.execute(serverConfig, String.format("select:server->thread->%s", type));
		if (result instanceof List && ((List<?>) result).size() > 0) {
			Ndb.execute(serverConfig,
					String.format("update:server->thread->%s !! count=%s, delay=%s", type, Integer.toString(count), Long.toString(delay)));
		} else {
			Ndb.execute(serverConfig,
					String.format("insert:server->thread->%s !! count=%s, delay=%s", type, Integer.toString(count), Long.toString(delay)));
		}
	}

	/**
	 * 是否启用消息归并
	 * 
	 * @param active 启动标记 true 启用 false 禁用
	 * @param cycle 消息归并周期(ms)
	 */
	public void activeMerger(boolean active, long cycle) {
		Object result = Ndb.execute(serverConfig, "select:server->merger");
		if (result instanceof List && ((List<?>) result).size() > 0) {
			Ndb.execute(serverConfig,
					String.format("update:server->merger !! enable=%s, cycle=%s", Boolean.toString(active), Long.toString(cycle)));
		} else {
			Ndb.execute(serverConfig,
					String.format("insert:server->merger !! enable=%s, cycle=%s", Boolean.toString(active), Long.toString(cycle)));
		}
	}

	/**
	 * 增加消息接收器
	 * 
	 * @param type 接收器类型
	 * @param active 是否激活接收器
	 * @param original 是否采集原始消息
	 * @param parameters 接收器参数
	 */
	public void addReceiver(String type, boolean active, boolean original, String codec, Map<String, Object> parameters) {
		Object result = Ndb.execute(serverConfig, String.format("select:server->receiver->type:%s", type));
		if (result instanceof List && ((List<?>) result).size() > 0) {
			Ndb.execute(serverConfig, String.format("update:server->receiver->type:%s !! enable=%s, original=%s, codec=%s", type,
					Boolean.toString(active), Boolean.toString(original), codec));
		} else {
			Ndb.execute(serverConfig, String.format("insert:server->receiver !! type=%s, enable=%s, original=%s, codec=%s", type,
					Boolean.toString(active), Boolean.toString(original), codec));
		}

		Object receiver = Ndb.execute(serverConfig, String.format("one:server->receiver->type:%s", type));
		if (receiver instanceof Map) {
			Map<?, ?> receiverMap = (Map<?, ?>) receiver;
			setParameter(receiverMap, parameters);
		}
	}

	/**
	 * 设置消息计数器
	 * 
	 * @param parameters 消息计数器参数
	 */
	public void setCounter(Map<String, Object> parameters) {
		Object result = Ndb.execute(serverConfig, "select:server->counter");
		if (result instanceof List && ((List<?>) result).size() == 0) {
			Ndb.execute(serverConfig, String.format("insert:server->counter"));
		}
		Object counter = Ndb.execute(serverConfig, "one:server->counter");
		if (counter instanceof Map) {
			Map<?, ?> counterMap = (Map<?, ?>) counter;
			setParameter(counterMap, parameters);
		}
	}

	/**
	 * 设置原始消息参数
	 * 
	 * @param parameters 原始消息参数
	 */
	public void setOriginal(Map<String, Object> parameters) {
		Object result = Ndb.execute(serverConfig, "select:server->original");
		if (result instanceof List && ((List<?>) result).size() == 0) {
			Ndb.execute(serverConfig, String.format("insert:server->original"));
		}
		Object original = Ndb.execute(serverConfig, "one:server->original");
		if (original instanceof Map) {
			Map<?, ?> originalMap = (Map<?, ?>) original;
			setParameter(originalMap, parameters);
		}

	}

	/**
	 * 增加消息过滤器
	 * 
	 * @param startup 启动顺序
	 * @param name 过滤器名称
	 * @param active 是否启用过滤器
	 * @param filterClass 过滤器执行类
	 * @param parameters 过滤器参数
	 */
	public void addFilter(int startup, String name, boolean active, String filterClass, Map<String, Object> parameters) {
		Object result = Ndb.execute(serverConfig, String.format("select:server->filter->name:%s", name));
		if (result instanceof List && ((List<?>) result).size() > 0) {
			Ndb.execute(serverConfig, String.format("update:server->filter->name:%s !! startup=%s, enable=%s, kwClass=%s", name,
					Integer.toString(startup), Boolean.toString(active), filterClass));
		} else {
			Ndb.execute(serverConfig, String.format("insert:server->filter !! name=%s, startup=%s, enable=%s, kwClass=%s", name,
					Integer.toString(startup), Boolean.toString(active), filterClass));
		}

		Object filter = Ndb.execute(serverConfig, String.format("one:server->filter->name:%s", name));
		if (filter instanceof Map) {
			setParameter((Map<?, ?>) filter, parameters);
		}
	}
	
	public void addFilter(int startup, String name, boolean active, Class<?> filterClass, Map<String, Object> parameters) {
		addFilter(startup, name, active, filterClass.getName(), parameters);
	}

	/**
	 * 增加消息处理器
	 * 
	 * @param name 处理器名称
	 * @param active 是否启用处理器
	 * @param filterClass 处理器执行类
	 * @param parameters 处理器参数
	 */
	public void addProcessor(String name, boolean active, String processorClass, Map<String, Object> parameters) {
		Object result = Ndb.execute(serverConfig, String.format("select:server->processor->name:%s", name));
		if (result instanceof List && ((List<?>) result).size() > 0) {
			Ndb.execute(serverConfig,
					String.format("update:server->processor->name:%s !! enable=%s, kwClass=%s", name, Boolean.toString(active), processorClass));
		} else {
			Ndb.execute(serverConfig,
					String.format("insert:server->processor !! name=%s, enable=%s, kwClass=%s", name, Boolean.toString(active), processorClass));
		}

		Object processor = Ndb.execute(serverConfig, String.format("one:server->processor->name:%s", name));
		if (processor instanceof Map) {
			setParameter((Map<?, ?>) processor, parameters);
		}
	}
	
	public void addProcessor(String name, boolean active, Class<?> processorClass, Map<String, Object> parameters) {
		addProcessor(name, active, processorClass.getName(), parameters);
	}

	/**
	 * 增加消息路由器
	 * 
	 * @param name 路由器名称
	 * @param active 是否启用路由器
	 * @param routerClass 路由器执行类
	 * @param parameters 路由器参数
	 */
	public void addRouter(String name, boolean active, String routerClass, Map<String, Object> parameters) {
		Object result = Ndb.execute(serverConfig, String.format("select:server->router->name:%s", name));
		if (result instanceof List && ((List<?>) result).size() > 0) {
			Ndb.execute(serverConfig,
					String.format("update:server->router->name:%s !! enable=%s, kwClass=%s", name, Boolean.toString(active), routerClass));
		} else {
			Ndb.execute(serverConfig,
					String.format("insert:server->router !! name=%s, enable=%s, kwClass=%s", name, Boolean.toString(active), routerClass));
		}

		Object router = Ndb.execute(serverConfig, String.format("one:server->router->name:%s", name));
		if (router instanceof Map) {
			setParameter((Map<?, ?>) router, parameters);
		}
	}
	
	public void addRouter(String name, boolean active, Class<?> routerClass, Map<String, Object> parameters) {
		addRouter(name, active, routerClass.getName(), parameters);
	}

	/**
	 * 增加消息输出器
	 * 
	 * @param name 消息输出器名称
	 * @param active 是否启用该消息输出器
	 * @param outputClass 消息输出器执行类
	 * @param parameters 消息输出器参数
	 */
	public void addOutputor(String name, boolean active, String outputClass, Map<String, Object> parameters) {
		Object result = Ndb.execute(serverConfig, String.format("select:server->outputor->name:%s", name));
		if (result instanceof List && ((List<?>) result).size() > 0) {
			Ndb.execute(serverConfig,
					String.format("update:server->outputor->name:%s !! enable=%s, kwClass=%s", name, Boolean.toString(active), outputClass));
		} else {
			Ndb.execute(serverConfig,
					String.format("insert:server->outputor !! name=%s, enable=%s, kwClass=%s", name, Boolean.toString(active), outputClass));
		}

		Object output = Ndb.execute(serverConfig, String.format("one:server->outputor->name:%s", name));
		if (output instanceof Map) {
			setParameter((Map<?, ?>) output, parameters);
		}
	}
	
	public void addOutputor(String name, boolean active, Class<?> outputClass, Map<String, Object> parameters) {
		addOutputor(name, active, outputClass.getName(), parameters);
	}

	/*
	 * 参数转换
	 * 将K-V格式的参数列表转换为参数列表
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Map<?, ?> setParameter(Map target, Map<String, Object> parameters) {
		if (parameters != null) {
			List<Map<?, ?>> paramList = new ArrayList<Map<?, ?>>();

			Set<String> keySet = parameters.keySet();
			
			for (String key : keySet) {
				Object value = parameters.get(key);
				
				Map<String, Object> param = new HashMap<String, Object>();
				param.put("key", key);
				param.put("value", value);
				paramList.add(param);
			}

			Map params = new HashMap();
			params.put("parameter", paramList);
			target.put("parameters", params);
		}
		return target;
	}

}
