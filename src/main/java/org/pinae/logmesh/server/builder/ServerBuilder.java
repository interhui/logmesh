package org.pinae.logmesh.server.builder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.pinae.ndb.Statement;

/**
 * 日志服务配置信息构建器
 * 
 * @author Huiyugeng
 * 
 */
public class ServerBuilder {

	private Statement statement = new Statement();

	private Map<String, Object> serverConfig = new HashMap<String, Object>();

	/**
	 * 获得日志服务配置信息
	 * 
	 * @return 日志服务配置信息
	 */
	@SuppressWarnings("unchecked")
	public Map<String, Object> build() {
		if (serverConfig.containsKey("server")) {
			return (Map<String, Object>) serverConfig.get("server");
		}
		return serverConfig;
	}

	/**
	 * 设置日志服务所有者
	 * 
	 * @param owner 服务所有者
	 */
	public void setOwner(String owner) {
		Object result = statement.execute(serverConfig, "select:server->owner");
		if (result instanceof List && ((List<?>) result).size() > 0) {
			statement.execute(serverConfig, String.format("update:server->owner:%s !! owner=%s", owner, owner));
		} else {
			statement.execute(serverConfig, String.format("insert:server !! owner=%s", owner));
		}
	}

	/**
	 * 增加消息队列，当消息队列存在时更新消息队列的长度
	 * 
	 * @param name 消息队列名称
	 * @param size 消息队列长度
	 */
	public void addQueue(String name, long size) {
		Object result = statement.execute(serverConfig, String.format("select:server->queue->name:%s", name));
		if (result instanceof List && ((List<?>) result).size() > 0) {
			statement.execute(serverConfig, String.format("update:server->queue->name:%s !! size=%s", name, Long.toString(size)));
		} else {
			statement.execute(serverConfig, String.format("insert:server->queue !! name=%s, size=%s", name, Long.toString(size)));
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
		Object result = statement.execute(serverConfig, String.format("select:server->thread->%s", type));
		if (result instanceof List && ((List<?>) result).size() > 0) {
			statement.execute(serverConfig,
					String.format("update:server->thread->%s !! count=%s, delay=%s", type, Integer.toString(count), Long.toString(delay)));
		} else {
			statement.execute(serverConfig,
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
		Object result = statement.execute(serverConfig, "select:server->merger");
		if (result instanceof List && ((List<?>) result).size() > 0) {
			statement.execute(serverConfig,
					String.format("update:server->merger !! enable=%s, cycle=%s", Boolean.toString(active), Long.toString(cycle)));
		} else {
			statement.execute(serverConfig,
					String.format("insert:server->merger !! enable=%s, cycle=%s", Boolean.toString(active), Long.toString(cycle)));
		}
	}

	/**
	 * 增加消息接收器
	 * 
	 * @param type 接收器类型
	 * @param active 是否激活接收器
	 * @param original 是否采集原始日志
	 * @param parameters 接收器参数
	 */
	public void addReceiver(String type, boolean active, boolean original, String codec, Map<String, String> parameters) {
		Object result = statement.execute(serverConfig, String.format("select:server->receiver->type:%s", type));
		if (result instanceof List && ((List<?>) result).size() > 0) {
			statement.execute(serverConfig, String.format("update:server->receiver->type:%s !! enable=%s, original=%s, codec=%s", type,
					Boolean.toString(active), Boolean.toString(original), codec));
		} else {
			statement.execute(serverConfig, String.format("insert:server->receiver !! type=%s, enable=%s, original=%s, codec=%s", type,
					Boolean.toString(active), Boolean.toString(original), codec));
		}

		Object receiver = statement.execute(serverConfig, String.format("one:server->receiver->type:%s", type));
		if (receiver instanceof Map) {
			Map<?, ?> receiverMap = (Map<?, ?>) receiver;
			setParameter(receiverMap, parameters);
		}
	}

	/**
	 * 设置日志计数器
	 * 
	 * @param parameters 日志计数器参数
	 */
	public void setCounter(Map<String, String> parameters) {
		Object result = statement.execute(serverConfig, "select:server->counter");
		if (result instanceof List && ((List<?>) result).size() == 0) {
			statement.execute(serverConfig, String.format("insert:server->counter"));
		}
		Object counter = statement.execute(serverConfig, "one:server->counter");
		if (counter instanceof Map) {
			Map<?, ?> counterMap = (Map<?, ?>) counter;
			setParameter(counterMap, parameters);
		}
	}

	/**
	 * 设置原始日志参数
	 * 
	 * @param parameters 原始日志参数
	 */
	public void setOriginal(Map<String, String> parameters) {
		Object result = statement.execute(serverConfig, "select:server->original");
		if (result instanceof List && ((List<?>) result).size() == 0) {
			statement.execute(serverConfig, String.format("insert:server->original"));
		}
		Object original = statement.execute(serverConfig, "one:server->original");
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
	public void addFilter(int startup, String name, boolean active, String filterClass, Map<String, String> parameters) {
		Object result = statement.execute(serverConfig, String.format("select:server->filter->name:%s", name));
		if (result instanceof List && ((List<?>) result).size() > 0) {
			statement.execute(serverConfig, String.format("update:server->filter->name:%s !! startup=%s, enable=%s, kwClass=%s", name,
					Integer.toString(startup), Boolean.toString(active), filterClass));
		} else {
			statement.execute(serverConfig, String.format("insert:server->filter !! name=%s, startup=%s, enable=%s, kwClass=%s", name,
					Integer.toString(startup), Boolean.toString(active), filterClass));
		}

		Object filter = statement.execute(serverConfig, String.format("one:server->filter->name:%s", name));
		if (filter instanceof Map) {
			setParameter((Map<?, ?>) filter, parameters);
		}
	}

	/**
	 * 增加消息处理器
	 * 
	 * @param name 处理器名称
	 * @param active 是否启用处理器
	 * @param filterClass 处理器执行类
	 * @param parameters 处理器参数
	 */
	public void addProcessor(String name, boolean active, String processorClass, Map<String, String> parameters) {
		Object result = statement.execute(serverConfig, String.format("select:server->processor->name:%s", name));
		if (result instanceof List && ((List<?>) result).size() > 0) {
			statement.execute(serverConfig,
					String.format("update:server->processor->name:%s !! enable=%s, kwClass=%s", name, Boolean.toString(active), processorClass));
		} else {
			statement.execute(serverConfig,
					String.format("insert:server->processor !! name=%s, enable=%s, kwClass=%s", name, Boolean.toString(active), processorClass));
		}

		Object processor = statement.execute(serverConfig, String.format("one:server->processor->name:%s", name));
		if (processor instanceof Map) {
			setParameter((Map<?, ?>) processor, parameters);
		}
	}

	/**
	 * 增加消息路由器
	 * 
	 * @param name 路由器名称
	 * @param active 是否启用路由器
	 * @param routerClass 路由器执行类
	 * @param parameters 路由器参数
	 */
	public void addRouter(String name, boolean active, String routerClass, Map<String, String> parameters) {
		Object result = statement.execute(serverConfig, String.format("select:server->router->name:%s", name));
		if (result instanceof List && ((List<?>) result).size() > 0) {
			statement.execute(serverConfig,
					String.format("update:server->router->name:%s !! enable=%s, kwClass=%s", name, Boolean.toString(active), routerClass));
		} else {
			statement.execute(serverConfig,
					String.format("insert:server->router !! name=%s, enable=%s, kwClass=%s", name, Boolean.toString(active), routerClass));
		}

		Object router = statement.execute(serverConfig, String.format("one:server->router->name:%s", name));
		if (router instanceof Map) {
			setParameter((Map<?, ?>) router, parameters);
		}
	}

	/**
	 * 增加消息输出器
	 * 
	 * @param name 消息输出器名称
	 * @param active 是否启用该消息输出器
	 * @param outputClass 消息输出器执行类
	 * @param parameters 消息输出器参数
	 */
	public void addOutputor(String name, boolean active, String outputClass, Map<String, String> parameters) {
		Object result = statement.execute(serverConfig, String.format("select:server->output->name:%s", name));
		if (result instanceof List && ((List<?>) result).size() > 0) {
			statement.execute(serverConfig,
					String.format("update:server->output->name:%s !! enable=%s, kwClass=%s", name, Boolean.toString(active), outputClass));
		} else {
			statement.execute(serverConfig,
					String.format("insert:server->output !! name=%s, enable=%s, kwClass=%s", name, Boolean.toString(active), outputClass));
		}

		Object output = statement.execute(serverConfig, String.format("one:server->output->name:%s", name));
		if (output instanceof Map) {
			setParameter((Map<?, ?>) output, parameters);
		}
	}

	/*
	 * 参数转换
	 * 将K-V格式的参数列表转换为参数列表
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Map<?, ?> setParameter(Map target, Map<String, String> parameters) {
		if (parameters != null) {
			List<Map<?, ?>> paramList = new ArrayList<Map<?, ?>>();

			Set<String> paraKeySet = parameters.keySet();
			
			for (String paraKey : paraKeySet) {
				Map<String, Object> param = new HashMap<String, Object>();
				param.put("key", paraKey);
				param.put("value", parameters.get(paraKey));

				paramList.add(param);
			}

			Map params = new HashMap();
			params.put("parameter", paramList);

			target.put("parameters", params);
		}
		return target;
	}

}
