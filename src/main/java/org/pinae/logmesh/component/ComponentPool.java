package org.pinae.logmesh.component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.pinae.logmesh.component.filter.MessageFilter;
import org.pinae.logmesh.component.router.MessageRouter;

/**
 * 实例化组件池
 * 
 * 包括：消息过滤器，消息归并器，消息处理器
 * 
 * @author Huiyugeng
 * 
 * 
 */
public class ComponentPool {
	private static Logger logger = Logger.getLogger(ComponentPool.class);

	private static Map<String, Object> COMPONENT_POOL = new ConcurrentHashMap<String, Object>();
	private static AtomicInteger COMPONENT_COUNT = new AtomicInteger();

	/**
	 * 注册组件
	 * 一种类型的组件仅能注册一次
	 * 
	 * @param component 组件信息
	 */
	public static void registeComponent(Object component) {

		if (component instanceof MessageFilter || component instanceof MessageProcessor
				|| component instanceof MessageRouter) {

			String name = component.getClass().getSimpleName();
			String count = Integer.toString(COMPONENT_COUNT.incrementAndGet());

			synchronized (COMPONENT_POOL) {
				COMPONENT_POOL.put(name + "-" + count, component);
			}
		}
	}

	/**
	 * 根据类信息获取组件列表
	 * 
	 * @param clazz 需要获取的组件类
	 * 
	 * @return 组件类列表
	 */
	public static List<Object> getComponent(Class<?> clazz) {
		List<Object> componentList = new ArrayList<Object>();
		String name = clazz.getSimpleName();

		synchronized (COMPONENT_POOL) {
			Set<String> componentNameSet = COMPONENT_POOL.keySet();

			for (String componentName : componentNameSet) {
				if (componentName.startsWith(name)) {
					Object component = COMPONENT_POOL.get(componentName);
					componentList.add(component);
				}
			}
		}
		return componentList;
	}

	/**
	 * 重新加载指定类型的组件
	 * 
	 * @param clazz 重新加载的组件类
	 */
	public synchronized static void reloadComponent(Class<?> clazz) {
		logger.info(String.format("Reload %s Component", clazz.getSimpleName()));

		List<Object> componentList = getComponent(clazz);
		for (Object component : componentList) {
			if (component instanceof MessageFilter) {
				((MessageFilter) component).init();
			} else if (component instanceof MessageProcessor) {
				((MessageProcessor) component).init();
			} else if (component instanceof MessageRouter) {
				((MessageRouter) component).init();
			}
		}
	}

}
