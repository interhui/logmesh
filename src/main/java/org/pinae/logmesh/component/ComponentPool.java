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
 * 组件池
 * 
 * @author Huiyugeng
 * 
 */
public class ComponentPool {
	private static Logger logger = Logger.getLogger(ComponentPool.class);

	private static Map<String, MessageComponent> COMPONENT_POOL = new ConcurrentHashMap<String, MessageComponent>();
	private static AtomicInteger COMPONENT_COUNT = new AtomicInteger();
	
	/**
	 * 注册组件
	 * 
	 * @param component 组件信息
	 */
	public static void registe(MessageComponent component) {
		String name = component.getClass().getName();
		String count = Integer.toString(COMPONENT_COUNT.incrementAndGet());

		COMPONENT_POOL.put(name + "-" + count, component);
	}

	/**
	 * 根据类信息获取组件列表
	 * 
	 * @param clazz 需要获取的组件类
	 * 
	 * @return 组件类列表
	 */
	public static List<MessageComponent> get(Class<?> clazz) {
		String className = clazz.getName();
		return get(className);
	}
	
	/**
	 * 根据类信息获取组件列表
	 * 
	 * @param className 需要获取的组件类的名称
	 * 
	 * @return 组件类列表
	 */
	public static List<MessageComponent> get(String className) {
		List<MessageComponent> componentList = new ArrayList<MessageComponent>();

		synchronized (COMPONENT_POOL) {
			Set<String> componentNameSet = COMPONENT_POOL.keySet();

			for (String componentName : componentNameSet) {
				if (componentName.startsWith(className)) {
					MessageComponent component = COMPONENT_POOL.get(componentName);
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
	public synchronized static void reload(Class<?> clazz) {
		logger.info(String.format("Reload %s Component", clazz.getSimpleName()));

		List<MessageComponent> componentList = get(clazz);
		for (MessageComponent component : componentList) {
			if (component instanceof MessageFilter) {
				((MessageFilter) component).initialize();
			} else if (component instanceof MessageProcessor) {
				((MessageProcessor) component).initialize();
			} else if (component instanceof MessageRouter) {
				((MessageRouter) component).initialize();
			}
		}
	}

}
