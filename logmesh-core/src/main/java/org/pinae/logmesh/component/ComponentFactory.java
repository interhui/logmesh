package org.pinae.logmesh.component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.pinae.logmesh.util.ConfigMap;
import org.pinae.ndb.Ndb;

/**
 * 组件工厂
 * 
 * @author Huiyugeng
 *
 */
public class ComponentFactory {

	private static Logger logger = Logger.getLogger(ComponentFactory.class);

	/**
	 * 构建组件
	 * 
	 * @param componentMap 组件描述信息
	 * 
	 * @return 构建后的组件
	 */
	public static MessageComponent create(Map<String, Object> componentMap) {
		
		ConfigMap<String, Object> configMap = new ConfigMap<String, Object>(componentMap);
		
		String name = configMap.getString("name", null);
		String className = configMap.getString("kwClass", null);
		
		if (StringUtils.isEmpty(name) || StringUtils.isEmpty(className)) {
			return null;
		}
		
		// 对于单例的组件, 例如ScreenOutputor仅创建一次
		boolean singleton = configMap.getBoolean("singleton", false);
		if (singleton) {
			List<MessageComponent> componentList = ComponentPool.get(className);
			if (componentList.size() > 0) {
				return componentList.get(0);
			}
		}
		
		
		Map<String, Object> parameters = createParameter(componentMap);

		Object component = null;
		try {
			Class<?> clazz = Class.forName(className);
			component = clazz.newInstance();
			if (component instanceof ComponentInfo) {
				ComponentInfo info = (ComponentInfo) component;
				if (StringUtils.isBlank(name)) {
					name = component.toString();
				}
				info.setName(name);
				info.setParameters(parameters);
			}
		} catch (Exception e) {
			logger.error(String.format("Create Processor Exception: exception=%s", e.getMessage()));
		}
		if (component instanceof MessageComponent) {
			return (MessageComponent)component;
		} else {
			return null;
		}
	}

	/**
	 * 构建组件参数信息
	 * 
	 * @param parameter 参数描述
	 * 
	 * @return 参数列表
	 */
	@SuppressWarnings("unchecked")
	public static Map<String, Object> createParameter(Object parameter) {
		Map<String, Object> parameterMap = new HashMap<String, Object>();

		if (parameter instanceof Map) {
			List<?> paramItems = (List<?>) Ndb.execute((Map<String, Object>) parameter, "select:parameters->parameter");
			for (Object paramItem : paramItems) {
				if (paramItem instanceof Map) {
					Map<String, String> paramMap = (Map<String, String>) paramItem;
					if (paramMap.containsKey("key") && paramMap.containsKey("value")) {
						String key = paramMap.get("key");
						String value = paramMap.get("value");
						
						if (parameterMap.containsKey(key)) {
							Object paramValueObject = parameterMap.get(key);
							
							if (paramValueObject != null) {
								if (paramValueObject instanceof List) {
									((List<String>)paramValueObject).add(value);
								} else if (paramValueObject instanceof String){
									List<String> paramValueList = new ArrayList<String>();
									paramValueList.add(paramValueObject.toString());
									paramValueList.add(value);
									
									parameterMap.put(key, paramValueList);
								}
							}
						} else {
							parameterMap.put(key, value);
						}
					}
				}

			}
		}

		return parameterMap;
	}


}
