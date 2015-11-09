package org.pinae.logmesh.processor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.pinae.ndb.Statement;

public class ProcessorFactory {

	private static Logger log = Logger.getLogger(ProcessBuilder.class);

	/**
	 * 构建处理器
	 * 
	 * @param processorMap 处理器描述信息
	 * 
	 * @return 构建后的处理器
	 */
	public static Object create(Map<String, Object> processorMap) {

		String name = (String) processorMap.get("name");
		String className = (String) processorMap.get("kwClass");
		Map<String, String> parameters = createParameter(processorMap);

		Object processor = null;
		try {
			Class<?> clazz = Class.forName(className);
			processor = clazz.newInstance();
			if (processor instanceof ProcessorInfo) {
				ProcessorInfo info = (ProcessorInfo) processor;
				if (StringUtils.isBlank(name)) {
					name = processor.toString();
				}
				info.setName(name);
				info.setParameters(parameters);
			}
		} catch (Exception e) {
			log.error(String.format("Create Processor Exception: exception=%s", e.getMessage()));
		}
		return processor;
	}

	/**
	 * 构建处理器参数信息
	 * 
	 * @param parameter 参数描述
	 * 
	 * @return 参数列表
	 */
	@SuppressWarnings("unchecked")
	public static Map<String, String> createParameter(Object parameter) {
		Map<String, String> parameterMap = new HashMap<String, String>();

		if (parameter == null || parameter instanceof String) {
			return null;
		}

		if (parameter instanceof Map) {
			Statement statement = new Statement();

			List<?> parameters = (List<?>) statement.execute((Map<String, Object>) parameter,
					"select:parameters->parameter");

			for (Object _parameter : parameters) {
				if (_parameter instanceof Map) {
					Map<String, String> _parameterMap = (Map<String, String>) _parameter;
					if (_parameterMap.containsKey("key") && _parameterMap.containsKey("value")) {
						parameterMap.put(_parameterMap.get("key"), _parameterMap.get("value"));
					}
				}

			}
		}

		return parameterMap;
	}

	public static Thread getThread(String name, Processor processor) {
		ProcessorPool.addProcessor(name, processor);
		return new Thread(processor, name);
	}

}
