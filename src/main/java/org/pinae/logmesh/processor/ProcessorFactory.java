package org.pinae.logmesh.processor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.pinae.ndb.Statement;

public class ProcessorFactory {

	private static Logger logger = Logger.getLogger(ProcessBuilder.class);

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
		Map<String, Object> parameters = createParameter(processorMap);

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
			logger.error(String.format("Create Processor Exception: exception=%s", e.getMessage()));
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
	public static Map<String, Object> createParameter(Object parameter) {
		Map<String, Object> parameterMap = new HashMap<String, Object>();

		if (parameter instanceof Map) {
			Statement statement = new Statement();

			List<?> paramItems = (List<?>) statement.execute((Map<String, Object>) parameter, "select:parameters->parameter");

			for (Object paramItem : paramItems) {
				if (paramItem instanceof Map) {
					Map<String, String> paramMap = (Map<String, String>) paramItem;
					if (paramMap.containsKey("key") && paramMap.containsKey("value")) {
						parameterMap.put(paramMap.get("key"), paramMap.get("value"));
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
