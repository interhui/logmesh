package org.pinae.logmesh.processor;

import java.util.HashMap;
import java.util.Map;

/**
 * 处理器信息
 * 
 * @author Huiyugeng
 * 
 * 
 */
public abstract class ProcessorInfo {
	private String name; // 处理器名称

	private Map<String, String> parameters = new HashMap<String, String>(); // 处理器参数

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public boolean hasParameter(String key) {
		return parameters.containsKey(key);
	}

	public String getParameter(String key) {
		return parameters.get(key);
	}

	public void setParameters(Map<String, String> parameters) {
		this.parameters = parameters;
	}

	public Map<String, String> getParameters() {
		return parameters;
	}

}
