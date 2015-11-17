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

	private Map<String, Object> parameters = new HashMap<String, Object>(); // 处理器参数

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public boolean hasParameter(String key) {
		return parameters.containsKey(key);
	}
	
	public Object getValue(String key) {
		return parameters.get(key);
	}

	public String getStringValue(String key, String defaultValue) {
		String value = defaultValue;
		if (parameters.containsKey(key)) {
			value = (String)parameters.get(key);
		}
		return value;
	}
	
	public boolean getBooleanValue(String key, boolean defaultValue) {
		boolean value = defaultValue;
		try {
			value = Boolean.parseBoolean(getStringValue(key, "false"));
		} catch (Exception e) {
			value = defaultValue;
		}
		return value;
	}
	
	public int getIntegerValue(String key, int defaultValue) {
		int value = defaultValue;
		try {
			value = Integer.parseInt(getStringValue(key, "0"));
		} catch (Exception e) {
			value = defaultValue;
		}
		return value;
	}
	
	public long getLongValue(String key, long defaultValue) {
		long value = defaultValue;
		try {
			value = Long.parseLong(getStringValue(key, "0"));
		} catch (Exception e) {
			value = defaultValue;
		}
		return value;
	}

	public void setParameters(Map<String, Object> parameters) {
		this.parameters = parameters;
	}

	public Map<String, Object> getParameters() {
		return parameters;
	}

}
