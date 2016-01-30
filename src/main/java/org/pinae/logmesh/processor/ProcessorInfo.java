package org.pinae.logmesh.processor;

import java.util.Map;

import org.pinae.logmesh.util.ConfigMap;

/**
 * 处理器信息
 * 
 * @author Huiyugeng
 * 
 * 
 */
public abstract class ProcessorInfo {
	/* 处理器名称 */
	private String name;

	/* 处理器参数 */
	private ConfigMap<String, Object> parameters = new ConfigMap<String, Object>();

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
		return parameters.getString(key, defaultValue);
	}
	
	public boolean getBooleanValue(String key, boolean defaultValue) {
		return parameters.getBoolean(key, defaultValue);
	}
	
	public int getIntegerValue(String key, int defaultValue) {
		return parameters.getInt(key, defaultValue);
	}
	
	public long getLongValue(String key, long defaultValue) {
		return parameters.getLong(key, defaultValue);
	}

	public void setParameters(Map<String, Object> parameters) {
		this.parameters = new ConfigMap<String, Object>(parameters);
	}

	public Map<String, Object> getParameters() {
		return parameters;
	}

}
