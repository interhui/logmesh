package org.pinae.logmesh.component;

import java.util.Map;

import org.pinae.logmesh.util.ConfigMap;

/**
 * 组件信息
 * 
 * @author Huiyugeng
 * 
 */
public abstract class ComponentInfo {
	
	/* 组件名称 */
	private String name;
	/* 组件参数 */
	private ConfigMap<String, Object> parameters = new ConfigMap<String, Object>();

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public boolean hasParameter(String key) {
		return this.parameters.containsKey(key);
	}
	
	public Object getValue(String key) {
		return this.parameters.get(key);
	}

	public String getStringValue(String key, String defaultValue) {
		return this.parameters.getString(key, defaultValue);
	}
	
	public boolean getBooleanValue(String key, boolean defaultValue) {
		return this.parameters.getBoolean(key, defaultValue);
	}
	
	public int getIntegerValue(String key, int defaultValue) {
		return this.parameters.getInt(key, defaultValue);
	}
	
	public long getLongValue(String key, long defaultValue) {
		return this.parameters.getLong(key, defaultValue);
	}

	public void setParameters(Map<String, Object> parameters) {
		this.parameters = new ConfigMap<String, Object>(parameters);
	}

	public Map<String, Object> getParameters() {
		return this.parameters;
	}

}
