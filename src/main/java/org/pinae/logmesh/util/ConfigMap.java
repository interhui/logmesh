package org.pinae.logmesh.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

/**
 * 配置信息Map
 * 
 * @author Huiyugeng
 *
 * @param <K> Map键类型
 * @param <V> Map值类型
 */
public class ConfigMap<K, V> extends HashMap<K, V> {

	private static final long serialVersionUID = 5154521904524322767L;

	public ConfigMap() {

	}

	public ConfigMap(Map<? extends K, ? extends V> map) {
		super(map);
	}

	public String getString(K key, String defaultValue) {
		V v = get(key);
		if (v == null) {
			return defaultValue;
		}
		return v.toString();
	}

	public long getLong(K key, long defaultValue) {
		V v = get(key);
		if (v != null) {
			try {
				return Long.parseLong(v.toString());
			} catch (NumberFormatException e) {

			}
		}
		return defaultValue;
	}

	public int getInt(K key, int defaultValue) {
		V v = get(key);
		if (v != null) {
			try {
				return Integer.parseInt(v.toString());
			} catch (NumberFormatException e) {

			}
		}
		return defaultValue;
	}

	public double getDouble(K key, double defaultValue) {
		V v = get(key);
		if (v != null) {
			try {
				return Double.parseDouble(v.toString());
			} catch (NumberFormatException e) {

			}
		}
		return defaultValue;
	}

	public boolean getBoolean(K key, boolean defaultValue) {
		V v = get(key);
		if (v != null) {
			return Boolean.parseBoolean(v.toString().toLowerCase());
		}
		return defaultValue;
	}

	public boolean isNotBlank(K key) {
		V v = get(key);
		if (v == null) {
			return false;
		}
		return StringUtils.isNotBlank(v.toString());
	}

	public boolean equals(K key, V object) {
		V v = get(key);
		if (v == null && object == null) {
			return true;
		}
		if (v != null && object != null) {
			return v.equals(object);
		}
		return false;
	}

	public boolean equalsIgnoreCase(K key, V object) {
		V v = get(key);
		if (v == null && object == null) {
			return true;
		}
		if (v != null && object != null) {
			return v.toString().equalsIgnoreCase(object.toString());
		}
		return false;
	}

}
