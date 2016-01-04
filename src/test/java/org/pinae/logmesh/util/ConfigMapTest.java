package org.pinae.logmesh.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class ConfigMapTest {
	
	@Test
	public void testGetString() {
		Map<String, Object> values = new HashMap<String, Object>();
		values.put("Null", null);
		values.put("String", "string");
		values.put("Integer", 1);
		values.put("Long", 1L);
		values.put("Boolean", true);
		
		ConfigMap<String, Object> configMap = new ConfigMap<String, Object>(values);
		assertEquals(configMap.getString("null", "NULL"), "NULL");
		assertEquals(configMap.getString("Null", "NULL"), "NULL");
		assertEquals(configMap.getString("String", "NULL"), "string");
		assertEquals(configMap.getString("Integer", "NULL"), "1");
		assertEquals(configMap.getString("Long", "NULL"), "1");
		assertEquals(configMap.getString("Boolean", "NULL"), "true");
	}
	
	@Test
	public void testGetInt() {
		Map<String, Object> values = new HashMap<String, Object>();
		values.put("Null", null);
		values.put("String", "string");
		values.put("Integer-1", 1);
		values.put("Integer-2", "1");
		values.put("Integer-3", "-1");
		
		ConfigMap<String, Object> configMap = new ConfigMap<String, Object>(values);
		assertEquals(configMap.getInt("null", 0), 0);
		assertEquals(configMap.getInt("Null", 0), 0);
		assertEquals(configMap.getInt("String", 0), 0);
		assertEquals(configMap.getInt("Integer-1", 0), 1);
		assertEquals(configMap.getInt("Integer-2", 0), 1);
		assertEquals(configMap.getInt("Integer-3", 0), -1);
	}
	
	@Test
	public void testGetLong() {
		Map<String, Object> values = new HashMap<String, Object>();
		values.put("Null", null);
		values.put("String", "string");
		values.put("Long-1", 1);
		values.put("Long-2", "1");
		values.put("Long-3", "-1");
		values.put("Long-4", Long.toString(Long.MAX_VALUE));
		
		ConfigMap<String, Object> configMap = new ConfigMap<String, Object>(values);
		assertEquals(configMap.getLong("null", 0), 0);
		assertEquals(configMap.getLong("Null", 0), 0);
		assertEquals(configMap.getLong("String", 0), 0);
		assertEquals(configMap.getLong("Long-1", 0), 1);
		assertEquals(configMap.getLong("Long-2", 0), 1);
		assertEquals(configMap.getLong("Long-3", 0), -1);
		assertEquals(configMap.getLong("Long-4", 0), Long.MAX_VALUE);
	}
	
	@Test
	public void testGetDouble() {
		Map<String, Object> values = new HashMap<String, Object>();
		values.put("Null", null);
		values.put("String", "string");
		values.put("Double-1", 1.01);
		values.put("Double-2", "1.01");
		values.put("Double-3", "-1.01");
		
		ConfigMap<String, Object> configMap = new ConfigMap<String, Object>(values);
		assertEquals(configMap.getDouble("null", 0), 0, 2);
		assertEquals(configMap.getDouble("Null", 0), 0, 2);
		assertEquals(configMap.getDouble("String", 0), 0, 2);
		assertEquals(configMap.getDouble("Long-1", 0), 1.01, 2);
		assertEquals(configMap.getDouble("Long-2", 0), 1.01, 2);
		assertEquals(configMap.getDouble("Long-3", 0), -1.01, 2);
	}
	
	@Test
	public void testGetBoolean() {
		Map<String, Object> values = new HashMap<String, Object>();
		values.put("Null", null);
		values.put("String", "string");
		values.put("Boolean-1", true);
		values.put("Boolean-2", "true");
		values.put("Boolean-3", "True");
		values.put("Boolean-4", "TRUE");
		
		ConfigMap<String, Object> configMap = new ConfigMap<String, Object>(values);
		assertEquals(configMap.getBoolean("null", false), false);
		assertEquals(configMap.getBoolean("Null", false), false);
		assertEquals(configMap.getBoolean("String", false), false);
		assertEquals(configMap.getBoolean("Boolean-1", false), true);
		assertEquals(configMap.getBoolean("Boolean-2", false), true);
		assertEquals(configMap.getBoolean("Boolean-3", false), true);
		assertEquals(configMap.getBoolean("Boolean-4", false), true);
	}
	
	@Test
	public void testIsNotBlank() {
		Map<String, Object> values = new HashMap<String, Object>();
		values.put("Null", null);
		values.put("Blank", "");
		values.put("Whitespace", " ");
		values.put("String", "string");
		values.put("Integer", 1);
		values.put("Boolean", true);
		
		ConfigMap<String, Object> configMap = new ConfigMap<String, Object>(values);
		assertFalse(configMap.isNotBlank("null"));
		assertFalse(configMap.isNotBlank("Null"));
		assertFalse(configMap.isNotBlank("Blank"));
		assertFalse(configMap.isNotBlank("Whitespace"));
		assertTrue(configMap.isNotBlank("String"));
		assertTrue(configMap.isNotBlank("Integer"));
		assertTrue(configMap.isNotBlank("Boolean"));
	}
	
	@Test
	public void testEquals() {
		Map<String, Object> values = new HashMap<String, Object>();
		values.put("Null", null);
		values.put("Blank", "");
		values.put("Whitespace", " ");
		values.put("String", "string");
		values.put("Integer", 1);
		values.put("Boolean", true);
		
		ConfigMap<String, Object> configMap = new ConfigMap<String, Object>(values);
		assertTrue(configMap.equals("null", null));
		assertTrue(configMap.equals("Null", null));
		assertTrue(configMap.equals("Whitespace", " "));
		assertTrue(configMap.equals("String", "string"));
		assertTrue(configMap.equals("Integer", 1));
		assertTrue(configMap.equals("Boolean", true));
	}
	
	@Test
	public void testEqualsIgnoreCase() {
		Map<String, Object> values = new HashMap<String, Object>();
		values.put("Null", null);
		values.put("Blank", "");
		values.put("Whitespace", " ");
		values.put("String", "string");
		values.put("Boolean", true);
		
		ConfigMap<String, Object> configMap = new ConfigMap<String, Object>(values);
		assertTrue(configMap.equalsIgnoreCase("null", null));
		assertTrue(configMap.equalsIgnoreCase("Null", null));
		assertTrue(configMap.equalsIgnoreCase("Whitespace", " "));
		assertTrue(configMap.equalsIgnoreCase("String", "STRING"));
		assertTrue(configMap.equalsIgnoreCase("Boolean", "TRUE"));
	}
}
