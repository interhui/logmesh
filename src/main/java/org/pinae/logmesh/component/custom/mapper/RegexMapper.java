package org.pinae.logmesh.component.custom.mapper;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.pinae.logmesh.util.FileUtils;
import org.pinae.nala.xb.Xml;
import org.pinae.nala.xb.exception.NoSuchPathException;
import org.pinae.nala.xb.exception.UnmarshalException;
import org.pinae.ndb.Ndb;

/**
 * 对文本消息进行正则表达式映射
 * 
 * @author Huiyugeng
 * 
 * 
 */
public class RegexMapper {
	private static Logger logger = Logger.getLogger(RegexMapper.class);

	private Map<String, Pattern> patternMap = new HashMap<String, Pattern>(); // 正则匹配
	private Map<String, Map<String, String[]>> itemMap = new HashMap<String, Map<String, String[]>>(); // 正则值映射
	private Map<String, String> formatMap = new HashMap<String, String>(); // 格式化映射

	public RegexMapper(File mapperFile) {
		load(mapperFile);
	}

	public RegexMapper(Map<String, Pattern> patternMap, Map<String, Map<String, String[]>> itemMap) {
		this.patternMap = patternMap;
		this.itemMap = itemMap;
	}

	@SuppressWarnings("unchecked")
	private void load(File mapperFile) {
		if (mapperFile == null) {
			throw new NullPointerException("Map File is null");
		}
		logger.info(String.format("Loading Regex Map File: %s", mapperFile.getAbsolutePath()));

		Map<String, Object> mapperConfig = null;

		try {
			mapperConfig = (Map<String, Object>) Xml.toMap(mapperFile, "UTF8");
		} catch (UnmarshalException e) {
			logger.error(String.format("Regex Mapper Load Exception: exception=%s", e.getMessage()));
		} catch (NoSuchPathException e) {
			logger.error(String.format("Regex Mapper Load Exception: exception=%s", e.getMessage()));
		}

		if (mapperConfig != null && mapperConfig.containsKey("import")) {
			List<String> importList = (List<String>) Ndb.execute(mapperConfig, "select:import->file");
			for (String file : importList) {
				if (StringUtils.isNotEmpty(file)) {
					load(FileUtils.getFile(file));
				}
			}
		}

		if (mapperConfig != null && mapperConfig.containsKey("map")) {

			List<Map<String, Object>> mapList = (List<Map<String, Object>>) Ndb.execute(mapperConfig, "select:map");
			for (Map<String, Object> map : mapList) {

				String name = (String) map.get("name");
				String type = (String) map.get("type");
				name = type + ":" + name;

				String pattern = (String) map.get("pattern");
				Map<String, String[]> item = buildItemMap(map);

				if (name != null && pattern != null) {

					patternMap.put(name, Pattern.compile(pattern));
					itemMap.put(name, item);

					if (map.containsKey("format")) {
						String format = (String) map.get("format");
						formatMap.put(name, format);
					}
				}
			}
		}
	}

	/**
	 * 构建映射值内容
	 * 
	 * @param regexMap 正则映射配置
	 * 
	 * @return 映射值内容
	 */
	@SuppressWarnings("unchecked")
	private Map<String, String[]> buildItemMap(Map<String, Object> regexMap) {
		Map<String, String[]> itemRefMap = null;

		List<Map<String, Object>> itemList = (List<Map<String, Object>>) Ndb.execute(regexMap,
				"select:items->item");
		if (itemList != null && itemList.size() > 0) {
			itemRefMap = new TreeMap<String, String[]>();

			for (Map<String, Object> item : itemList) {
				String index = (String) item.get("index");

				String key = (String) item.get("key");
				if (item.containsKey("split")) {
					String split = (String) item.get("split");
					String pattern = "(.*)";
					if (StringUtils.isEmpty(key) && item.containsKey("pattern")) {
						pattern = (String) item.get("pattern");
					}
					itemRefMap.put(index, new String[] { "split", key, split, pattern});
				} else if (item.containsKey("key")) {
					itemRefMap.put(index, new String[] { "match", key });
				}
			}
		}

		return itemRefMap;
	}

	/**
	 * 获取正则表达式映射表
	 * 
	 * @return 正则表达式映射表
	 */
	public Map<String, Pattern> getPatternMap() {
		return patternMap;
	}

	/**
	 * 获取解析映射表
	 * 
	 * @return 解析映射表
	 */
	public Map<String, Map<String, String[]>> getItemMap() {
		return itemMap;
	}

	/**
	 * 仅使用限定的规则将文本消息根据正则表达式进行映射，
	 * 
	 * @param message 消息内容
	 * 
	 * @return 映射后的Map
	 */
	public Map<String, String> map(String message) {
		return map(null, message);
	}

	/**
	 * 仅使用限定的规则将文本消息根据正则表达式进行映射
	 * 
	 * @param ruleList 规则名称列表
	 * @param message 消息内容
	 * 
	 * @return 映射后的Map
	 */
	public Map<String, String> map(List<String> ruleList, String message) {
		Map<String, String> itemRefMap = null;

		Set<String> mapNameSet = patternMap.keySet();
		for (String mapName : mapNameSet) {

			if (!matcheRuleName(ruleList, mapName)) {
				continue;
			}

			Pattern pattern = patternMap.get(mapName);

			Matcher matcher = pattern.matcher(message);
			while (matcher.find()) {
				Map<String, String[]> itemConfigMap = itemMap.get(mapName);

				if (itemConfigMap != null && itemConfigMap.size() > 0) {
					itemRefMap = new HashMap<String, String>();

					for (int i = 0; i <= matcher.groupCount(); i++) {
						String matchParameters[] = itemConfigMap.get(Integer.toString(i));
						String value = matcher.group(i);

						if (matchParameters != null && matchParameters.length > 1 && value != null) {
							String key = matchParameters[1];
							value = value.trim();
							
							// 根据key进行值设定
							if (matchParameters[0].equals("match") && matchParameters.length == 2) {
								itemRefMap.put(key, value);
							}
							// 切分后进行key设定
							else if (matchParameters[0].equals("split") && matchParameters.length == 4) {
								
								String split = matchParameters[2];
								Pattern subPattern = Pattern.compile(matchParameters[3]);
								
								String keyItems[] = null;
								if (key != null) {
									keyItems = key.split(";");
								}
								String valueItems[] = value.split(split);
								
								//如果键值的数量少于值的数量，则对键值数组进行填充
								if (keyItems != null && keyItems.length < valueItems.length) {
									String filledKeyItems[] = new String[valueItems.length];
									for (int j = 0; j < filledKeyItems.length; j++) {
										filledKeyItems[j] = "key_" + Integer.toString(j);
									}
									System.arraycopy(keyItems, 0, filledKeyItems, 0, keyItems.length);
									keyItems = filledKeyItems;
								}
								
								//对值进行填充
								for (int j = 0; j < valueItems.length; j++) {
									Matcher subMatcher = subPattern.matcher(valueItems[j]);
									while (subMatcher.find()) {
										String itemKey = null;
										String itemValue = null;
										
										if (keyItems != null) {
											itemKey = keyItems[j];
											itemValue = subMatcher.group(1);
										} else {
											itemKey = subMatcher.group(1);
											List<String> tempValue = new ArrayList<String>();
											for (int k = 2; k <= subMatcher.groupCount(); k++) {
												tempValue.add(subMatcher.group(k));
											}
											itemValue = StringUtils.join(tempValue, " ");
										}
										
										if (StringUtils.isNotEmpty(itemKey) && StringUtils.isNotEmpty(itemValue)) {
											itemRefMap.put(itemKey.trim(), itemValue.trim());
										}
									}
								}
								
							}

						}
					}
				}

				String mapNames[] = mapName.split(":");
				if (mapNames.length >= 2) {
					itemRefMap.put("_type", mapNames[0]);
					itemRefMap.put("_name", mapNames[1]);
				} else {
					itemRefMap.put("_type", mapName);
					itemRefMap.put("_name", mapName);
				}

				if (itemRefMap != null) {
					return itemRefMap;
				}
			}
		}
		return itemRefMap;
	}

	/**
	 * 根据映射值对消息进行格式化处理
	 * 
	 * @param message 消息内容
	 * 
	 * @return 格式化后的消息
	 */
	public String format(String message) {
		return format(map(message));
	}

	/**
	 * 根据映射值对消息进行格式化处理
	 * 
	 * @param map 值映射表
	 * 
	 * @return 格式化后的消息
	 */
	public String format(Map<String, String> map) {
		if (map != null && map.containsKey("_type") && map.containsKey("_name")) {

			String name = (String) map.get("_name");
			String type = (String) map.get("_type");
			name = type + ":" + name;

			String formattedMsg = formatMap.get(name);

			if (StringUtils.isNotEmpty(formattedMsg)) {
				Set<String> itemKeySet = map.keySet();
				for (String itemKey : itemKeySet) {
					String itemValue = map.get(itemKey);
					itemKey = "$" + itemKey;
					if (formattedMsg.contains(itemKey)) {
						try {
							formattedMsg = formattedMsg.replaceAll("\\" + itemKey, itemValue);
						} catch (Exception e) {

						}
					}
				}

				return formattedMsg;
			}
		}
		return null;
	}

	private boolean matcheRuleName(List<String> ruleList, String mapName) {
		if (ruleList != null) {
			for (String ruleName : ruleList) {
				if (mapName.matches(ruleName)) {
					return true;
				}
			}
			return false;
		} else {
			return true;
		}

	}
}
