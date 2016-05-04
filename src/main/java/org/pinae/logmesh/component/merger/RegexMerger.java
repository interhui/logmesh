package org.pinae.logmesh.component.merger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.util.MessageDigestUtils;
import org.pinae.ndb.Ndb;

/**
 * 根据正则表达式标示的关键字进行合并
 * 
 * @author Huiyugeng
 * 
 * 
 */
public class RegexMerger extends AbstractMerger {

	private List<Pattern> patternList = new ArrayList<Pattern>(); // 正则匹配
	
	public void initialize() {
		
	}

	/**
	 * 基于正则表达式的归并规则载入
	 * 
	 * @param filename 规则文件
	 */
	public void load(String filename) {
		load("", filename);
	}

	/**
	 * 基于正则表达式的归并规则载入
	 * 
	 * @param path 规则路径
	 * @param filename 规则文件
	 */
	@SuppressWarnings("unchecked")
	public void load(String path, String filename) {
		
		Map<String, Object> regexConfig = loadConfig(path, filename);

		// 设置引入外部文件
		if (regexConfig != null && regexConfig.containsKey("import")) {
			List<String> importList = (List<String>) Ndb.execute(regexConfig, "select:import->file");
			for (String file : importList) {
				if (StringUtils.isNotEmpty(file)) {
					load(path, file);
				}
			}
		}

		if (regexConfig != null && regexConfig.containsKey("merger")) {

			List<Map<String, Object>> mergerRuleList = (List<Map<String, Object>>) Ndb.execute(regexConfig,
					"select:merger");
			for (Map<String, Object> mergerRule : mergerRuleList) {

				String pattern = (String) mergerRule.get("pattern");

				if (pattern != null) {
					patternList.add(Pattern.compile(pattern));
				}
			}
		}
	}

	/**
	 * 基于正则表达式的归并规则载入
	 * 
	 * @param patternMap 正则表达式的归并规则
	 */
	public void load(List<Pattern> patternList) {
		this.patternList = patternList;
	}

	/**
	 * 向归并队列中加入消息内容
	 * 
	 * @param message 消息内容
	 */
	public void add(Message message) {

		if (message != null) {
			Object msgContent = message.getMessage();
			
			if (msgContent instanceof String) {
				
				for (Pattern pattern : patternList) {
	
					Matcher matcher = pattern.matcher((String)msgContent);
					while (matcher.find()) {
						String key = "";
						for (int i = 1; i <= matcher.groupCount(); i++) {
							key += matcher.group(i);
						}
						key = MessageDigestUtils.MD5(key);
						
						addToMergerPool(key, message);
					}
				}
			}
		}

	}

}
