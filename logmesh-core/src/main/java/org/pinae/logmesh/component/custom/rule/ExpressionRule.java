package org.pinae.logmesh.component.custom.rule;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mvel2.MVEL;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.util.MatchUtils;
import org.pinae.ndb.Ndb;

/**
 * 
 * 根据映射值与规则进行规则匹配
 * 
 * @author Huiyugeng
 * 
 */
public class ExpressionRule extends AbstractRule {

	public ExpressionRule(File ruleFile) {
		super.load(ruleFile);
	}

	public ExpressionRule(List<Map<String, Object>> ruleList) {
		super.ruleList = ruleList;
	}

	/**
	 * 将消息与规则表达式规则进行匹配
	 * 
	 * @param message 消息内容
	 * 
	 * @return 匹配的规则列表
	 */
	public MatchedRule match(Message message) {
		return super.match(this.ruleList, message);
	}

	/*
	 * 映射值与规则进行匹配
	 */
	@SuppressWarnings("unchecked")
	@Override
	public boolean matchMessageContent(Map<String, Object> rule, Object message) {
		if (rule == null || message == null) {
			return false;
		}

		if (rule.containsKey("pattern")) {
			Object pattern = rule.get("pattern");
			if (pattern instanceof Map) {
				rule = (Map<String, Object>) pattern;
			} else {
				return false;
			}
		} else {
			return false;
		}

		Map<String, String> msgContent = (Map<String, String>) message;

		if (rule.containsKey("expression") && rule.containsKey("item")) {
			String expression = (String) rule.get("expression");

			List<Map<String, Object>> itemList = (List<Map<String, Object>>) Ndb.execute(rule, "select:item");

			Map<String, Boolean> matchedMap = new HashMap<String, Boolean>();
			for (Map<String, Object> item : itemList) {
				if (item.containsKey("key") && item.containsKey("value")) {
					String key = (String) item.get("key");
					String value = (String) item.get("value");

					if (msgContent.containsKey(key)) {
						matchedMap.put(key, MatchUtils.matchString(value, msgContent.get(key)));
					} else {
						matchedMap.put(key, false);
					}
				}
			}

			return (Boolean) MVEL.eval(expression, matchedMap);

		}
		return false;
	}
}
