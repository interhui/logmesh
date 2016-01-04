package org.pinae.logmesh.component.custom.rule;

import java.util.List;
import java.util.Map;

import org.pinae.logmesh.util.MatchUtils;

/**
 * 告警规则匹配
 * 
 * @author Huiyugeng
 * 
 * 
 */
public class RegexRule extends Rule {
	
	public RegexRule(String filename) {
		super.load("", filename);
	}

	public RegexRule(String path, String filename) {
		super.load(path, filename);
	}

	public RegexRule(List<Map<String, Object>> alertRuleList) {
		super.ruleList = alertRuleList;
	}

	/**
	 * 将消息与正则表达式规则进行匹配
	 * 
	 * @param type 消息类型
	 * @param ip 消息发送地址
	 * @param time 消息发送时间
	 * @param message 消息内容
	 * 
	 * @return 匹配的规则列表
	 */
	public MatchedRule match(String type, String ip, String time, String message) {
		return super.match(ruleList, type, ip, time, message);
	}

	@Override
	public boolean match(Map<String, Object> rule, Object message) {
		if (rule == null || message == null) {
			return false;
		}
		return MatchUtils.matchString((String) rule.get("pattern"), message.toString());
	}
}