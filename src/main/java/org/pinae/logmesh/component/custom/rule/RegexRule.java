package org.pinae.logmesh.component.custom.rule;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.util.MatchUtils;

/**
 * 告警规则匹配
 * 
 * @author Huiyugeng
 * 
 */
public class RegexRule extends AbstractRule {

	public RegexRule(File ruleFile) {
		super.load(ruleFile);
	}

	public RegexRule(List<Map<String, Object>> alertRuleList) {
		super.ruleList = alertRuleList;
	}

	/**
	 * 将消息与正则表达式规则进行匹配
	 * 
	 * @param message 消息内容
	 * 
	 * @return 匹配的规则列表
	 */
	public MatchedRule match(Message message) {
		return super.match(ruleList, message);
	}

	public boolean matchMessageContent(Map<String, Object> rule, Object message) {
		if (rule == null || message == null) {
			return false;
		}
		return MatchUtils.matchString((String) rule.get("pattern"), message.toString());
	}

}
