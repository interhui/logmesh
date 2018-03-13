package org.pinae.logmesh.component.custom;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.pinae.logmesh.component.custom.rule.AbstractRule;
import org.pinae.logmesh.component.custom.rule.MatchedRule;
import org.pinae.logmesh.component.custom.rule.RegexRule;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.util.FileUtils;

public class RegexRuleProcessor extends AbstractCustomProcessor {
	
	private static Logger logger = Logger.getLogger(RegexRuleProcessor.class);
	
	private AbstractRule rule;
	
	public void initialize() {
		if (hasParameter("file")) {
			File regexRuleFile = FileUtils.getFile(getStringValue("file", "rule/regex_rule.xml"));
			this.rule = new RegexRule(regexRuleFile);
		} else {
			logger.error("Regex-Rule filename is NULL");
		}
	}
	
	public Message porcess(Message message) {
		if (this.rule != null && message != null) {
			MatchedRule result = this.rule.match(message);
			if (result != null) {
				Map<String, String> msgMap = new HashMap<String, String>();
				Object msgObj = message.getMessage();
				if (msgObj instanceof String) {
					msgMap.put("message", msgObj.toString());
				}
				msgMap.put("matched_level", Integer.toString(result.getLevel()));
				msgMap.put("matched_rules", StringUtils.join(result.getMatchedRuleList(), ","));
			}
		}
		return message;
	}

}
