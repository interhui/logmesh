package org.pinae.logmesh.component.custom.rule;

import java.util.ArrayList;
import java.util.List;

/**
 * 匹配的规则
 * 
 * @author huiyugeng
 * 
 */
public class MatchedRule {
	
	private List<String> matchedRuleList = new ArrayList<String>(); // 匹配的规则

	private int level = 1; // 告警规则级别

	public List<String> getMatchedRuleList() {
		return matchedRuleList;
	}

	public void addMatchedRule(String matchedRule) {
		this.matchedRuleList.add(matchedRule);
	}
	
	public boolean isMatched(){
		return matchedRuleList.size() > 0 ? true : false;
	}

	public int getLevel() {
		return level;
	}

	public void setLevel(int level) {
		if (level > this.level) {
			this.level = level;
		}
	}

}
