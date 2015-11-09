package org.pinae.logmesh.component.custom.rule;

import static org.junit.Assert.assertEquals;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.pinae.logmesh.component.custom.rule.ExpressionRule;
import org.pinae.logmesh.component.custom.rule.MatchedRule;
import org.pinae.logmesh.util.ClassLoaderUtils;

public class ExpressionRuleTest {
	
	@Test
	public void testMatch() {
		String path = ClassLoaderUtils.getResourcePath("");
		ExpressionRule rule = new ExpressionRule(path + "rule/expression_rule.xml");
		
		String today = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
		
		Map<String, String> message = null;
		MatchedRule matchedRule = null;
		
		message = new HashMap<String, String>();
		message.put("name", "ASA");
		message.put("level", "3");
		message.put("message", "deny 192.168.0.32 to 192.168.12.11");
		matchedRule = rule.match("firewall", "192.168.0.1", today + " 13:00:00", message);

		assertEquals(matchedRule.getMatchedRuleList().size(), 3);
		
		message = new HashMap<String, String>();
		message.put("name", "ASA");
		message.put("message", "deny 192.168.0.32 to 192.168.12.12");
		matchedRule = rule.match("firewall", "192.168.0.1", today + " 13:00:00", message);

		assertEquals(matchedRule.getMatchedRuleList().size(), 2); // ASA-Deny & ASA-192.168.0.32
		
		message = new HashMap<String, String>();
		message.put("name", "ASA");
		message.put("level", "3");
		message.put("message", "root login at 192.168.99.13");
		matchedRule = rule.match("firewall", "192.168.0.1", today + " 13:00:00", message);

		assertEquals(matchedRule.getMatchedRuleList().size(), 1); // ASA-HighLevel
		
		message = new HashMap<String, String>();
		message.put("name", "ASA");
		message.put("level", "2");
		message.put("message", "ip flood attack");
		matchedRule = rule.match("firewall", "192.168.0.1", today + " 13:00:00", message);

		assertEquals(matchedRule.getMatchedRuleList().size(), 1); // ASA-HighLevel

	}
}
