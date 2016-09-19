package org.pinae.logmesh.component.custom.rule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.util.ClassLoaderUtils;

public class ExpressionRuleTest {
	
	@Test
	public void testMatch() {
		String path = ClassLoaderUtils.getResourcePath("");
		ExpressionRule rule = new ExpressionRule(path + "rule/expression_rule.xml");
		
		String today = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
		SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		Map<String, String> messageContent = null;
		MatchedRule matchedRule = null;
		
		Message message = null;
		try {
			message = new Message("192.168.0.1", "Test", dateParser.parse(today + " 13:00:00").getTime(), null);
			message.setType("firewall");
		} catch (Exception e) {
			fail(e.getMessage());
		}
		
		messageContent = new HashMap<String, String>();
		messageContent.put("name", "ASA");
		messageContent.put("level", "3");
		messageContent.put("message", "deny 192.168.0.32 to 192.168.12.11");
		message.setMessage(messageContent);
		matchedRule = rule.match(message);
		assertEquals(matchedRule.getMatchedRuleList().size(), 3);
		
		messageContent = new HashMap<String, String>();
		messageContent.put("name", "ASA");
		messageContent.put("message", "deny 192.168.0.32 to 192.168.12.12");
		message.setMessage(messageContent);
		matchedRule = rule.match(message);
		assertEquals(matchedRule.getMatchedRuleList().size(), 2); // ASA-Deny & ASA-192.168.0.32
		
		messageContent = new HashMap<String, String>();
		messageContent.put("name", "ASA");
		messageContent.put("level", "3");
		messageContent.put("message", "root login at 192.168.99.13");
		message.setMessage(messageContent);
		matchedRule = rule.match(message);
		assertEquals(matchedRule.getMatchedRuleList().size(), 1); // ASA-HighLevel
		
		messageContent = new HashMap<String, String>();
		messageContent.put("name", "ASA");
		messageContent.put("level", "2");
		messageContent.put("message", "ip flood attack");
		message.setMessage(messageContent);
		matchedRule = rule.match(message);
		assertEquals(matchedRule.getMatchedRuleList().size(), 1); // ASA-HighLevel


	}
}
