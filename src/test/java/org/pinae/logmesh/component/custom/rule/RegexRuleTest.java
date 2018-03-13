package org.pinae.logmesh.component.custom.rule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Test;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.util.FileUtils;

public class RegexRuleTest {
	@Test
	public void testMatch() {
		
		File ruleFile = FileUtils.getFile("rule/regex_rule.xml");
		RegexRule alertRule = new RegexRule(ruleFile);
		
		String today = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
		SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		MatchedRule matchedRule = null;
		
		Message message = null;
		try {
			message = new Message("192.168.0.1", "Test", dateParser.parse(today + " 13:00:00").getTime(), null);
			message.setType("firewall");
		} catch (Exception e) {
			fail(e.getMessage());
		}
		
		try {
			message.setMessage("Firewall Log: deny 192.168.32.12 to 192.168.99.12");
			matchedRule = alertRule.match(message);
			assertEquals(matchedRule.getMatchedRuleList().size(), 1); // Deny Rule
			
			message.setMessage("Firewall Log: tcp syn flood attack from 192.168.88.12");
			matchedRule = alertRule.match(message);
			assertEquals(matchedRule.getMatchedRuleList().size(), 1); //Attack Rule
			
			message.setMessage("Firewall Log: host scan flood attack from 192.168.88.12");
			message.setTimestamp(dateParser.parse(today + " 22:00:00").getTime());
			matchedRule = alertRule.match(message);
			assertEquals(matchedRule.getMatchedRuleList().size(), 0); // Out of time
			
			message.setMessage("Firewall Log: udp flood attack from 192.168.88.12");
			message.setIP("192.168.0.2");
			message.setTimestamp(dateParser.parse(today + " 22:00:00").getTime());
			matchedRule = alertRule.match(message);
			assertEquals(matchedRule.getMatchedRuleList().size(), 0); // Out of ip
			
			message.setMessage("Firewall Log: drop packet 192.168.12.3 for udp scan attack");
			message.setIP("192.168.0.1");
			message.setTimestamp(dateParser.parse(today + " 19:00:00").getTime());
			matchedRule = alertRule.match(message);
			assertEquals(matchedRule.getMatchedRuleList().size(), 2);
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
}
