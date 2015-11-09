package org.pinae.logmesh.component.custom.rule;

import static org.junit.Assert.assertEquals;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Test;
import org.pinae.logmesh.component.custom.rule.MatchedRule;
import org.pinae.logmesh.component.custom.rule.RegexRule;
import org.pinae.logmesh.util.ClassLoaderUtils;

public class RegexRuleTest {
	@Test
	public void testMatch() {
		String path = ClassLoaderUtils.getResourcePath("");
		RegexRule alertRule = new RegexRule(path + "rule/regex_rule.xml");
		
		String today = new SimpleDateFormat("yyyy-MM-dd").format(new Date());

		String log = null;
		MatchedRule matchedRule = null;
		
		log = "Firewall Log: deny 192.168.32.12 to 192.168.99.12";
		matchedRule = alertRule.match("firewall", "192.168.0.1", today + " 13:00:00", log);

		assertEquals(matchedRule.getMatchedRuleList().size(), 1); // Deny Rule
		
		log = "Firewall Log: tcp syn flood attack from 192.168.88.12";
		matchedRule = alertRule.match("firewall", "192.168.0.1", today + " 13:00:00", log);
		
		assertEquals(matchedRule.getMatchedRuleList().size(), 1); //Attack Rule
		
		log = "Firewall Log: host scan flood attack from 192.168.88.12";
		matchedRule = alertRule.match("firewall", "192.168.0.1", today + " 22:00:00", log);
		
		assertEquals(matchedRule.getMatchedRuleList().size(), 0); // Out of time
		
		log = "Firewall Log: udp flood attack from 192.168.88.12";
		matchedRule = alertRule.match("firewall", "192.168.0.2", today + " 22:00:00", log);
		
		assertEquals(matchedRule.getMatchedRuleList().size(), 0); // Out of ip
		
		log = "Firewall Log: drop packet 192.168.12.3 for udp scan attack";
		matchedRule = alertRule.match("firewall", "192.168.0.1", today + " 19:00:00", log);
		
		assertEquals(matchedRule.getMatchedRuleList().size(), 2);

	}
}
