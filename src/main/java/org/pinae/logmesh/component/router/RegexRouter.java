package org.pinae.logmesh.component.router;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.pinae.logmesh.message.Message;

/**
 * 根据消息内容的正则匹配进行路由处理
 * 
 * @author Huiyugeng
 * 
 */
public class RegexRouter extends AbstractRouter {

	private Set<String> ruleNameSet = new HashSet<String>();
	private Map<String, List<String>> regexMap = new HashMap<String, List<String>>();

	public RegexRouter() {
		super();
	}

	@SuppressWarnings("unchecked")
	public void init() {
		super.init();

		this.ruleNameSet = routerRuleMap.keySet();
		for (String ruleName : this.ruleNameSet) {

			Map<String, Object> rule = routerRuleMap.get(ruleName);
			
			if (rule != null) {
				String type = rule.containsKey("type") ? rule.get("type").toString() : null;
				if (type != null && type.equalsIgnoreCase("regex")) {
					List<String> ipList = (List<String>) statement.execute(rule, "select:value");
	
					if (ipList != null && ipList.size() > 0) {
						this.regexMap.put(ruleName, ipList);
					}
				}
			}
		}
	}

	@Override
	public String match(Message message) {

		for (String ruleName : this.ruleNameSet) {

			List<String> regexList = this.regexMap.get(ruleName);

			if (regexList == null) {
				continue;
			}
			for (String regex : regexList) {
				Object msg = message.getMessage();
				if (msg instanceof byte[]) {
					try {
						msg = new String((byte[]) msg, "utf8");
					} catch (UnsupportedEncodingException e) {

					}
				}

				if (msg != null) {
					regex = regex.trim();
					if (msg.toString().matches(regex)) {
						return ruleName;
					}
				}
			}
		}
		return null;
	}

}
