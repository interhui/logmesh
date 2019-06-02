package org.pinae.logmesh.component.router.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.pinae.logmesh.component.router.AbstractRouter;
import org.pinae.logmesh.message.Message;
import org.pinae.ndb.Ndb;

/**
 * 根据源IP路由处理
 * 
 * @author Huiyugeng
 * 
 */
public class IPRouter extends AbstractRouter {

	private Set<String> ruleNameSet = new HashSet<String>();
	private Map<String, List<String>> ipMap = new HashMap<String, List<String>>();

	public IPRouter() {
		super();
	}

	@SuppressWarnings("unchecked")
	public void initialize() {
		super.initialize();
		
		this.ruleNameSet = ruleMap.keySet();
		for (String ruleName : this.ruleNameSet) {
			Map<String, Object> rule = ruleMap.get(ruleName);
			if (rule != null) {
				String type = rule.containsKey("type") ? rule.get("type").toString() : null;
				if (type != null && type.equalsIgnoreCase("ip")) {
					List<String> ipList = (List<String>) Ndb.execute(rule, "select:value");
	
					if (ipList != null && ipList.size() > 0) {
						this.ipMap.put(ruleName, ipList);
					}
				}
			}
		}

	}

	@Override
	public String match(Message message) {
		for (String ruleName : this.ruleNameSet) {
			List<String> ipList = this.ipMap.get(ruleName);
			if (ipList == null) {
				continue;
			}
			for (String ip : ipList) {
				ip = ip.trim();
				if (message.getIP().matches(ip)) {
					return ruleName;
				}
			}
		}
		return null;
	}
}
