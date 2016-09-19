package org.pinae.logmesh.component.custom.rule;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.util.MatchUtils;
import org.pinae.nala.xb.Xml;
import org.pinae.nala.xb.exception.NoSuchPathException;
import org.pinae.nala.xb.exception.UnmarshalException;
import org.pinae.ndb.Ndb;

/**
 * 告警规则匹配
 * 
 * @author Huiyugeng
 * 
 * 
 */
public abstract class AbstractRule {
	private static Logger logger = Logger.getLogger(AbstractRule.class);

	protected List<Map<String, Object>> ruleList = new ArrayList<Map<String, Object>>(); // 告警规则列表
	
	@SuppressWarnings("unchecked")
	protected void load(String path, String filename) {
		logger.info(String.format("Loading Alert Rule File: %s", path + filename));

		Map<String, Object> ruleConfig = null;

		try {
			ruleConfig = (Map<String, Object>)Xml.toMap(new File(path + filename), "UTF8");
		} catch (NoSuchPathException e) {
			logger.error(String.format("Rule Load Exception: exception=%s", e.getMessage()));
		} catch (UnmarshalException e) {
			logger.error(String.format("Rule Load Exception: exception=%s", e.getMessage()));
		}

		if (ruleConfig != null && ruleConfig.containsKey("import")) {
			List<String> importList = (List<String>) Ndb.execute(ruleConfig, "select:import->file");
			for (String file : importList) {
				if (StringUtils.isNotEmpty(file)) {
					load(path, file);
				}
			}
		}

		if (ruleConfig != null && ruleConfig.containsKey("rule")) {
			this.ruleList = (List<Map<String, Object>>) Ndb.execute(ruleConfig, "select:rule");
		}
	}

	/**
	 * 将消息与规则表达式规则进行匹配
	 * 
	 * @param message 消息内容
	 * 
	 * @return 匹配的规则列表
	 */
	protected MatchedRule match(List<Map<String, Object>> ruleList, Message message) {
		MatchedRule matchedRule = new MatchedRule();

		for (Map<String, Object> rule : ruleList) {
			if (rule.containsKey("name")) {
				
				String name = (String) rule.get("name");
				
				String type = message.getType();
				String ip = message.getIP();
				String time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(message.getTimestamp()));
				
				//时间/消息类型/IP地址匹配
				if (MatchUtils.matchString((String) rule.get("type"), type) && MatchUtils.matchString((String) rule.get("ip"), ip)
						&& MatchUtils.matchTime((String) rule.get("time"), time)) {
					
					//消息内容匹配
					if (match(rule, message.getMessage())) {

						int level = 1;
						if (rule.containsKey("level")) {
							try {
								level = Integer.parseInt((String) rule.get("level"));

								level = level > 3 ? level = 3 : level; // 告警最高等级为3
								level = level < 1 ? level = 1 : level; // 告警最低等级为1

							} catch (NumberFormatException e) {
								level = 1;
							}
						}

						matchedRule.addMatchedRule(name);
						matchedRule.setLevel(level);
					}

				}
			}
		}

		return matchedRule;
	}

	public abstract MatchedRule match(Message message);
	
	protected abstract boolean match(Map<String, Object> rule, Object message);

}
