package org.pinae.logmesh.output;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.pinae.logmesh.component.ComponentFactory;
import org.pinae.logmesh.component.MessageComponent;
import org.pinae.ndb.Ndb;

public class MessageOutputorFactory {
	
	private static Logger logger = Logger.getLogger(MessageOutputorFactory.class);
	
	/**
	 * 载入消息输出器列表
	 * 
	 * @param config 消息输出器配置信息
	 * 
	 * @return 消息输出器列表
	 */
	
	public static List<MessageOutputor> create(List<Map<String, Object>> outputorConfigList) {
		List<MessageOutputor> outputorList = new ArrayList<MessageOutputor>();
		
		for (Map<String, Object> outputorConfig : outputorConfigList) {

			MessageComponent outputorComponent = ComponentFactory.create(outputorConfig);

			if (outputorComponent != null && outputorComponent instanceof MessageOutputor) {
				MessageOutputor outputor = (MessageOutputor) outputorComponent;
				outputor.initialize();
				outputorList.add(outputor);
			}
		}

		return outputorList;
	}
	
	@SuppressWarnings("unchecked")
	public static List<Map<String, Object>> getOutputorConfigList(Map<String, Object> config) {
		List<Map<String, Object>> outputorConfigList = (List<Map<String, Object>>) Ndb.execute(config, "select:outputor->enable:true");
		
		for (Map<String, Object> outputorConfig : outputorConfigList) {
			String name = (String)outputorConfig.get("name");
			String className = (String)outputorConfig.get("kwClass");
			if (StringUtils.isNotEmpty(name) && StringUtils.isNotEmpty(className)) {
				logger.info(String.format("Start Message Outputor %s (%s)", name, className));
			}
		}
		
		return outputorConfigList;
	}
}
