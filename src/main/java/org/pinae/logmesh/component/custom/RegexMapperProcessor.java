package org.pinae.logmesh.component.custom;

import java.io.File;
import java.util.Map;

import org.apache.log4j.Logger;
import org.pinae.logmesh.component.custom.mapper.Mapper;
import org.pinae.logmesh.component.custom.mapper.RegexMapper;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.util.FileUtils;

public class RegexMapperProcessor extends AbstractCustomProcessor {
	
	private static Logger logger = Logger.getLogger(RegexMapperProcessor.class);
	
	private Mapper mapper;
	
	public void initialize() {
		if (hasParameter("file")) {
			File regexMapperFile = FileUtils.getFile(getStringValue("file", "mapper/regex_mapper.xml"));
			this.mapper = new RegexMapper(regexMapperFile);
		} else {
			logger.error("Regex-Mapper filename is NULL");
		}
		
	}

	@SuppressWarnings("unchecked")
	public Message porcess(Message message) {
		
		Object msgObj = message.getMessage();
		if (this.mapper != null && msgObj != null) {
			if (msgObj instanceof String) {
				String msg = msgObj.toString();
				message.setMessage(mapper.map(msg));
			} else if (msgObj instanceof Map) {
				Map<String, String> msgMap = (Map<String, String>)msgObj;
				if (msgMap.containsKey("message")) {
					Object msg = msgMap.get("message");
					if (msg != null && msg instanceof String) {
						msgMap.putAll(mapper.map(msg.toString()));
						message.setMessage(msgMap);
					}
				}
			} else {
				logger.debug("Message content is not String or Map instance, but " + msgObj.getClass().getName());
			}
		} else {
			logger.debug("Message content is NULL");
		}
		
		return message;
	}

}
