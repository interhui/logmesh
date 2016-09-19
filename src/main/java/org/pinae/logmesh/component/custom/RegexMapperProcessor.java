package org.pinae.logmesh.component.custom;

import java.util.Map;

import org.apache.log4j.Logger;
import org.pinae.logmesh.component.custom.mapper.RegexMapper;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.util.ClassLoaderUtils;

public class RegexMapperProcessor extends AbstractCustomProcessor {
	
	private static Logger logger = Logger.getLogger(RegexMapperProcessor.class);
	
	private RegexMapper mapper;
	
	public void initialize() {
		if (hasParameter("file")) {
			String path = ClassLoaderUtils.getResourcePath("");
			String regexFilename = getStringValue("file", "mapper/regex_mapper.xml");
			this.mapper = new RegexMapper(path, regexFilename);
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
				logger.debug("Message content is not String or Map, but " + msgObj.getClass().getName());
			}
		} else {
			logger.debug("Message content is NULL");
		}
		
		return message;
	}

}
