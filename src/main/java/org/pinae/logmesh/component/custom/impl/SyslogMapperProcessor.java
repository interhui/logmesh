package org.pinae.logmesh.component.custom.impl;

import org.apache.log4j.Logger;
import org.pinae.logmesh.component.custom.AbstractCustomProcessor;
import org.pinae.logmesh.component.custom.mapper.Mapper;
import org.pinae.logmesh.component.custom.mapper.SyslogMapper;
import org.pinae.logmesh.message.Message;

public class SyslogMapperProcessor extends AbstractCustomProcessor {
	
	private static Logger logger = Logger.getLogger(SyslogMapperProcessor.class);
	
	private Mapper mapper;
	
	public void initialize() {
		this.mapper = new SyslogMapper();
	}

	public Message porcess(Message message) {
		
		Object msgObj = message.getMessage();
		if (this.mapper != null && msgObj != null) {
			String msg = msgObj.toString();
			message.setMessage(mapper.map(msg));
		} else {
			logger.debug("Message content is NULL");
		}
		
		return message;
	}

}
