package org.pinae.logmesh.component.custom;

import org.pinae.logmesh.message.Message;

/**
 * 默认自定义处理器
 * 
 * @author Huiyugeng
 * 
 */
public class BasicCustomProcessor extends AbstractCustomProcessor {
	
	public void initialize() {

	}

	public Message porcess(Message message) {
		return message;
	}
}
