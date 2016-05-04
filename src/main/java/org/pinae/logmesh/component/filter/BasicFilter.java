package org.pinae.logmesh.component.filter;

import org.pinae.logmesh.message.Message;

/**
 * 默认过滤器
 * 
 * @author Huiyugeng
 * 
 */
public class BasicFilter extends AbstractFilter {
	
	
	@Override
	public Message filter(Message message) {
		return message;
	}

	@Override
	public void initialize() {
		
	}
	


}
