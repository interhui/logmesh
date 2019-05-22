package org.pinae.logmesh.component;

import org.pinae.logmesh.message.Message;

/**
 * 消息处理类
 * 
 * @author Huiyugeng
 * 
 */
public interface MessageProcessor extends MessageComponent {
	/**
	 * 消息处理方法
	 * 
	 * @param message 需要处理的消息对象
	 * @return 处理后的消息对象
	 */
	public Message porcess(Message message);
}
