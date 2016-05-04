package org.pinae.logmesh.component.filter;

import org.pinae.logmesh.component.MessageComponent;
import org.pinae.logmesh.message.Message;

/**
 * 消息过滤器
 * 
 * @author Huiyugeng
 * 
 * 
 */
public interface MessageFilter extends MessageComponent {

	/**
	 * 初始化过滤器
	 */
	public void init();

	/**
	 * 过滤消息
	 * 
	 * @param message 消息内容
	 * @return 过滤后的内容（返回null为消息被过滤）
	 */
	public Message filter(Message message);
}
