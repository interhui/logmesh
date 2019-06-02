package org.pinae.logmesh.output;

import org.pinae.logmesh.component.MessageComponent;
import org.pinae.logmesh.message.Message;

/**
 * 消息输出器
 * 
 * @author Huiyugeng
 * 
 */
public interface MessageOutputor extends MessageComponent {
	/* 消息输出器状态 */
	public int status = 0;
	
	/**
	 * 初始化消息输出器
	 */
	public void initialize();

	/**
	 * 输出消息(回调函数)
	 * 
	 * @param message 消息体
	 */
	public void output(Message message);
	
	/**
	 * 关闭输出器
	 */
	public void close();
}
