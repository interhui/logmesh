package org.pinae.logmesh.output;

import org.pinae.logmesh.message.Message;

/**
 * 消息输出器
 * 
 * @author Huiyugeng
 * 
 */
public interface MessageOutputor {
	/**
	 * 初始化消息输出器
	 */
	public void initialize();

	/**
	 * 显示消息（回调函数）
	 * 
	 * @param message 消息体
	 */
	public void showMessage(Message message);
}
