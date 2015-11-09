package org.pinae.logmesh.component;

import org.pinae.logmesh.message.Message;

/**
 * 消息处理类
 * 
 * @author Huiyugeng
 * 
 */
public interface MessageProcessor {
	/**
	 * 初始化消息处理器
	 * 
	 */
	public void init();

	/**
	 * 消息处理方法
	 * 
	 * @param message 需要处理的消息
	 */
	public void porcess(Message message);
}
