package org.pinae.logmesh.component.router;

import org.pinae.logmesh.message.Message;

/**
 * 消息路由器
 * 
 * @author Huiyugeng
 *
 */
public interface MessageRouter {

	/**
	 * 初始化路由器
	 */
	public void init();

	/**
	 * 路由消息处理
	 * 
	 * @param message 需要处理的消息
	 */
	public void porcess(Message message);
}