package org.pinae.logmesh.output.storer;

import org.pinae.logmesh.message.Message;

/**
 * 消息存储器
 * 
 * @author Huiyugeng
 * 
 */
public interface Storer {

	/**
	 * 连接存储器
	 * 
	 * @throws StorerException 异常处理
	 */
	public void connect() throws StorerException;

	/**
	 * 存储消息
	 * 
	 * @param message 需要存储的消息
	 */
	public void save(Message message);

	/**
	 * 关闭存储器
	 * 
	 */
	public void close() throws StorerException;;
}
