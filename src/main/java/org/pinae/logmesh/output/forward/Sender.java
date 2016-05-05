package org.pinae.logmesh.output.forward;

/**
 * 
 * 消息发送器
 * 
 * @author Huiyugeng
 * 
 * 
 */
public interface Sender {

	/**
	 * 打开连接
	 * 
	 * @throws SendException 异常处理
	 */
	public void connect() throws SendException;

	/**
	 * 发送消息
	 * 
	 * @param message 消息
	 * @throws SendException 异常处理
	 */
	public void send(Object message) throws SendException;

	/**
	 * 关闭连接
	 * 
	 * @throws SendException 异常处理
	 */
	public void close() throws SendException;
}
