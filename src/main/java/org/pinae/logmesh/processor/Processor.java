package org.pinae.logmesh.processor;

/**
 * 消息处理器线程
 * 消息处理器
 * 
 * @author huiyugeng
 * 
 */
public interface Processor extends Runnable {
	
	/**
	 * 启动消息处理器
	 * 
	 * @param name 处理器名称
	 */
	public void start(String name);

	/**
	 * 停止消息处理器
	 */
	public void stop();
	
	/**
	 * 处理器是否在运行状态
	 * 
	 * @return true 运行状态 , false 停止状态
	 */
	public boolean isRunning();

}
