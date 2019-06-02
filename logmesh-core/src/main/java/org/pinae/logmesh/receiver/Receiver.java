package org.pinae.logmesh.receiver;

import org.pinae.logmesh.processor.Processor;

/**
 * 消息采集器
 * 
 * @author Huiyugeng
 *
 */
public interface Receiver extends Processor {
	/**
	 * 启动采集器
	 * 
	 * @param name 采集器名称
	 */
	public void start(String name);
	
	/**
	 * 停止采集器
	 */
	public void stop();
	
	/**
	 * 采集器是否运行
	 * 
	 * @return 运行状态(true/false)
	 */
	public boolean isRunning();
	
	/**
	 * 获取采集器名称
	 * 
	 * @return 采集器名称
	 */
	public String getName();
	
}
