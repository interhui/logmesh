package org.pinae.logmesh.processor;

/**
 * 消息处理器线程
 * 
 * @author huiyugeng
 * 
 */
public interface Processor extends Runnable {
	
	public void start(String name);

	public void stop();

	void run();

}
