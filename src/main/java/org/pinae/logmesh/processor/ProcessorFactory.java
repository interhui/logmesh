package org.pinae.logmesh.processor;

/**
 * 处理器工厂
 * 
 * @author Huiyugeng
 *
 */
public class ProcessorFactory {

	public static Thread getThread(String name, Processor processor) {
		ProcessorPool.addProcessor(name, processor);
		return new Thread(processor, name);
	}
	
}
