package org.pinae.logmesh.processor.imp;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.pinae.logmesh.component.ComponentPool;
import org.pinae.logmesh.component.custom.MessageProcessor;
import org.pinae.logmesh.component.custom.MessageProcessorFactory;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.message.MessagePool;
import org.pinae.logmesh.processor.Processor;
import org.pinae.logmesh.processor.ProcessorFactory;

/**
 * 
 * 自定义处理器线程
 * 
 * @author Huiyugeng
 * 
 */
public class CustomProcessor implements Processor {
	private static Logger logger = Logger.getLogger(CustomProcessor.class);
	
	public static final String GLOBAL_PROCESSOR = "global";
	
	public static final String ROUTER_PROCESSOR = "router";

	/* 消息处理器配置信息 */
	private List<Map<String, Object>> processorConfigList;

	/* 自定义消息处理组件列表 */
	private List<MessageProcessor> processorList = new ArrayList<MessageProcessor>();

	/* 自定义消息处理线程是否停止 */
	private boolean isStop = false;

	public CustomProcessor(List<Map<String, Object>> processorConfigList) {
		this.processorConfigList = processorConfigList;
	}

	public void start(String name) {

		this.processorList = MessageProcessorFactory.create(this.processorConfigList);

		// 将过滤器在组件池中进行注册
		for (MessageProcessor processor : this.processorList) {
			ComponentPool.registe(processor);
		}
		// 设置线程启动标记
		this.isStop = false;
		// 启动自定义处理器线程
		ProcessorFactory.getThread(name, this).start(); 
	}

	public void stop() {
		// 设置线程停止标记
		this.isStop = true;
		logger.info("Custom Processor STOP");
	}
	
	public boolean isRunning() {
		return !this.isStop;
	}

	public void run() {
		while (!isStop) {

			while (!MessagePool.PROCESSOR_QUEUE.isEmpty()) {
				Message message = MessagePool.PROCESSOR_QUEUE.poll();
				for (MessageProcessor processor : processorList) {
					if (processor != null && message != null) {
						message = processor.porcess(message);
					}
				}
				
				if (message != null) {
					MessagePool.OUTPUT_QUEUE.offer(message);
				}
			}
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				logger.error(String.format("CustomProcessor Exception: exception=%s", e.getMessage()));
			}
		}
	}

}
