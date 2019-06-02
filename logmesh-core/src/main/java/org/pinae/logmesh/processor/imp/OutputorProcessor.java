package org.pinae.logmesh.processor.imp;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.pinae.logmesh.component.ComponentPool;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.message.MessagePool;
import org.pinae.logmesh.output.MessageOutputor;
import org.pinae.logmesh.output.MessageOutputorFactory;
import org.pinae.logmesh.processor.Processor;
import org.pinae.logmesh.processor.ProcessorFactory;

/**
 * 
 * 消息输出器线程
 * 
 * @author Huiyugeng
 * 
 */
public class OutputorProcessor implements Processor {
	private static Logger logger = Logger.getLogger(OutputorProcessor.class);
	
	/* 消息输出器配置信息 */
	private List<Map<String, Object>> outputorConfigList;

	/* 消息输出组件列表 */
	private List<MessageOutputor> outputorList = new ArrayList<MessageOutputor>();

	/* 消息输出线程是否停止 */
	private boolean isStop = false;

	public OutputorProcessor(List<Map<String, Object>> outputorConfigList) {
		this.outputorConfigList = outputorConfigList;
	}

	public void run() {
		while (!isStop) {

			while (!MessagePool.OUTPUT_QUEUE.isEmpty()) {
				Message message = MessagePool.OUTPUT_QUEUE.poll();
				if (message != null) {
					for (MessageOutputor outputor : outputorList) {
						if (outputor != null) {
							outputor.output(message);
						}
					}
				}
			}
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				logger.error(String.format("OutputorProcessor Exception: exception=%s", e.getMessage()));
			}
		}
	}

	public void stop() {
		for (MessageOutputor outputor : outputorList) {
			if (outputor != null) {
				outputor.close();
			}
		}
		// 设置线程停止标记
		this.isStop = true;
		logger.info("Outputor Processor STOP");
	}
	
	public boolean isRunning() {
		return !this.isStop;
	}

	public void start(String name) {
		this.outputorList = MessageOutputorFactory.create(this.outputorConfigList);

		// 将过滤器在组件池中进行注册
		for (MessageOutputor outputor : this.outputorList) {
			ComponentPool.registe(outputor);
		}

		// 设置线程启动标记
		this.isStop = false;
		// 启动自定义处理器线程
		ProcessorFactory.getThread(name, this).start();
	}

}
