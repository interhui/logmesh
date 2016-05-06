package org.pinae.logmesh.processor.imp;

import java.util.Collection;
import java.util.Map;

import org.apache.log4j.Logger;
import org.pinae.logmesh.component.ComponentFactory;
import org.pinae.logmesh.component.MessageComponent;
import org.pinae.logmesh.component.merger.MessageMerger;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.message.MessagePool;
import org.pinae.logmesh.processor.Processor;
import org.pinae.logmesh.processor.ProcessorFactory;

/**
 * 
 * 消息归并器线程
 * 
 * @author Huiyugeng
 * 
 */
public class MergerProcessor implements Processor {
	private static Logger logger = Logger.getLogger(MergerProcessor.class);

	/* 消息归并器配置信息 */
	private Map<String, Object> config;

	/* 归并周期 */
	private int cycle = 5000;
	/* 消息归并线程是否停止 */
	private boolean isStop;

	/* 消息归并组件 */
	private MessageMerger merger;

	public MergerProcessor(Map<String, Object> config) {
		this.config = config;
	}

	public void start(String name) {
		try {
			this.cycle = Integer.parseInt((String) this.config.get("cycle"));
			if (this.cycle < 1000) {
				this.cycle = 1000;
			}
		} catch (NumberFormatException e) {
			this.cycle = 5000;
		}

		logger.info(String.format("Merger Process Cycle is %d ms", cycle));

		// 生成消息归并器实例
		MessageComponent mergerComponent = ComponentFactory.create(this.config);
		if (mergerComponent != null && mergerComponent instanceof MessageMerger) {
			this.merger = (MessageMerger) mergerComponent;
		}
		this.isStop = false; // 设置线程启动

		// 线程启动
		ProcessorFactory.getThread(name, this).start(); // 启动消息归并线程
	}

	public void stop() {
		this.isStop = true;
		logger.info("Message Merger STOP");
	}

	public void run() {
		while (!isStop) {
			try {
				Thread.sleep(cycle);
			} catch (InterruptedException e) {
				logger.error(String.format("MergerProcessor Exception: exception=%s", e.getMessage()));
			}

			if (merger != null) {
				synchronized (MessagePool.MERGER_QUEUE) {
					// 从归并队列中获取消息, 并加入消息处理
					while (!MessagePool.MERGER_QUEUE.isEmpty()) {
						Message message = MessagePool.MERGER_QUEUE.poll();
						merger.add(message);
					}

					// 从归并输出池中输出归并后的消息
					Collection<Message> messages = MessageMerger.MERGER_POOL.values();
					for (Message message : messages) {
						MessagePool.ROUTE_QUEUE.offer(message);
						MessagePool.PROCESSOR_QUEUE.offer(message);

						// 将消息填入统计队列中
						MessagePool.COUNTER_QUEUE.offer(message);
					}

					// 清理消息归并池
					MessageMerger.MERGER_POOL.clear();
				}
			}
		}
	}

}
