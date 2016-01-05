package org.pinae.logmesh.receiver;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.message.MessagePool;
import org.pinae.logmesh.processor.Processor;
import org.pinae.logmesh.processor.ProcessorPool;
import org.pinae.logmesh.util.ConfigMap;

/**
 * 消息接收器
 * 
 * @author Huiyugeng
 * 
 */
public abstract class Receiver implements Processor {

	private boolean isRecordOriginal = false; // 是否记录原始日志

	protected boolean isStop = false; // 接收器是否停止

	private String owner; // 日志所有者

	protected ConfigMap<String, Object> config;
	
	public Receiver() {

	}

	public void setOwner(String owner) {
		if (StringUtils.isEmpty(owner)) {
			owner = "system";
		}
		this.owner = owner;
	}

	public void init(Map<String, Object> config) {
		if (config != null) {
			this.config = new ConfigMap<String, Object>(config);
		}

		this.isRecordOriginal = this.config.getBoolean("original", false);
	}

	protected void addMessage(Message message) {
		if (message != null) {
			message.setOwner(owner);
			if (isRecordOriginal) {
				MessagePool.ORIGINAL_QUEUE.offer(message); // 加入原始日志处理队列
			}
			MessagePool.FILTER_QUEUE.offer(message); // 加入处理队列
		}
	}

	public void start(String name) {
		ProcessorPool.addProcessor(name, this);
		isStop = false;
	}

	public boolean isRunning() {
		return isStop;
	}

	public abstract void stop();

	public abstract String getName();

}
