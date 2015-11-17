package org.pinae.logmesh.receiver;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.message.MessagePool;
import org.pinae.logmesh.processor.Processor;
import org.pinae.logmesh.processor.ProcessorPool;

/**
 * 消息接收器
 * 
 * @author Huiyugeng
 * 
 */
public abstract class Receiver implements Processor {

	private Map<String, Object> configMap = new HashMap<String, Object>();

	private boolean isRecordOriginal = false; // 是否记录原始日志

	protected boolean isStop = false; // 接收器是否停止

	private String owner; // 日志所有者

	public Receiver() {

	}

	public void setOwner(String owner) {
		if (StringUtils.isEmpty(owner)) {
			owner = "system";
		}
		this.owner = owner;
	}

	public void init(Map<String, Object> config) {

		this.configMap = config;

		try {
			this.isRecordOriginal = Boolean.parseBoolean(getParameter("original"));
		} catch (Exception e) {
			this.isRecordOriginal = false;
		}
	}

	public String getParameter(String key) {
		if (configMap.containsKey(key)) {
			return (String) configMap.get(key);
		} else {
			return null;
		}
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
