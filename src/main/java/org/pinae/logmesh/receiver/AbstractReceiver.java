package org.pinae.logmesh.receiver;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.message.MessagePool;
import org.pinae.logmesh.processor.ProcessorPool;
import org.pinae.logmesh.util.ConfigMap;

/**
 * 消息采集器抽象类
 * 
 * @author Huiyugeng
 * 
 */
public abstract class AbstractReceiver implements Receiver {

	/* 是否记录原始日志 */
	private boolean isRecordOriginal = false; 
	/* 接收器是否停止 */
	protected boolean isStop = false; 
	/* 日志所有者 */
	private String owner; 

	/* 采集器配置信息 */
	protected ConfigMap<String, Object> config;
	
	public AbstractReceiver() {

	}

	/**
	 * 设置日志所有者
	 * 
	 * @param owner 日志所有者
	 */
	public void setOwner(String owner) {
		if (StringUtils.isEmpty(owner)) {
			owner = "system";
		}
		this.owner = owner;
	}

	/**
	 * 采集器初始化
	 * 
	 * @param config 采集器配置信息
	 */
	public void init(Map<String, Object> config) {
		if (config != null) {
			this.config = new ConfigMap<String, Object>(config);
		}

		this.isRecordOriginal = this.config.getBoolean("original", false);
	}

	/**
	 * 向采集器中增加消息
	 * 
	 * @param message 消息内容
	 */
	protected void addMessage(Message message) {
		if (message != null) {
			message.setOwner(owner);
			if (isRecordOriginal) {
				MessagePool.ORIGINAL_QUEUE.offer(message); // 加入原始日志处理队列
			}
			MessagePool.FILTER_QUEUE.offer(message); // 加入过滤处理队列
		}
	}

	public void start(String name) {
		ProcessorPool.addProcessor(name, this);
		isStop = false;
	}

	public boolean isRunning() {
		return isStop;
	}
	
	public void run() {
		// Do nothing
	}

}
