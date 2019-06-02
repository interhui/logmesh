package org.pinae.logmesh.message;

import java.util.concurrent.LinkedBlockingQueue;

import org.pinae.logmesh.message.MemoryMessageQueue;

/**
 * 内存模式消息队列 
 * 
 * 基于LinkedBlockingQueue实现
 * 
 * @author Huiyugeng
 * 
 */
public class MemoryMessageQueue extends LinkedBlockingQueue<Message> implements MessageQueue {

	private static final long serialVersionUID = 2669009460413252047L;

	/* 消息队列名称 */
	private String name;

	/* 消息队列计数器 */
	private long count = 0;

	/* 消息队列最大容量 */
	private int maxSize = 0;

	public MemoryMessageQueue(String name) {
		this(name, Integer.MAX_VALUE);
	}

	public MemoryMessageQueue(String name, int maxSize) {
		this.name = name;
		this.maxSize = maxSize;
	}

	@Override
	public boolean offer(Message message) {
		// 如果超出预定容量则根据FIFO原则进行弹出
		synchronized (this) {
			if (super.size() >= this.maxSize) {
				super.poll();
			}
			boolean result = super.offer(message);
			if (result) {
				count++;
			}
			return result;
		}
	}

	@Override
	public Message poll() {
		return super.poll();
	}

	/**
	 * 获取消息队列名称
	 * 
	 * @return 消息队列名称
	 */
	public String getName() {
		return name;
	}
	
	/**
	 * 设置消息队列长度
	 * 
	 * @param size 消息队列长度
	 */
	public void setMaxSize(int size) {
		this.maxSize = size;
	}
	
	/**
	 * 获得消息队列长度
	 * 
	 * @return 消息对了长度
	 */
	public int getMaxSize() {
		return this.maxSize;
	}

	/**
	 * 重置计数器
	 * 
	 * @return 消息队列处理数量
	 */
	public long reset() {
		long c = count;
		count = 0;
		return c;
	}

	/**
	 * 获取消息计数
	 * 
	 * @return 消息计数
	 */
	public long count() {
		return count;
	}

	public String toString() {
		return String.format("Message queue:%s, size:%d", name, maxSize);
	}
}
