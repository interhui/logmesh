package org.pinae.logmesh.message;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * 消息队列
 * 
 * @author Huiyugeng
 * 
 */
public class MessageQueue extends LinkedBlockingQueue<Message> {

	private static final long serialVersionUID = 2669009460413252047L;

	/* 消息队列名称 */
	private String name;

	/* 消息队列计数器 */
	private long count = 0;

	/* 消息队列最大容量 */
	private int size = 0;

	public MessageQueue(String name) {
		this(name, Integer.MAX_VALUE);
	}

	public MessageQueue(String name, int size) {
		this.name = name;
		this.size = size;
	}

	@Override
	public boolean offer(Message message) {
		// 如果超出预定容量则根据FIFO原则进行弹出
		synchronized (this) {
			if (super.size() >= this.size) {
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
	public void setSize(int size) {
		this.size = size;
	}
	
	/**
	 * 获得消息队列长度
	 * 
	 * @return 消息对了长度
	 */
	public int getSize() {
		return this.size;
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
		return String.format("Queue:%s, Size:%d", name, size);
	}
}
