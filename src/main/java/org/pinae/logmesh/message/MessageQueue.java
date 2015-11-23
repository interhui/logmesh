package org.pinae.logmesh.message;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * 消息队列
 * 
 * @author Huiyugeng
 * 
 * 
 */
public class MessageQueue extends LinkedBlockingQueue<Message> {

	private static final long serialVersionUID = 2669009460413252047L;

	private String name; // 消息队列名称

	private long count = 0; // 消息队列计数器

	private int size = 0; // 消息队列最大容量

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
	 * 重置计数器
	 * 
	 * @return 消息队列处理数量
	 */
	public long resetCounter() {
		long _count = count;
		count = 0;
		return _count;
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
		return name;
	}
}
