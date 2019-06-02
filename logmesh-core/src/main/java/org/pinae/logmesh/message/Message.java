package org.pinae.logmesh.message;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 消息体
 * 
 * @author Huiyugeng
 * 
 */
public class Message implements Cloneable, Serializable {

	private static final long serialVersionUID = 827069274403052860L;
	
	/* 消息所属者 */
	private String owner = "system";
	/* 消息类型 */
	private String type = "unknown";
	/* 消息内容 */
	private Object message;
	/* 消息发送地址 */
	private String ip;
	/* 消息归并计数 */
	private AtomicLong counter = new AtomicLong(1);
	/* 消息时间戳 */
	private long timestamp = System.currentTimeMillis();

	public Message(Object message) {
		this.message = message;
		this.ip = "0.0.0.0";
	}

	public Message(String ip, Object message) {
		this.message = message;
		this.ip = ip;
	}

	public Message(String ip, String owner, Object message) {
		this.message = message;
		this.owner = owner;
		this.ip = ip;
	}
	
	public Message(String ip, String owner, long timestamp, Object message) {
		this.message = message;
		this.owner = owner;
		this.ip = ip;
		this.timestamp = timestamp;
	}

	public Object getMessage() {
		return message;
	}

	public void setMessage(Object message) {
		this.message = message;
	}

	public String getIP() {
		return ip;
	}
	
	public void setIP(String ip) {
		this.ip = ip;
	}

	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public long getCount() {
		return counter.get();
	}

	public long getTimestamp() {
		return timestamp;
	}
	
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public synchronized long incCounter() {
		return counter.incrementAndGet();
	}

	public String toString() {
		String msg = null;

		if (message != null) {
			if (message instanceof byte[]) {
				try {
					msg = new String((byte[]) message, "utf8");
				} catch (UnsupportedEncodingException e) {

				}
			} else {
				msg = message.toString();
			}
		}

		return msg;
	}

	public boolean equals(Message msg) {
		if (msg == null) {
			return false;
		}
		if (owner == null || ip == null || message == null || type == null) {
			return false;
		}

		return ip.equals(msg.getIP()) && owner.equals(msg.getOwner()) && 
				type.equals(msg.getType()) && message.equals(msg.getMessage());
	}

	public Message clone() throws CloneNotSupportedException {
		Object cloneObj = super.clone();
		if (cloneObj instanceof Message) {
			return (Message) cloneObj;
		}
		return null;
	}
}
