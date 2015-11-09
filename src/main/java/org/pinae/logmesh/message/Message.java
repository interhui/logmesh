package org.pinae.logmesh.message;

import java.io.UnsupportedEncodingException;

/**
 * 消息体
 * 
 * @author Huiyugeng
 * 
 */
public class Message implements Cloneable {
	
	private String owner = "system"; // 日志所属
	private String type = ""; // 日志类型
	private Object message; // 消息内容
	private String ip; // 消息发送地址
	private int counter = 1; // 日志归并数量

	private long timestamp = System.currentTimeMillis(); // 消息时间戳

	public Message(Object message) {
		this.message = message;
		this.ip = "127.0.0.1";
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

	public Object getMessage() {
		return message;
	}

	public void setMessage(Object message) {
		this.message = message;
	}

	public String getIP() {
		return ip;
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

	public int getCount() {
		return counter;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public synchronized void incCounter() {
		counter = counter + 1;
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
		if (owner == null || ip == null || message == null) {
			return false;
		}
		
		return ip.equals(msg.getIP()) && owner.equals(msg.getOwner()) && message.equals(msg.getMessage());
	}
	
	public Message clone() throws CloneNotSupportedException {
		Object cloneObj = super.clone();
		if (cloneObj instanceof Message) {
			return (Message)cloneObj;
		}
		return null;
	}
}
