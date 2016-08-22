package org.pinae.logmesh.message;

/**
 * 消息队列
 * 
 * @author Huiyugeng
 *
 */
public interface MessageQueue {
	
	public boolean offer(Message message);
	
	public Message poll();
	
	public void clear();
	
	public String getName();
	
	public void setMaxSize(int size);
	
	public int getMaxSize();
	
	public long reset();
	
	public long count();
	
	public boolean isEmpty();
}
