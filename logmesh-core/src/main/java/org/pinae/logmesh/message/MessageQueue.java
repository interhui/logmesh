package org.pinae.logmesh.message;

public interface MessageQueue {

	public boolean offer(Message message);

	public Message poll();

	public String getName();
	
	public int getMaxSize();

	public long reset();

	public long count();
	
	public int size();
	
	public boolean isEmpty();
	
	public void clear();
	
}
