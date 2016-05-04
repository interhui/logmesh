package org.pinae.logmesh.component.merger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.pinae.logmesh.component.MessageComponent;
import org.pinae.logmesh.message.Message;

/**
 * 消息归并器
 * 
 * @author huiyugeng
 *
 */
public interface MessageMerger extends MessageComponent {
	/* 消息归并池 */
	public static Map<String, Message> MERGER_POOL = new ConcurrentHashMap<String, Message>();
	
	/**
	 * 消息归并
	 * 
	 * @param message 消息内容
	 */
	public void add(Message message);
}
