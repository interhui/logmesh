package org.pinae.logmesh.component.custom;

import java.io.UnsupportedEncodingException;

import org.pinae.logmesh.component.MessageProcessor;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.processor.ProcessorInfo;

/**
 * 自定义测试处理器
 * 
 * @author Huiyugeng
 * 
 * 
 */
public class DemoCustomProcessor extends ProcessorInfo implements MessageProcessor {

	public void initialize() {

	}
	
	public Message porcess(Message message) {
		Object msgContent = message.getMessage();

		String msg = null;

		if (msgContent instanceof byte[]) {
			try {
				msg = new String((byte[]) msgContent, "utf8");
			} catch (UnsupportedEncodingException e) {

			}
		} else {
			msg = msgContent.toString();
		}
		msg = message.getIP() + ":" + message.getType() + ":" + msg;
		message.setMessage(msg);
		return message;
	}

}
