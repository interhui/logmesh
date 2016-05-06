package org.pinae.logmesh.component.custom;

import java.io.UnsupportedEncodingException;

import org.apache.commons.lang.StringUtils;
import org.pinae.logmesh.component.MessageProcessor;
import org.pinae.logmesh.component.ComponentInfo;
import org.pinae.logmesh.message.Message;

/**
 * 自定义测试处理器
 * 
 * @author Huiyugeng
 * 
 */
public class DemoCustomProcessor extends ComponentInfo implements MessageProcessor {

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
		message.setMessage(StringUtils.join(new String[]{"MAIN_SERVER", message.getIP(),  message.getType(), msg}, ":"));
		return message;
	}

}
