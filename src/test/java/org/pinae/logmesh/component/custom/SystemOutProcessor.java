package org.pinae.logmesh.component.custom;

import java.io.UnsupportedEncodingException;

import org.pinae.logmesh.component.MessageProcessor;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.output.appender.OutStreamAppender;

/**
 * 自定义测试处理器
 * 
 * @author Huiyugeng
 * 
 * 
 */
public class SystemOutProcessor implements MessageProcessor {
	private static int id = 1;

	public void init() {
		System.setOut(new OutStreamAppender(System.out));
	}
	
	public void porcess(Message message) {
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

		if (msg != null) {
			System.out.print("MAIN_SERVER " + Integer.toString(id++) + " : " + msg + "\n");
		}
	}

}
