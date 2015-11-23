package org.pinae.logmesh.component.custom;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.atomic.AtomicInteger;

import org.pinae.logmesh.component.MessageProcessor;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.output.appender.OutStreamAppender;
import org.pinae.logmesh.processor.ProcessorInfo;

/**
 * 自定义测试处理器
 * 
 * @author Huiyugeng
 * 
 * 
 */
public class SystemOutProcessor extends ProcessorInfo implements MessageProcessor {
	
	private static AtomicInteger MSG_ID = new AtomicInteger(0);
	
	private boolean displayMessage = true;

	public void init() {
		System.setOut(new OutStreamAppender(System.out));
		this.displayMessage = getBooleanValue("display", true);
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

		if (msg != null && displayMessage) {
			System.out.print("MAIN_SERVER " + Integer.toString(MSG_ID.incrementAndGet()) + " : " + msg + "\n");
		}
	}

}
