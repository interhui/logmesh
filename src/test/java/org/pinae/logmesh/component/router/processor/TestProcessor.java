package org.pinae.logmesh.component.router.processor;

import org.pinae.logmesh.component.MessageProcessor;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.processor.ProcessorInfo;

public class TestProcessor extends ProcessorInfo implements MessageProcessor {

	private String routerType = null;
	
	public void initialize() {
		this.routerType = getStringValue("type", "");
	}

	public void porcess(Message message) {
		String ip = message.getIP();
		String owner = message.getOwner();
		Object msgContent = message.getMessage();

		if (msgContent instanceof String) {
			msgContent = this.routerType + ":" + ip + " " + owner + " " + msgContent.toString();
			message.setMessage(msgContent);
		}
	}

}
