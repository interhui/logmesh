package org.pinae.logmesh.component.router.processor;

import org.apache.log4j.Logger;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.output.MessageOutputor;

public class TestOutputor implements MessageOutputor {

	private static Logger logger = Logger.getLogger(TestOutputor.class);

	public void initialize() {

	}

	public void showMessage(Message message) {
		if (message != null && message.getMessage() != null) {
			logger.info(message.getMessage().toString());
		}
	}

}
