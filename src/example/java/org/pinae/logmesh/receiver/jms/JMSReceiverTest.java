package org.pinae.logmesh.receiver.jms;

import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.pinae.logmesh.receiver.AbstractReceiver;
import org.pinae.logmesh.receiver.event.JMSReceiver;

public class JMSReceiverTest {
	
	public static void main(String args[]) {
		AbstractReceiver receiver = new JMSReceiver();

		Map<String, Object> parameters = new HashMap<String, Object>();
		parameters.put("username", ActiveMQConnectionFactory.DEFAULT_USER);
		parameters.put("password", ActiveMQConnectionFactory.DEFAULT_PASSWORD);
		parameters.put("url", "tcp://localhost:61616");
		parameters.put("type", "queue");
		parameters.put("target", "test");

		receiver.initialize(parameters);
		receiver.start("Test");

	}
}
