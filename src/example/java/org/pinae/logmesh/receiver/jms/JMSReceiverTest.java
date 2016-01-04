package org.pinae.logmesh.receiver.jms;

import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.pinae.logmesh.receiver.JMSReceiver;
import org.pinae.logmesh.receiver.Receiver;

public class JMSReceiverTest {
	
	public static void main(String args[]) {
		Receiver receiver = new JMSReceiver();

		Map<String, Object> parameters = new HashMap<String, Object>();
		parameters.put("username", ActiveMQConnectionFactory.DEFAULT_USER);
		parameters.put("password", ActiveMQConnectionFactory.DEFAULT_PASSWORD);
		parameters.put("url", "tcp://localhost:61616");
		parameters.put("type", "queue");
		parameters.put("target", "test");

		receiver.init(parameters);
		receiver.start("Test");

	}
}