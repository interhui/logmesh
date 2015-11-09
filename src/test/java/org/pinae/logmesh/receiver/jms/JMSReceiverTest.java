package org.pinae.logmesh.receiver.jms;

import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.pinae.logmesh.receiver.JMSReceiver;
import org.pinae.logmesh.receiver.Receiver;

public class JMSReceiverTest {
	public static void main(String args[]) {
		Receiver receiver = new JMSReceiver();

		Map<String, String> config = new HashMap<String, String>();
		config.put("username", ActiveMQConnectionFactory.DEFAULT_USER);
		config.put("password", ActiveMQConnectionFactory.DEFAULT_PASSWORD);
		config.put("url", "tcp://localhost:61616");
		config.put("type", "queue");
		config.put("target", "test");

		receiver.init(config);
		receiver.start("Test");
		// Thread thread = new Thread(receiver);

	}
}
