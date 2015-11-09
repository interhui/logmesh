package org.pinae.logmesh.receiver.jms;

import org.apache.activemq.ActiveMQConnection;
import org.junit.Test;
import org.pinae.logmesh.sender.JMSSender;
import org.pinae.logmesh.sender.SendException;
import org.pinae.logmesh.sender.Sender;

public class JMSSenderTest {
	@Test
	public void testSend() {
		Sender sender = new JMSSender("tcp://localhost:61616", ActiveMQConnection.DEFAULT_USER,
				ActiveMQConnection.DEFAULT_PASSWORD, "queue", "test");
		try {
			sender.connect();
			sender.send("Hello World");
			sender.close();
		} catch (SendException e) {

		}
	}
}
