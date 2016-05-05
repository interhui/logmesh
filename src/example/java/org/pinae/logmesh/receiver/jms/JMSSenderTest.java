package org.pinae.logmesh.receiver.jms;

import static org.junit.Assert.fail;

import org.apache.activemq.ActiveMQConnection;
import org.junit.Test;
import org.pinae.logmesh.output.forward.JMSSender;
import org.pinae.logmesh.output.forward.SendException;
import org.pinae.logmesh.output.forward.Sender;

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
			fail(e.getMessage());
		}
	}
	
}
