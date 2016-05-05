package org.pinae.logmesh.receiver.udp;

import static org.junit.Assert.fail;

import org.junit.Test;
import org.pinae.logmesh.output.forward.SendException;
import org.pinae.logmesh.output.forward.Sender;
import org.pinae.logmesh.output.forward.UDPSender;

/**
 * UDP消息发送器测试类
 * 
 * @author Huiyugeng
 * 
 * 
 */
public class UDPSenderTest {

	@Test
	public void testSend() {

		String message = "Hello World";

		try {
			Sender sender = new UDPSender("127.0.0.1", 514);
			sender.connect();
			sender.send(message);
			sender.close();
		} catch (SendException e) {
			fail(e.getMessage());
		}
	}
}
