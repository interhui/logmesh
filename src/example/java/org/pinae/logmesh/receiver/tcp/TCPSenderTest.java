package org.pinae.logmesh.receiver.tcp;

import static org.junit.Assert.fail;

import org.junit.Test;
import org.pinae.logmesh.sender.SendException;
import org.pinae.logmesh.sender.Sender;
import org.pinae.logmesh.sender.TCPSender;

/**
 * TCP消息发送器测试类
 * 
 * @author Huiyugeng
 * 
 * 
 */
public class TCPSenderTest {

	@Test
	public void testSend() {
		String message = "Hello World";
		try {
			Sender sender = new TCPSender("127.0.0.1", 514);
			sender.connect();
			sender.send(message);
			sender.close();
		} catch (SendException e) {
			fail(e.getMessage());
		}
	}
}
