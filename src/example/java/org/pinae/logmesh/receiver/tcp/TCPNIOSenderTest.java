package org.pinae.logmesh.receiver.tcp;

import static org.junit.Assert.fail;

import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.junit.Test;
import org.pinae.logmesh.output.forward.SendException;
import org.pinae.logmesh.output.forward.Sender;
import org.pinae.logmesh.output.forward.TCPNIOSender;

public class TCPNIOSenderTest {
	
	private static Logger logger = Logger.getLogger(TCPNIOSenderTest.class);

	@Test
	public void testSend() {

		String message = "Hello World";

		try {
			Sender sender = new TCPNIOSender("127.0.0.1", 514, 3, new Processor());
			sender.connect();
			sender.send(message);
			sender.close();
		} catch (SendException e) {
			fail(e.getMessage());
		}
	}

	private class Processor extends SimpleChannelHandler {

		public void messageReceived(ChannelHandlerContext ctx, MessageEvent event) throws Exception {
			ChannelBuffer buffer = (ChannelBuffer) event.getMessage();
			byte[] message = buffer.copy().toByteBuffer().array();
			if (message instanceof byte[]) {
				logger.info(new String((byte[]) message));
			}
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
			logger.error(e.getCause());
		}
	}
}
