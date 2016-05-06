package org.pinae.logmesh.output;

import org.apache.log4j.Logger;
import org.pinae.logmesh.component.ComponentInfo;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.output.forward.JMSSender;
import org.pinae.logmesh.output.forward.KafkaSender;
import org.pinae.logmesh.output.forward.SendException;
import org.pinae.logmesh.output.forward.Sender;
import org.pinae.logmesh.output.forward.TCPSender;
import org.pinae.logmesh.output.forward.UDPSender;

/**
 * 消息转发输出
 * 
 * @author Huiyugeng
 * 
 */
public class ForwardOutputor extends ComponentInfo implements MessageOutputor {

	private static Logger logger = Logger.getLogger(ForwardOutputor.class);

	private Sender sender;

	public ForwardOutputor() {

	}

	public void initialize() {

		String protocol = getStringValue("protocol", "udp");
		String destination = getStringValue("destination", "127.0.0.1");

		int dstPort = getIntegerValue("port", 514);

		try {
			if (protocol.equalsIgnoreCase("tcp")) {
				sender = new TCPSender(destination, dstPort);
			} else if (protocol.equalsIgnoreCase("udp")) {
				sender = new UDPSender(destination, dstPort);
			} else if (protocol.equalsIgnoreCase("jms")) {
				String dst[] = destination.split(":");
				if (dst != null) {
					if (dst.length == 3) {
						// JMS URL格式为: url:type:target
						sender = new JMSSender(dst[0], dst[1], dst[2]);
					} else if (dst.length == 5) {
						// JMS URL格式为: url:user:password:type:target
						sender = new JMSSender(dst[0], dst[1], dst[2], dst[3], dst[4]);
					}
				}
			} else if (protocol.equalsIgnoreCase("kafka")) {
				String dst[] = destination.split(":");
				if (dst != null) {
					if (dst.length == 2) {
						// Kafka URL格式为: url:topic
						sender = new KafkaSender(dst[0], dst[1]);
					} else if (dst.length == 3) {
						// Kafka URL格式为: url:topic:client-id
						sender = new KafkaSender(dst[0], dst[1], dst[2], false, null);
					} else if (dst.length == 4) {
						// Kafka URL格式为: url:topic:client-id:callback-class
						sender = new KafkaSender(dst[0], dst[1], dst[2], true, Class.forName(dst[3]));
					}
				}
			}

			if (sender != null) {
				sender.connect(); // 转发器连接
			}
		} catch (Exception e) {
			logger.error(String.format("ForwardOutputor Exception: exception=%s", e.getMessage()));
		}
	}

	public void output(Message message) {
		if (sender != null && message != null) {
			try {
				sender.send(message);
			} catch (SendException e) {
				logger.error(String.format("SendMessage Exception: exception=%s", e.getMessage()));
			}
		}
	}

	public void close() {
		if (sender != null) {
			try {
				sender.close();
			} catch (SendException e) {
				logger.error(String.format("SendMessage Exception: exception=%s", e.getMessage()));
			}
		}
	}

}
