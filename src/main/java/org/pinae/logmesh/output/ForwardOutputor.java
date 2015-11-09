package org.pinae.logmesh.output;

import org.apache.log4j.Logger;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.processor.ProcessorInfo;
import org.pinae.logmesh.sender.JMSSender;
import org.pinae.logmesh.sender.SendException;
import org.pinae.logmesh.sender.Sender;
import org.pinae.logmesh.sender.TCPSender;
import org.pinae.logmesh.sender.UDPSender;

/**
 * 日志转发输出
 * 
 * @author huiyugeng
 * 
 */
public class ForwardOutputor extends ProcessorInfo implements MessageOutputor {

	private static Logger log = Logger.getLogger(ForwardOutputor.class);

	private Sender sender;

	public ForwardOutputor() {

	}

	public void init() {

		String protocol = hasParameter("protocol") ? getParameter("protocol") : "udp";
		String destination = hasParameter("destination") ? getParameter("destination") : "127.0.0.1";

		int dstPort = 514;
		try {
			dstPort = Integer.parseInt(getParameter("port"));
		} catch (NumberFormatException e) {
			dstPort = 514;
		}

		try {
			if (protocol.equalsIgnoreCase("tcp")) {
				sender = new TCPSender(destination, dstPort);
			} else if (protocol.equalsIgnoreCase("udp")) {
				sender = new UDPSender(destination, dstPort);
			} else if (protocol.equalsIgnoreCase("jms")) {
				String dst[] = destination.split(":");
				if (dst != null && dst.length == 5) {
					sender = new JMSSender(dst[0], dst[1], dst[2], dst[3], dst[4]);
				}
			}

			if (sender != null) {
				sender.connect(); // 转发器连接
			}
		} catch (SendException e) {
			log.error(String.format("ForwardOutputor Exception: exception=%s", e.getMessage()));
		}
	}

	public void showMessage(Message message) {
		if (sender != null && message != null) {
			try {
				sender.send(message.toString());
			} catch (SendException e) {
				log.error(String.format("SendMessage Exception: exception=%s", e.getMessage()));
			}
		}
	}

}
