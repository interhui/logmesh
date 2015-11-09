package org.pinae.logmesh.receiver;

import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.MessageEvent;
import org.pinae.logmesh.message.Message;

/**
 * 基于Netty的消息接收器
 * 
 * @author Huiyugeng
 * 
 * 
 */
public abstract class NettyReceiver extends Receiver {

	protected ServerBootstrap bootstrap = null;

	protected int port = 514; // 默认端口
	private String msgType; // 消息内容类型
	private String codec; // 消息内容编码

	public void init(Map<String, String> config) {
		super.init(config);
		try {
			this.port = Integer.parseInt(getParameter("port"));
		} catch (Exception e) {
			this.port = 514;
		}
		this.msgType = getParameter("message");
		this.codec = getParameter("codec");

	}

	/**
	 * 构造消息内容
	 * 
	 * @param event Netty消息事件
	 * @return 消息内容
	 */
	protected Message getMessage(MessageEvent event) {
		ChannelBuffer buffer = (ChannelBuffer) event.getMessage();
		byte[] message = buffer.copy().toByteBuffer().array();

		if (message != null && message.length > 0) {

			String ip = event.getRemoteAddress().toString();

			// 解析IP格式地址，例如(/127.0.0.1:11554)
			if (ip.startsWith("/")) {
				ip = ip.substring(1);
			}
			if (ip.indexOf(":") > 0) {
				String ips[] = ip.split(":");
				if (ips.length == 2) {
					ip = ips[0];
				}
			}

			Object msg = message;
			if (this.msgType != null && this.msgType.equalsIgnoreCase("string")) {
				if (this.codec == null) {
					this.codec = "utf8";
				}
				try {
					msg = new String(message, this.codec);
				} catch (UnsupportedEncodingException e) {

				}

			}
			return new Message(ip, msg);
		}

		return null;
	}

	public void run() {
		// TODO Auto-generated method stub
		
	}
}
