package org.pinae.logmesh.receiver.event;

import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.MessageEvent;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.receiver.AbstractReceiver;
import org.pinae.logmesh.receiver.EventDrivenReceiver;

/**
 * 基于Netty的消息接收器
 * 
 * @author Huiyugeng
 * 
 */
public abstract class NettyReceiver extends AbstractReceiver implements EventDrivenReceiver {

	protected ServerBootstrap bootstrap = null;

	/* 消息采集端口 */
	protected int port = 514;
	/* 消息内容类型 */
	private String msgType;
	/* 消息内容编码 */
	private String codec;

	public void initialize(Map<String, Object> config) {
		super.initialize(config);
		
		this.port = super.config.getInt("port", 514);
		this.msgType = super.config.getString("message", "String");
		this.codec = super.config.getString("codec", "utf8");
	}

	/**
	 * 构造消息内容
	 * 
	 * @param event Netty消息事件
	 * 
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
			if ("String".equalsIgnoreCase(this.msgType)) {
				try {
					msg = new String(message, this.codec);
				} catch (UnsupportedEncodingException e) {

				}
			}
			return new Message(ip, msg);
		}

		return null;
	}

}
