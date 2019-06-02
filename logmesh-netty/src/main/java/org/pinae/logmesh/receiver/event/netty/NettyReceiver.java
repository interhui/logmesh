package org.pinae.logmesh.receiver.event.netty;

import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.receiver.AbstractReceiver;
import org.pinae.logmesh.receiver.EventDrivenReceiver;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;

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
	 * @param ip 发送端IP地址
	 * @param message 消息内容(字节缓冲)
	 * 
	 * @return 消息内容
	 */
	protected Message getMessage(String ip, ByteBuf message) {

		if (message != null) {

			if ("String".equalsIgnoreCase(this.msgType)) {
				try {
					ByteBuf buffer = (ByteBuf) message;
					byte[] bytes = new byte[buffer.readableBytes()];
					buffer.readBytes(bytes);
					
					return new Message(ip, new String(bytes, this.codec));
				} catch (UnsupportedEncodingException e) {

				}
			} else {
				return new Message(ip, message);
			}
			
		}

		return null;
	}

}
