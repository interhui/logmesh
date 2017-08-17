package org.pinae.logmesh.receiver.event;

import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.log4j.Logger;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;

/**
 * UDP消息接收器
 * 
 * @author Huiyugeng
 * 
 */
public class UDPReceiver extends NettyReceiver {
	private static Logger logger = Logger.getLogger(UDPReceiver.class.getName());

	private EventLoopGroup group = null;

	public void initialize(Map<String, Object> config) {
		super.initialize(config);

		logger.info(String.format("Start UDP Receiver AT %d", port));
	}

	/**
	 * 启动UDP消息接收器
	 */
	public void start(String name) {
		super.start(name);

		EventLoopGroup group = new NioEventLoopGroup();
		
		Bootstrap bootstrap = new Bootstrap();
		bootstrap.group(group).channel(NioDatagramChannel.class);
		bootstrap.handler(new UDPMessageHandler());
		bootstrap.option(ChannelOption.SO_BROADCAST, true);
		
		bootstrap.option(ChannelOption.SO_RCVBUF, 1024 * 1024);  // 设置UDP读缓冲区为1M  
		bootstrap.option(ChannelOption.SO_SNDBUF, 1024 * 1024);  // 设置UDP写缓冲区为1M  

		try {
			bootstrap.bind(new InetSocketAddress(port)).sync().channel();
		} catch (InterruptedException e) {
			logger.error("UDP Server Exception: exception=" + e.getMessage());
		}
		
	}

	public void stop() {
		group.shutdownGracefully();
		isStop = true;
		logger.info("UDP Receiver STOP");
	}

	public String getName() {
		return "UDP Receiver AT " + Integer.toString(port);
	}

	private class UDPMessageHandler extends SimpleChannelInboundHandler<DatagramPacket> {

		@Override
		protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket message) throws Exception {
			InetSocketAddress sender = message.sender();
			if (sender != null) {
				String ip = sender.getAddress().getHostAddress();
				addMessage(getMessage(ip, message.content()));
			}
		}

	}

}
