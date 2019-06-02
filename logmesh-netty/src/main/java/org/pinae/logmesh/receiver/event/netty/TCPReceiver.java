package org.pinae.logmesh.receiver.event.netty;

import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.log4j.Logger;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * TCP消息接收器
 * 
 * @author Huiyugeng
 * 
 */
public class TCPReceiver extends NettyReceiver {
	private static Logger logger = Logger.getLogger(TCPReceiver.class.getName());

	private EventLoopGroup masterGroup = new NioEventLoopGroup();
	private EventLoopGroup workerGroup = new NioEventLoopGroup();

	public void initialize(Map<String, Object> config) {
		super.initialize(config);

		logger.info(String.format("Start TCP Receiver AT %d", port));
	}

	/**
	 * 启动TCP消息接收器
	 */
	public void start(String name) {
		super.start(name);

		ServerBootstrap bootstrap = new ServerBootstrap();

		bootstrap.group(masterGroup, workerGroup).channel(NioServerSocketChannel.class).option(ChannelOption.SO_BACKLOG, 1024)
				.childHandler(new TCPMessageHandler());

		try {
			bootstrap.bind(new InetSocketAddress(port)).sync();
		} catch (InterruptedException e) {
			logger.error("TCP Server Exception: exception=" + e.getMessage());
		}

	}

	public void stop() {
		
		masterGroup.shutdownGracefully();
		workerGroup.shutdownGracefully();
		
		isStop = true;
		logger.info("TCP Receiver is Stopped");
	}

	public String getName() {
		return "TCP Receiver AT " + Integer.toString(port);
	}

	private class TCPMessageHandler extends SimpleChannelInboundHandler<Object>  {

		@Override
		protected void channelRead0(ChannelHandlerContext ctx, Object message) throws Exception {
			NioSocketChannel channel = (NioSocketChannel)ctx.channel();
			InetSocketAddress sender = channel.remoteAddress();
			if (sender != null) {
				String ip = sender.getAddress().getHostAddress();
				addMessage(getMessage(ip, (ByteBuf)message));
			}
		}

	}

}
