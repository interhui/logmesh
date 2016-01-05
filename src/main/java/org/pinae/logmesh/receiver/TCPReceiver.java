package org.pinae.logmesh.receiver;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

/**
 * TCP消息接收器
 * 
 * @author Huiyugeng
 * 
 */
public class TCPReceiver extends NettyReceiver {
	private static Logger logger = Logger.getLogger(TCPReceiver.class.getName());

	private ServerBootstrap bootstrap = null;

	public void init(Map<String, Object> config) {
		super.init(config);

		logger.info(String.format("Start TCP Receiver AT %d", port));
	}

	/**
	 * 启动TCP消息接收器
	 */
	public void start(String name) {
		super.start(name);

		bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool()));

		bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			public ChannelPipeline getPipeline() throws Exception {
				ChannelPipeline pipeline = Channels.pipeline();
				pipeline.addLast("handler", new TCPMessageHandler());

				return pipeline;
			}
		});

		bootstrap.bind(new InetSocketAddress(port));

	}

	@Override
	public void stop() {
		bootstrap.releaseExternalResources();
		isStop = true;

		logger.info("TCP Receiver STOP");
	}

	@Override
	public String getName() {
		return "TCP Receiver AT " + Integer.toString(port);
	}

	private class TCPMessageHandler extends SimpleChannelHandler {

		public void messageReceived(ChannelHandlerContext ctx, MessageEvent event) throws Exception {
			addMessage(getMessage(event));
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
			logger.info(e.getCause().getMessage());
		}
	}

	@Override
	public void run() {

	}

}
