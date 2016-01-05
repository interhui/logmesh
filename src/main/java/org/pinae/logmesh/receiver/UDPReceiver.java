package org.pinae.logmesh.receiver;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ConnectionlessBootstrap;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioDatagramChannelFactory;

/**
 * UDP消息接收器
 * 
 * @author Huiyugeng
 * 
 */
public class UDPReceiver extends NettyReceiver {
	private static Logger logger = Logger.getLogger(UDPReceiver.class.getName());

	private ConnectionlessBootstrap bootstrap = null;

	public void init(Map<String, Object> config) {
		super.init(config);

		logger.info(String.format("Start UDP Receiver AT %d", port));
	}

	/**
	 * 启动UDP消息接收器
	 */
	public void start(String name) {
		super.start(name);

		bootstrap = new ConnectionlessBootstrap(new NioDatagramChannelFactory(Executors.newCachedThreadPool()));

		bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			public ChannelPipeline getPipeline() throws Exception {
				ChannelPipeline pipeline = Channels.pipeline();
				pipeline.addLast("handler", new UDPMessageHandler());

				return pipeline;
			}
		});

		bootstrap.bind(new InetSocketAddress(port));
	}

	@Override
	public void stop() {
		bootstrap.releaseExternalResources();
		isStop = true;

		logger.info("UDP Receiver STOP");
	}

	@Override
	public String getName() {
		return "UDP Receiver AT " + Integer.toString(port);
	}

	private class UDPMessageHandler extends SimpleChannelHandler {
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
