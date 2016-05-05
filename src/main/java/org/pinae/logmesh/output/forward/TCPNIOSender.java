package org.pinae.logmesh.output.forward;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.handler.codec.string.StringEncoder;
import org.jboss.netty.util.CharsetUtil;

/**
 * TCP NIO 消息发送
 * 
 * @author Huiyugeng
 * 
 */
public class TCPNIOSender implements Sender {
	/* 发送目标地址 */
	private String ip; // 
	/* 发送目标端口 */
	private int port = 514;
	/* 重试次数（每次间隔1s） */
	private int retryTime = 3;

	private ChannelFuture future;
	private ClientBootstrap bootstrap;

	private SimpleChannelHandler handler;

	/**
	 * 构造函数
	 * 
	 * @param ip 发送目标IP地址
	 * @param port 发送目标端口
	 */
	public TCPNIOSender(String ip, int port, int tryTime, SimpleChannelHandler handler) throws SendException {
		this.ip = ip;
		this.port = port;
		this.handler = handler;
		this.retryTime = tryTime;
	}

	public void connect() throws SendException {
		bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool()));

		bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			public ChannelPipeline getPipeline() throws Exception {
				ChannelPipeline pipeline = Channels.pipeline();
				pipeline.addLast("handler", handler);
				pipeline.addLast("stringDecoder", new StringDecoder(CharsetUtil.UTF_8));
				pipeline.addLast("stringEncoder", new StringEncoder(CharsetUtil.UTF_8));
				return pipeline;
			}
		});

		future = bootstrap.connect(new InetSocketAddress(ip, port));

	}

	public void send(Object message) throws SendException {
		for (int i = 0; i < retryTime; i++) {
			if (future != null && future.getChannel().isConnected()) {
				future.getChannel().write(message);
				break;
			} else {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					throw new SendException(e);
				}
			}
		}
	}

	public void close() throws SendException {
		if (bootstrap != null && future != null && future.getChannel().isConnected()) {
			future.getChannel().close();
			bootstrap.releaseExternalResources();
		}

	}

}
