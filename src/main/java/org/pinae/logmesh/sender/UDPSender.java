package org.pinae.logmesh.sender;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

import org.pinae.logmesh.util.ObjectUtils;

/**
 * UDP消息发送器
 * 
 * @author Huiyugeng
 * 
 * 
 */
public class UDPSender implements Sender {
	/* 发送目标地址 */
	private String ip;
	/* 发送目标端口 */
	private int port = 514;

	private DatagramSocket socket = null;
	private InetAddress address = null;

	/**
	 * 构造函数
	 * 
	 * @param ip 发送目标IP地址
	 * @param port 发送目标端口
	 */
	public UDPSender(String ip, int port) throws SendException {
		this.ip = ip;
		this.port = port;
	}

	public void connect() throws SendException {
		try {
			this.socket = new DatagramSocket();
			this.address = InetAddress.getByName(ip);
		} catch (SocketException e) {
			throw new SendException(e);
		} catch (UnknownHostException e) {
			throw new SendException(e);
		}
	}

	/**
	 * 发送消息
	 * 
	 * @param message 需要发送的消息
	 * @throws SendException 异常处理
	 */
	public void send(Object message) throws SendException {
		try {
			byte[] data = null;
			if (message != null) {
				if (message instanceof String) {
					data = message.toString().getBytes();
				} else {
					data = ObjectUtils.getBytes(message);
				}
			}
			if (this.socket == null) {
				throw new SendException("Socket is NULL, No connect to any server");
			}
			if (data != null) {
				DatagramPacket packet = new DatagramPacket(data, data.length, address, port);
				this.socket.send(packet);
			}
		} catch (IOException e) {
			throw new SendException(e);
		}
	}

	/**
	 * 关闭连接
	 */
	public void close() {
		this.socket.close();
	}
}
