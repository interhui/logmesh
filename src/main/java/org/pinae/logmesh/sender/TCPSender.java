package org.pinae.logmesh.sender;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

import org.pinae.logmesh.util.ObjectUtils;

/**
 * TCP消息发送器
 * 
 * @author Huiyugeng
 * 
 */
public class TCPSender implements Sender {
	/* 发送目标地址 */
	private String ip;
	/* 发送目标端口 */
	private int port = 514;

	private Socket socket = null;
	private DataOutputStream output = null;

	/**
	 * 构造函数
	 * 
	 * @param ip 发送目标IP地址
	 * @param port 发送目标端口
	 */
	public TCPSender(String ip, int port) throws SendException {
		this.ip = ip;
		this.port = port;
	}

	public void connect() throws SendException {
		try {
			this.socket = new Socket(ip, port);// 根据服务器名和端口号建立Socket
			this.output = new DataOutputStream(socket.getOutputStream());// 获得Socket的输出流
		} catch (UnknownHostException e) {
			throw new SendException(e);
		} catch (IOException e) {
			throw new SendException(e);
		}
	}

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
			if (this.output == null) {
				throw new SendException("No connect server");
			}
			if (data != null) {
				this.output.write(data);
				this.output.flush();
			}
		} catch (IOException e) {
			throw new SendException(e);
		}
	}

	public void close() throws SendException {
		try {
			this.output.close();
			this.socket.close();
		} catch (IOException e) {
			throw new SendException(e);
		}
	}

}
