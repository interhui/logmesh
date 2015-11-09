package org.pinae.logmesh.sender;

import java.io.Serializable;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * JMS消息发送器
 * 
 * @author Huiyugeng
 * 
 * 
 */
public class JMSSender implements Sender {

	private ConnectionFactory connectionFactory;
	private Connection connection;
	private Session session;// 一个发送或接收消息的线程
	private Destination destination;// 消息的目的地;消息发送给谁.
	private MessageProducer producer;// 消息发送者

	private String url;
	private String user;
	private String password;
	private String type = "queue";
	private String target;

	/**
	 * 构造函数
	 * 
	 * @param url MQ服务URL地址
	 * @param user 服务用户名
	 * @param password 服务密码
	 * @param type MQ类型：queue 队列，topic 主题
	 * @param target MQ目标
	 */
	public JMSSender(String url, String user, String password, String type, String target) {
		this.url = url;
		this.user = user;
		this.password = password;
		this.type = type;
		this.target = target;
	}

	public void connect() throws SendException {
		connectionFactory = new ActiveMQConnectionFactory(user, password, url);

		try {
			connection = connectionFactory.createConnection();
			connection.start();// 启动
			session = connection.createSession(Boolean.FALSE, Session.AUTO_ACKNOWLEDGE);// 获取操作连接
			if ("queue".equalsIgnoreCase(type)) {
				destination = session.createQueue(target);
			} else {
				destination = session.createTopic(target);
			}
			if (destination != null) {
				producer = session.createProducer(destination);
				producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
			}
		} catch (JMSException e) {
			throw new SendException(e);
		}
	}

	public void send(Object message) throws SendException {
		try {
			Message msg = null;
			if (message != null) {
				if (message instanceof String) {
					msg = session.createTextMessage((String) message);
				} else if (message instanceof Serializable) {
					msg = session.createObjectMessage((Serializable) message);
				}
			}

			if (msg != null) {
				producer.send(msg);
				session.commit();
			}
		} catch (JMSException e) {
			throw new SendException(e);
		}
	}

	public void close() throws SendException {
		try {
			if (null != session) {
				session.close();
			}
			if (null != connection) {
				connection.close();
			}
		} catch (JMSException e) {
			throw new SendException(e);
		}
	}

}
