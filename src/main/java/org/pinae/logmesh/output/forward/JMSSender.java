package org.pinae.logmesh.output.forward;

import java.io.Serializable;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * JMS消息发送器
 * 
 * @author Huiyugeng
 * 
 */
public class JMSSender implements Sender {
	/* MQ连接工厂 */
	private ConnectionFactory connectionFactory;
	/* MQ连接 */
	private Connection connection;
	/* 发送或接收消息的线程 */
	private Session session;// 
	/* 消息的目的地 */
	private Destination destination;
	/* 消息生产者 */
	private MessageProducer producer;

	/* MQ服务URL地址 */
	private String url;
	/* MQ服务用户名 */
	private String user;
	/* MQ服务密码 */
	private String password;
	/* MQ类型: queue 队列, topic 主题 */
	private String type;
	/* MQ目标 */
	private String target;

	/**
	 * 构造函数
	 * 
	 * @param url MQ服务URL地址
	 * @param type MQ类型: queue 队列, topic 主题
	 * @param target MQ目标
	 */
	public JMSSender(String url, String type, String target) {
		this.url = url;
		this.user = ActiveMQConnection.DEFAULT_USER;
		this.password = ActiveMQConnection.DEFAULT_PASSWORD;
		this.type = type;
		this.target = target;
	}
	
	/**
	 * 构造函数
	 * 
	 * @param url MQ服务URL地址
	 * @param user MQ服务用户名
	 * @param password MQ服务密码
	 * @param type MQ类型: queue 队列, topic 主题
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
	
		try {
			this.connectionFactory = new ActiveMQConnectionFactory(this.user, this.password, this.url);
			
			this.connection = this.connectionFactory.createConnection();
			this.connection.start(); // 启动连接
			this.session = this.connection.createSession(Boolean.FALSE, Session.AUTO_ACKNOWLEDGE); // 获取会话
			
			if ("queue".equalsIgnoreCase(this.type)) {
				this.destination = this.session.createQueue(this.target);
			} else if ("topic".equalsIgnoreCase(this.type)) {
				this.destination = this.session.createTopic(this.target);
			} else {
				throw new SendException("Unknown MQ Target Type : " + this.type);
			}
			
			if (this.destination != null) {
				this.producer = this.session.createProducer(this.destination);
				this.producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
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
					msg = this.session.createTextMessage((String) message);
				} else if (message instanceof Serializable) {
					msg = this.session.createObjectMessage((Serializable) message);
				}
			}

			if (msg != null) {
				this.producer.send(msg);
				this.session.commit();
			}
		} catch (JMSException e) {
			throw new SendException(e);
		}
	}

	public void close() throws SendException {
		try {
			if (null != this.session) {
				this.session.close();
			}
			if (null != this.connection) {
				this.connection.close();
			}
		} catch (JMSException e) {
			throw new SendException(e);
		}
	}

}
