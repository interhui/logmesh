package org.pinae.logmesh.receiver.event;

import java.util.Map;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.log4j.Logger;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.receiver.AbstractReceiver;
import org.pinae.logmesh.receiver.EventDrivenReceiver;

/**
 * JMS消息接收器
 * 
 * @author Huiyugeng
 * 
 */
public class JMSReceiver extends AbstractReceiver implements EventDrivenReceiver {
	private static Logger logger = Logger.getLogger(JMSReceiver.class.getName());

	/* 一个发送或接收消息的线程 */
	private Session session; 
	/* 消息的目的地 */
	private Destination destination;

	/* JMS 服务器地址 */
	private String brokerURL;
	/* 消息目标类型: queue 队列, topic 主题 */
	private String type;
	/* 消息目标 */
	private String target;

	public void initialize(Map<String, Object> config) {
		super.initialize(config);

		String username = super.config.getString("username", ActiveMQConnection.DEFAULT_USER);
		String password = super.config.getString("password", ActiveMQConnection.DEFAULT_PASSWORD);

		this.brokerURL = super.config.getString("url", "tcp://localhost:61616");
		this.type = super.config.getString("type", "queue");
		this.target = super.config.getString("target", "default");

		try {
			ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(username, password, brokerURL);
			Connection connection = connectionFactory.createConnection();

			connection.start();// 启动
			session = connection.createSession(Boolean.FALSE, Session.AUTO_ACKNOWLEDGE);// 获取操作连接

			if ("queue".equalsIgnoreCase(type)) {
				destination = session.createQueue(target);
			} else if ("topic".equalsIgnoreCase(type)) {
				destination = session.createTopic(target);
			}

		} catch (JMSException e) {
			logger.error(String.format("JMS Receiver Started Error : url=%s, type=%s, target=%s, exception=%s", 
					brokerURL, type, target, e.getMessage()));
		}

	}

	public void start(String name) {
		super.start(name);

		try {
			MessageConsumer messageConsumer = session.createConsumer(destination);
			messageConsumer.setMessageListener(new JMSMessageHandler());
		} catch (JMSException e) {
			logger.error(String.format("JMS Receiver Started Error: %s", e.getMessage()));
		}

	}

	public void stop() {
		isStop = true;
		logger.info("JMS Receiver STOP");
	}

	public String getName() {
		return String.format("JMS Receiver AT %s - %s %s", brokerURL, type, target);
	}

	private class JMSMessageHandler implements MessageListener {
		
		public void onMessage(javax.jms.Message message) {
			
			try {
				
				if (message instanceof TextMessage) {

					String msgContent = ((TextMessage) message).getText();

					if (msgContent != null && msgContent.matches("\\d+.\\d+.\\d+.\\d+:.*")) {

						int split = msgContent.indexOf(":");
						String ip = msgContent.substring(0, split - 1);
						String text = msgContent.substring(split + 1);

						addMessage(new Message(ip, text));
					} else {
						addMessage(new Message(message));
					}
					
					message.acknowledge();
				} else if (message instanceof ObjectMessage) {
					Object msgContent = ((ObjectMessage) message).getObject();
					if (msgContent != null && msgContent instanceof Message) {
						addMessage((Message) msgContent);
					}
					message.acknowledge();
				}
			} catch (JMSException e) {
				logger.error(String.format("JMS Message Handle Error: %s ", e.getMessage()));
			}

		}

	}

}
