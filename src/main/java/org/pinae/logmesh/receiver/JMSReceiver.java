package org.pinae.logmesh.receiver;

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

/**
 * JMS消息接收器
 * 
 * @author Huiyugeng
 * 
 * 
 */
public class JMSReceiver extends Receiver {
	private static Logger log = Logger.getLogger(JMSReceiver.class.getName());

	private Session session;// 一个发送或接收消息的线程
	private Destination destination;// 消息的目的地;

	private String brokerURL = "tcp://localhost:61616";
	private String type = "queue";
	private String target = "default";

	public void init(Map<String, String> config) {
		super.init(config);

		String username = config.containsKey("username") ? config.get("username") : ActiveMQConnection.DEFAULT_USER;
		String password = config.containsKey("password") ? config.get("password") : ActiveMQConnection.DEFAULT_PASSWORD;

		brokerURL = config.containsKey("url") ? config.get("url") : "tcp://localhost:61616";
		type = config.containsKey("type") ? config.get("type") : "queue";
		target = config.containsKey("target") ? config.get("target") : "default";

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

		}

	}

	public void start(String name) {
		super.start(name);

		try {
			MessageConsumer messageConsumer = session.createConsumer(destination);
			messageConsumer.setMessageListener(new JMSMessageHandler());
		} catch (JMSException e) {

		}

	}

	public void run() {

	}

	@Override
	public void stop() {
		isStop = true;
		log.info("JMS Receiver STOP");
	}

	@Override
	public String getName() {
		return String.format("JMS Receiver AT %s - %s's %s", brokerURL, type, target);
	}

	private class JMSMessageHandler implements MessageListener {

		public void onMessage(javax.jms.Message message) {

			try {
				if (message instanceof TextMessage) {

					String msgContent = ((TextMessage) message).getText();
					System.out.println(msgContent);

					message.acknowledge();
					if (msgContent != null && msgContent.matches("\\d+.\\d+.\\d+.\\d+:.*")) {

						int split = msgContent.indexOf(":");
						String ip = msgContent.substring(0, split - 1);
						String text = msgContent.substring(split + 1);

						addMessage(new Message(ip, text));
					}
				} else if (message instanceof ObjectMessage) {
					Object msgContent = ((ObjectMessage) message).getObject();
					if (msgContent != null && msgContent instanceof Message) {
						addMessage((Message) msgContent);
					}
				}
			} catch (JMSException e) {

			}

		}

	}

}
