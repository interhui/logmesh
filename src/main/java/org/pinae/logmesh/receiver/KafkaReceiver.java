package org.pinae.logmesh.receiver;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;
import org.pinae.logmesh.message.Message;

import kafka.utils.ShutdownableThread;

public class KafkaReceiver extends Receiver {
	
	private static Logger logger = Logger.getLogger(KafkaReceiver.class.getName());
	
	private Properties props = new Properties();
	private String topic = "default";
	private long fetchCycle = 1000;
	
	public void init(Map<String, Object> config) {
		super.init(config);
		
		this.props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, super.config.getString("url", "127.0.0.1:9092"));
		this.props.put(ConsumerConfig.CLIENT_ID_CONFIG, super.config.getString("clientId", "default-client"));
		this.props.put(ConsumerConfig.GROUP_ID_CONFIG, super.config.getString("groupId", "default-group"));
		this.props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, super.config.getString("autoCommit", "true"));
		this.props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, super.config.getString("autoCommitInterval", "1000"));
		this.props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, super.config.getString("sessionTimeout", "30000"));
		this.props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, super.config.getString("requestTimeout", "40000"));
		
		this.props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.LongDeserializer");
		this.props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		
		this.topic = super.config.getString("topic", "default");
		this.fetchCycle = super.config.getLong("fetchCycle", 1000);
	}
	
	public void start(String name) {
		super.start(name);

		try {
			MessageConsumer consumer = new MessageConsumer(this.props);
			consumer.start();
		} catch (Exception e) {
			logger.error(String.format("Kafka Receiver Started Error: %s", e.getMessage()));
		}

	}
	
	public void run() {

	}

	@Override
	public void stop() {
		isStop = true;
		logger.info("Kafka Receiver STOP");
	}

	@Override
	public String getName() {
		return String.format("Kafka Receiver AT %s - %s, %s", 
				this.props.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG), 
				this.props.getProperty(ConsumerConfig.GROUP_ID_CONFIG), 
				this.props.getProperty("topic"));
	}

	private class MessageConsumer extends ShutdownableThread {
		
		private KafkaConsumer<Long, String> consumer;
		
		public MessageConsumer(Properties props) {
			super("Kafaka-Receiver", false);
			this.consumer = new KafkaConsumer<Long, String>(props);
		}

		@Override
		public void doWork() {
			this.consumer.subscribe(Collections.singletonList(topic));
			ConsumerRecords<Long, String> records = consumer.poll(fetchCycle);
			for (ConsumerRecord<Long, String> record : records) {
				String message = record.value();
				if (message!= null && message.matches("\\d+.\\d+.\\d+.\\d+:.*")) {

					int split = message.indexOf(":");
					String ip = message.substring(0, split - 1);
					String text = message.substring(split + 1);

					addMessage(new Message(ip, text));
				} else {
					addMessage(new Message(message));
				}
			}
		}
		
	}
}
