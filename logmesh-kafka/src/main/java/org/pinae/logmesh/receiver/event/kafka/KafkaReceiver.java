package org.pinae.logmesh.receiver.event.kafka;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.receiver.AbstractReceiver;
import org.pinae.logmesh.receiver.EventDrivenReceiver;

public class KafkaReceiver extends AbstractReceiver implements EventDrivenReceiver {
	
	private static Logger logger = Logger.getLogger(KafkaReceiver.class.getName());
	
	private Properties props = new Properties();
	/* Kafka消息队列 */
	private String topic = "default";
	/* 消息提交周期 */
	private long fetch = 1000;
	
	public void initialize(Map<String, Object> config) {
		super.initialize(config);
		
		Set<String> keySet = super.config.keySet();
		for (String key : keySet) {
			Object value = super.config.get(key);
			if (value != null) {
				this.props.put(key, value.toString());
			}
		}
		this.props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, super.config.getString("url", "127.0.0.1:9092"));
		if (super.config.containsKey("clientId")) {
			this.props.put(ConsumerConfig.CLIENT_ID_CONFIG, super.config.getString("clientId", null));
		}
		this.props.put(ConsumerConfig.GROUP_ID_CONFIG, super.config.getString("groupId", "default-group"));
		this.props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, super.config.getString("autoCommit", "true"));
		this.props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, super.config.getString("autoCommitInterval", "1000"));
		
		this.props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, super.config.getString("sessionTimeout", "30000"));
		this.props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, super.config.getString("requestTimeout", "40000"));
		this.props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, super.config.getString("heartBeat", "3000"));
		
		this.props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.LongDeserializer");
		this.props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		
		this.topic = super.config.getString("topic", "default");
		this.fetch = super.config.getLong("fetch", 100);
	}
	
	public void start(String name) {
		super.start(name);

		try {
			MessageConsumer consumer = new MessageConsumer(this.props);
			new Thread(consumer, "Kafka-Receiver").start();
		} catch (Exception e) {
			logger.error(String.format("Kafka Receiver Started Error: %s", e.getMessage()));
		}
	}

	public void stop() {
		isStop = true;
		logger.info("Kafka Receiver is Stopped");
	}

	public String getName() {
		return String.format("Kafka Receiver AT %s - %s, %s", 
				this.props.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG), 
				this.props.getProperty(ConsumerConfig.GROUP_ID_CONFIG), 
				this.props.getProperty("topic"));
	}

	private class MessageConsumer implements Runnable {
		
		private KafkaConsumer<String, String> consumer;
		
		public MessageConsumer(Properties props) {
			this.consumer = new KafkaConsumer<String, String>(props);
		}

		public void run() {
			while(!isStop) {
				this.consumer.subscribe(Collections.singletonList(topic));
				ConsumerRecords<String, String> records = consumer.poll(fetch);
				for (ConsumerRecord<String, String> record : records) {
					String key = record.key();
					String message = record.value();
					String keys[] = key.split("$");
					if (message != null && keys != null && keys.length == 3) {
						addMessage(new Message(keys[1], keys[2], message));
					} else {
						addMessage(new Message(message));
					}
				}
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					
				}
			}
			
		}
		
	}
}
