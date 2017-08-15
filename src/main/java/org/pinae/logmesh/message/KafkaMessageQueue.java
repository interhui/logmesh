package org.pinae.logmesh.message;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.pinae.logmesh.util.ConfigMap;

/**
 * 基于Kafka的消息队列 
 * 
 * 
 * @author Huiyugeng
 * 
 */
public class KafkaMessageQueue {
	
	private Properties props = new Properties();
	
	private KafkaProducer<String, Message> producer;
	private KafkaConsumer<String, Message> consumer;

	/* 消息队列名称 */
	private String name;

	/* 消息队列计数器 */
	private long count = 0;


	public KafkaMessageQueue(String name, ConfigMap<String, String> config) {
		this.name = name;
		
		this.props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, config.getString("url", "127.0.0.1:9092"));
		if (config.containsKey("clientId")) {
			this.props.put(CommonClientConfigs.CLIENT_ID_CONFIG, config.getString("clientId", null));
		}
		
		this.props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		this.props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

		this.props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getString("groupId", "default-group"));
		this.props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, config.getString("autoCommit", "true"));
		this.props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, config.getString("autoCommitInterval", "1000"));
		
		this.props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, config.getString("sessionTimeout", "30000"));
		this.props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, config.getString("requestTimeout", "40000"));
		this.props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, config.getString("heartBeat", "3000"));
		
		this.props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		this.props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.ByteArrayDeserializer");
		
		this.producer = new KafkaProducer<String, Message>(this.props);
		this.consumer = new KafkaConsumer<String, Message>(this.props);
		
		this.consumer.subscribe(Collections.singletonList(name));
	}

	public boolean offer(Message message) {
		if (message != null) {
			String msgKey = new StringBuffer().append(message.getIP()).append("-").append(Long.toString(message.getTimestamp())).toString();
			producer.send(new ProducerRecord<String, Message>(this.name, msgKey, message));
			return true;
		} else {
			return false;
		}
	}

	public Message poll() {
		ConsumerRecords<String, Message> records = this.consumer.poll(1);
		for (ConsumerRecord<String, Message> record : records) {
			return record.value();
		}
		return null;
	}

	/**
	 * 获取消息队列名称
	 * 
	 * @return 消息队列名称
	 */
	public String getName() {
		return name;
	}
	
	/**
	 * 获得消息队列长度
	 * 
	 * @return 消息对了长度
	 */
	public int getMaxSize() {
		return Integer.MAX_VALUE;
	}

	/**
	 * 重置计数器
	 * 
	 * @return 消息队列处理数量
	 */
	public long reset() {
		long c = count;
		count = 0;
		return c;
	}

	/**
	 * 获取消息计数
	 * 
	 * @return 消息计数
	 */
	public long count() {
		return count;
	}

	public String toString() {
		return String.format("Message queue:%s, size:%d", name, Integer.MAX_VALUE);
	}
}
