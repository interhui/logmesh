package org.pinae.logmesh.output.forward;

import java.io.Serializable;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.pinae.logmesh.message.Message;

/**
 * Kafka消息发送器
 * 
 * @author Huiyugeng
 * 
 */
public class KafkaSender implements Sender {
	
	/* 消息主题 */
	private String topic = "default";;
	/* 是否异步模式 */
	private boolean isAsync;
	/* 消息回调类 */
	private Class<?> callbackClass;
	
	private Properties props = new Properties();
	private KafkaProducer<String, String> producer;
	
	public KafkaSender(String url, String topic) {
		this(url, topic, null, false, null);
	}
	
	public KafkaSender(String url, String topic, String clientId, boolean isAsync, Class<?> callbackClass) {
		this.topic = topic;
		this.isAsync = isAsync;
		this.callbackClass = callbackClass;
		
		this.props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, url);
		this.props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		this.props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		
		if (clientId != null) {
			props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
		}
	}
	
	public void connect() throws SendException {
		this.producer = new KafkaProducer<String, String>(this.props);
	}

	public void send(Object message) throws SendException {
		if (message != null) {
			String msgKey = Long.toString(System.currentTimeMillis());
			String msgContent = null;
			if (message instanceof String || message instanceof Message) {
				msgContent = message.toString();
			} else if (message instanceof Serializable) {
				msgContent = message.toString();
			} else {
				throw new SendException("Message is not Serializable");
			}
			
			if (msgKey != null && msgContent != null) {
				try {
					if (this.isAsync && this.callbackClass != null) {
						try {
							Object callbackObj = this.callbackClass.newInstance();
							if (callbackObj != null && callbackObj instanceof KafkaSenderCallback) {
								KafkaSenderCallback senderCallback = (KafkaSenderCallback)callbackObj;
								senderCallback.setMessage(message);
								this.producer.send(new ProducerRecord<String, String>(this.topic, msgKey, msgContent), senderCallback);
							}
						} catch (InstantiationException e) {
							throw new SendException(e);
						} catch (IllegalAccessException e) {
							throw new SendException(e);
						}
					} else {
						this.producer.send(new ProducerRecord<String, String>(this.topic, msgKey, msgContent));
					}
				}catch (Exception e) {
					throw new SendException(e);
				}
			}
		}
		
	}

	public void close() throws SendException {
		this.producer.close();
	}

}
