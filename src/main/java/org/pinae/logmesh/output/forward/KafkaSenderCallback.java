package org.pinae.logmesh.output.forward;

import org.apache.kafka.clients.producer.Callback;

/**
 * Kafka消息发送器回调类
 * 
 * @author Huiyugeng
 *
 */
public interface KafkaSenderCallback extends Callback {
	public void setMessage(Object message);
}
