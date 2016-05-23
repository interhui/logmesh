package org.pinae.logmesh.receiver.event;

import org.pinae.logmesh.receiver.Receiver;

/**
 * RockerMQ采集器
 * 
 * @author Huiyugeng
 *
 */
public class RocketMQReceiver implements Receiver {
	
	public RocketMQReceiver(String nsUrl, String topic, Auth auth) {
		
	}

	public void run() {
		
	}

	public void start(String name) {

	}

	public void stop() {
	}

	public boolean isRunning() {
		return false;
	}

	public String getName() {
		return null;
	}
	
	/**
	 * RockerMQ认证
	 *
	 */
	public class Auth {
		
	}

}
