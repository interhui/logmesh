package org.pinae.logmesh.receiver.pollable;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.receiver.AbstractReceiver;
import org.pinae.logmesh.receiver.PollableReceiver;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisWatcher extends AbstractReceiver implements PollableReceiver {
	private static Logger logger = Logger.getLogger(RedisWatcher.class);

	private String host;
	private int port;
	
	private int db;
	private String password;
	
	private JedisPool redisPool;
	
	private long watchCycle;
	private String watchKey;
	
	private long batchSize;
	
	private String localAddress;
	
	
	public RedisWatcher() {

	}

	public void initialize(Map<String, Object> config) {
		super.initialize(config);

		this.host = super.config.getString("host", "127.0.0.1");
		this.port = super.config.getInt("port", 6379);

		this.db = super.config.getInt("db", 0);
		this.password = super.config.getString("password", null);
		
		this.watchKey = super.config.getString("key", null);
		if (StringUtils.isBlank(this.watchKey)) {
			throw new NullPointerException("Watch Key is empty");
		}
		this.watchCycle = super.config.getLong("cycle", 1000);
		this.batchSize = super.config.getLong("batch", 1000);
		
		try {
			this.localAddress = super.config.getString("address", InetAddress.getLocalHost().getHostAddress());
		} catch (UnknownHostException e) {
			this.localAddress = "127.0.0.1";
		}

	}

	public void stop() {
		isStop = true;
		logger.info("RedisWatcher STOP");
		
	}

	public String getName() {
		return String.format("RedisWatcher AT %s:%d", host, port);
	}
	
	public void start(String name) {
		this.redisPool = new JedisPool(host, port);

		new Thread(new RedisPollable()).start();
		logger.info(String.format("RedisWatcher AT %s:%d", host, port));
	}
	
	private class RedisPollable implements Runnable {
		
		private Jedis redis;
		
		public RedisPollable() {
			this.redis = redisPool.getResource();
			this.redis.select(db);
			if (StringUtils.isNoneBlank(password)) {
				this.redis.auth(password);
			}
		}

		public void run() {
			while (!isStop) {
				long count = 0;
				
				String value = null;
				while (count < batchSize && (value = redis.rpop(watchKey))!= null) {
					addMessage(new Message(localAddress, value.trim()));
					count++;
				}
				try {
					Thread.sleep(watchCycle);
				} catch (InterruptedException e) {

				}
			}
		}
		
	}
}
