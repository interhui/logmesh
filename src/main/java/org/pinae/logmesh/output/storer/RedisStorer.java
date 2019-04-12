package org.pinae.logmesh.output.storer;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.message.MessagePool;
import org.pinae.logmesh.message.MessageQueue;
import org.pinae.logmesh.processor.Processor;
import org.pinae.logmesh.processor.ProcessorFactory;
import org.pinae.logmesh.util.ConfigMap;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisStorer implements Storer {
	private static Logger logger = Logger.getLogger(RedisStorer.class);

	private String host;
	private int port;

	private int db;
	private String password;

	private String storeKey;
	
	private long cycle; // 写入周期
	private long batch; // 批处理数量

	private JedisPool redisPool;
	
	private RedisSaver redisSaver; // Redis存储线程

	private ConfigMap<String, Object> config;
	private MessageQueue messageQueue;

	public RedisStorer(Map<String, Object> config) {
		this(config, MessagePool.getQueue(config.containsKey("queue") ? (String) config.get("queue") : "REDIS_STORE_QUEUE"));
	}

	public RedisStorer(Map<String, Object> config, MessageQueue messageQueue) {
		if (config != null) {
			this.config = new ConfigMap<String, Object>(config);
		}
		this.messageQueue = messageQueue;
	}
	
	public void connect() throws StorerException {
		connect("RedisStore");
	}

	public void connect(String name) throws StorerException {

		this.host = this.config.getString("host", "127.0.0.1");
		this.port = this.config.getInt("port", 6379);

		this.db = this.config.getInt("db", 0);
		this.password = this.config.getString("password", null);

		this.redisPool = new JedisPool(host, port);

		this.storeKey = this.config.getString("key", null);
		if (StringUtils.isBlank(this.storeKey)) {
			throw new NullPointerException("Store Key is empty");
		}
		
		this.cycle = this.config.getLong("cycle", 5000);
		this.batch = this.config.getLong("batch", 5000);
		
		if (messageQueue != null) {
			this.redisSaver = new RedisSaver();
			this.redisSaver.start(name);
		} else {
			logger.error("RedisStorer's MessageQueue is null");
		}
	}

	public void save(Message message) {
		if (this.messageQueue != null) {
			this.messageQueue.offer(message);
		}
	}

	public void close() throws StorerException {
		redisPool.close();
	}
	
	public String handleMessage(Message message) {
		if (message != null) {
			Object msgContent = message.getMessage();
			if (msgContent != null) {
				String msg = new String(msgContent.toString());
				return msg.trim();
			}
		}
		return null;
	}

	private class RedisSaver implements Processor {

		private boolean isStop = false; // 处理线程是否停止

		private Jedis redis;

		public RedisSaver() {
			this.redis = redisPool.getResource();
			this.redis.select(db);
			if (StringUtils.isNoneBlank(password)) {
				this.redis.auth(password);
			}
		}

		public void run() {
			while (!isStop) {
				long count = 0;

				while (!messageQueue.isEmpty() && count < batch) {
					String message = handleMessage(messageQueue.poll());
					if (message != null) {
						this.redis.lpush(storeKey, message);
					}
					count++;
				}
				try {
					Thread.sleep(cycle);
				} catch (InterruptedException e) {

				}
			}
			if (redis != null) {
				redis.close();
			}
		}
		
		public void stop() {
			// 设置线程停止标志
			this.isStop = true; 
			logger.info("RedisStorer STOP");
		}

		public void start(String name) {
			// 设置线程启动标志
			this.isStop = false; 
			// Redis存储线程启动
			ProcessorFactory.getThread(name, this).start(); 
		}
		
		public boolean isRunning() {
			return !this.isStop;
		}

	}



}
