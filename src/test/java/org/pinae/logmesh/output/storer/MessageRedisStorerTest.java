package org.pinae.logmesh.output.storer;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.pinae.logmesh.message.MemoryMessageQueue;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.output.MessageMaker;
import org.pinae.logmesh.output.storer.RedisStorer;
import org.pinae.logmesh.output.storer.Storer;
import org.pinae.logmesh.output.storer.StorerException;
import org.pinae.logmesh.util.ConfigMap;

public class MessageRedisStorerTest extends MessageMaker implements Runnable {
	
	private static Logger logger = Logger.getLogger(MessageRedisStorerTest.class);
	
	private Storer storer;
	
	private long sleep;
	
	public MessageRedisStorerTest(long sleep) {
		
		Map<String, Object> config = new ConfigMap<String, Object>();
		config.put("host", "127.0.0.1");
		config.put("port", "6379");
		config.put("db", "5");
		config.put("key", "log");
		config.put("cycle", "100");
		
		this.sleep = sleep;
		
		try {
			this.storer = new RedisStorer(config, new MemoryMessageQueue("REDIS_QUEUE"));
			this.storer.connect();
		} catch (StorerException e) {
			logger.error(e.getMessage());
		}
	}
	
	public void run() {
		try {
			while(true) {
				Message message = new Message(getMessage());
				storer.save(message);
				TimeUnit.SECONDS.sleep(sleep);
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

}
