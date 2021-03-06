package org.pinae.logmesh.output.impl;

import org.apache.log4j.Logger;
import org.pinae.logmesh.component.ComponentInfo;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.output.MessageOutputor;
import org.pinae.logmesh.output.storer.Storer;
import org.pinae.logmesh.output.storer.StorerException;
import org.pinae.logmesh.output.storer.elasticsearch.ElasticsearchStore;
import org.pinae.logmesh.output.storer.redis.RedisStorer;
import org.pinae.logmesh.output.storer.txtfile.TextFileStorer;

/**
 * 消息存储输出
 * 
 * @author Huiyugeng
 * 
 */
public class StorerOutputor extends ComponentInfo implements MessageOutputor {

	private static Logger logger = Logger.getLogger(ForwardOutputor.class);

	private Storer store;

	public void initialize() {

		String type = getStringValue("type", "file");

		try {
			if (type.equalsIgnoreCase("file")) {
				this.store = new TextFileStorer(getParameters());
			} else if (type.equalsIgnoreCase("es")) {
				this.store = new ElasticsearchStore(getParameters());
			} else if (type.equalsIgnoreCase("redis")) {
				this.store = new RedisStorer(getParameters()); 
			}

			if (store != null) {
				// 存储器连接
				store.connect(); 
			}
		} catch (StorerException e) {
			logger.error(String.format("StoreOutputor Exception: exception=%s", e.getMessage()));
		}
	}

	public void output(Message message) {
		if (store != null && message != null) {
			store.save(message);
		}
	}

	public void close() {
		if (store != null) {
			try {
				store.close();
			} catch (StorerException e) {
				logger.error(String.format("StoreOutputor Exception: exception=%s", e.getMessage()));
			}
		}
	}
}
