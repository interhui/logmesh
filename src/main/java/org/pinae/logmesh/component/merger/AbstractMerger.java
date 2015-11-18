package org.pinae.logmesh.component.merger;

import java.io.File;
import java.util.Map;

import org.apache.log4j.Logger;
import org.pinae.logmesh.message.Message;
import org.pinae.nala.xb.Xml;

/**
 * 消息合并抽象类
 * 
 * @author Huiyugeng
 *
 */
public abstract class AbstractMerger implements MessageMerger {
	
	private static Logger logger = Logger.getLogger(MessageMerger.class);
	
	@SuppressWarnings("unchecked")
	protected Map<String, Object> loadConfig(String path, String filename) {
		
		logger.info(String.format("Loading Merger Config File: %s", path + filename));
		
		Map<String, Object> config = null;
		try {
			config = (Map<String, Object>) Xml.toMap(new File(path + filename), "UTF8");
		} catch (Exception e) {
			logger.error(String.format("Merger Load Exception: exception=%s", e.getMessage()));
		}
		
		return config;
	}
	
	protected void addToMergerPool(String key, Message message) {
		Message msg = MERGER_POOL.get(key);
		if (msg != null) {
			msg.incCounter();
		} else {
			msg = message;
		}
		if (msg != null) {
			MERGER_POOL.put(key, msg);
		}
	}
	
}
