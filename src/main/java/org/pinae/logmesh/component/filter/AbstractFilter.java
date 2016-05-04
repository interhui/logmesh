package org.pinae.logmesh.component.filter;

import java.io.File;
import java.util.Map;

import org.apache.log4j.Logger;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.processor.ProcessorInfo;
import org.pinae.nala.xb.Xml;

/**
 * 抽象消息过滤器
 * 
 * @author Huiyugeng
 *
 */
public abstract class AbstractFilter extends ProcessorInfo implements MessageFilter {
	
	private static Logger logger = Logger.getLogger(MessageFilter.class);

	public abstract void initialize();

	public abstract Message filter(Message message);
	
	@SuppressWarnings("unchecked")
	protected Map<String, Object> loadConfig(File fileterFile) {
		Map<String, Object> config = null;

		try {
			if (fileterFile != null) {
				config = (Map<String, Object>) Xml.toMap(fileterFile, "UTF8");
			}
		} catch (Exception e) {
			logger.error(String.format("Filter Load Exception: exception=%s", e.getMessage()));
		}
		
		return config;
	}

}
