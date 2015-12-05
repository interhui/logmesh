package org.pinae.logmesh.component.filter;

import java.io.File;
import java.util.Map;

import org.apache.log4j.Logger;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.processor.ProcessorInfo;
import org.pinae.logmesh.util.FileUtils;
import org.pinae.nala.xb.Xml;

/**
 * @author Huiyugeng
 *
 */
public abstract class AbstractFilter extends ProcessorInfo implements MessageFilter {
	
	private static Logger logger = Logger.getLogger(MessageFilter.class);

	public abstract void init();

	public abstract Message filter(Message message);
	
	@SuppressWarnings("unchecked")
	protected Map<String, Object> loadConfig(String path, String filename) {
		Map<String, Object> config = null;

		try {
			File configFile = FileUtils.getFile(path, filename);
			if (configFile != null) {
				config = (Map<String, Object>) Xml.toMap(configFile, "UTF8");
			}
		} catch (Exception e) {
			logger.error(String.format("Filter Load Exception: exception=%s", e.getMessage()));
		}
		
		return config;
	}

}
