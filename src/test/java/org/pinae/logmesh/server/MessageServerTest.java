package org.pinae.logmesh.server;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.pinae.logmesh.util.ClassLoaderUtils;
import org.pinae.logmesh.util.FileUtils;

public class MessageServerTest {
	
	@SuppressWarnings("unchecked")
	@Test
	public void testLoadConfig() {		
		MessageServer server = new MessageServer();
		
		String path = ClassLoaderUtils.getResourcePath("");
		File configFile = FileUtils.getFile(path, "server.xml");
		if (configFile != null) {
			Map<String, Object> config = server.loadConfig(configFile);
			
			assertEquals(config.get("owner"), "Pinae");
			
			List<Map<String, Object>> processorList = (List<Map<String, Object>>)config.get("processor");
			assertEquals(processorList.size(), 3);
			
			List<Map<String, Object>> queueList = (List<Map<String, Object>>)config.get("queue");
			assertEquals(queueList.size(), 3);
			
			List<Map<String, Object>> filterList = (List<Map<String, Object>>)config.get("filter");
			assertEquals(filterList.size(), 5);
			
			List<Map<String, Object>> receiverList = (List<Map<String, Object>>)config.get("receiver");
			assertEquals(receiverList.size(), 4);
			
			List<Map<String, Object>> outputorList = (List<Map<String, Object>>)config.get("output");
			assertEquals(outputorList.size(), 2);
		}
	}
}
