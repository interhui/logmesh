package org.pinae.logmesh.server.demo;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.pinae.logmesh.server.MessageServer;
import org.pinae.logmesh.util.ClassLoaderUtils;
import org.pinae.logmesh.util.FileUtils;
import org.pinae.ndb.Ndb;

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
			
			List<Map<String, Object>> processorList = (List<Map<String, Object>>)Ndb.execute(config, "select:processor");
			assertEquals(processorList.size(), 1);
			
			List<Map<String, Object>> queueList = (List<Map<String, Object>>)Ndb.execute(config, "select:queue");
			assertEquals(queueList.size(), 4);
			
			List<Map<String, Object>> filterList = (List<Map<String, Object>>)Ndb.execute(config, "select:filter");
			assertEquals(filterList.size(), 5);
			
			List<Map<String, Object>> receiverList = (List<Map<String, Object>>)Ndb.execute(config, "select:receiver");
			assertEquals(receiverList.size(), 4);
			
			List<Map<String, Object>> outputorList = (List<Map<String, Object>>)Ndb.execute(config, "select:outputor");
			assertEquals(outputorList.size(), 5);
		}
	}
}
