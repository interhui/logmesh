package org.pinae.logmesh.output.appender;

import org.apache.log4j.Logger;
import org.pinae.logmesh.server.MessageServer;
import org.pinae.logmesh.util.ClassLoaderUtils;

public class Log4jConsoleTest {

	private static Logger log = Logger.getLogger(Log4jConsoleTest.class);

	public static void main(String[] args) {
		String path = ClassLoaderUtils.getResourcePath("");
		MessageServer server = new MessageServer(path + "console.xml");
		server.start();
		
		for (int i = 0; i < 1; i++) {
			log.info(i + " Hello, There is a story about Long Long ago there is a country named far far away");
		}
	}

}
