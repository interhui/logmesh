package org.pinae.logmesh.output.appender;

import org.apache.log4j.Logger;
import org.pinae.logmesh.output.WindowOutputor;
import org.pinae.logmesh.server.MessageServer;
import org.pinae.logmesh.server.builder.MessageServerBuilder;

public class Log4jConsoleTest {

	private static Logger logger = Logger.getLogger(Log4jConsoleTest.class);

	public static void main(String[] args) {
		
		MessageServerBuilder builder = new MessageServerBuilder();
		builder.addOutputor("Windows", true, WindowOutputor.class, null);
		
		MessageServer server = new MessageServer(builder.build());
		server.start();
		
		for (int i = 0; i < 1; i++) {
			logger.info(i + " Hello, There is a story about Long Long ago there is a country named far far away");
		}
	}

}
