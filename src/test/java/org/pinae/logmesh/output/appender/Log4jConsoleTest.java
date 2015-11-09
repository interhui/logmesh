package org.pinae.logmesh.output.appender;

import org.apache.log4j.Logger;
import org.pinae.logmesh.server.LogServer;

public class Log4jConsoleTest {

	private static Logger log = Logger.getLogger(Log4jConsoleTest.class);

	public static void main(String[] args) {

		LogServer server = new LogServer("server.xml");

		for (int i = 0; i < 1; i++) {
			log.info(i + " Hello, There is a story about Long Long ago there is a country named far far away");
		}
	}

}
