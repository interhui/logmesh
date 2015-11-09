package org.pinae.logmesh.output.appender;

import org.pinae.logmesh.output.appender.OutStreamAppender;
import org.pinae.logmesh.server.LogServer;

public class SystemOutConsoleTest {

	public static void main(String[] args) {

		LogServer server = new LogServer("console.xml");

		OutStreamAppender out = new OutStreamAppender(System.out);
		System.setOut(out);

		for (int i = 0; i < 100000; i++) {
			System.out
					.println(i + " Hello, There is a story about Long Long ago there is a country named far far away");
		}
	}

}
