package org.pinae.logmesh.output.appender;

import org.pinae.logmesh.output.appender.OutStreamAppender;
import org.pinae.logmesh.server.MessageServer;
import org.pinae.logmesh.util.ClassLoaderUtils;

public class SystemOutConsoleTest {

	public static void main(String[] args) {

		String path = ClassLoaderUtils.getResourcePath("");
		MessageServer server = new MessageServer(path + "console.xml");
		server.start();
		
		OutStreamAppender out = new OutStreamAppender(System.out);
		System.setOut(out);

		for (int i = 0; i < 1; i++) {
			System.out.println(i + " Hello, There is a story about Long Long ago there is a country named far far away");
		}
	}

}
