package org.pinae.logmesh.output.appender;

import org.pinae.logmesh.output.WindowOutputor;
import org.pinae.logmesh.server.MessageServer;
import org.pinae.logmesh.server.builder.MessageServerBuilder;

public class SystemOutConsoleTest {

	public static void main(String[] args) {
		
		MessageServerBuilder builder = new MessageServerBuilder();
		builder.addOutputor("Windows", true, WindowOutputor.class, null);
		
		MessageServer server = new MessageServer(builder.build());
		server.start();
		
		OutStreamAppender out = new OutStreamAppender(System.out);
		System.setOut(out);

		for (int i = 0; i < 1; i++) {
			System.out.println(i + " Hello, There is a story about Long Long ago there is a country named far far away");
		}
	}

}
