package org.pinae.logmesh;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.pinae.logmesh.server.MessageServer;
import org.pinae.logmesh.util.ClassLoaderUtils;

public class Runner {
	public static void main(String args[]) {
		
		String path = ClassLoaderUtils.getResourcePath("");
		String serverFile = "server.xml";
		
		Options options = new Options();
		options.addOption("s", true, "Set server config file");

		CommandLineParser parser = new PosixParser(); 
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
			
			if (cmd != null) {
				if (cmd.hasOption("s")) {
					serverFile = cmd.getOptionValue("s");
				}
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}

		MessageServer server = new MessageServer(path, serverFile);
		server.start();

	}
}
