package org.pinae.logmesh;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.pinae.logmesh.server.MessageServer;

public class Runner {
	private static Logger logger = Logger.getLogger(Runner.class);
	
	public static void main(String args[]) {
		
		String serverFile = "server.xml";
		
		Options options = new Options();
		options.addOption("s", true, "Server config file");

		CommandLineParser parser = new DefaultParser(); 
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
			
			if (cmd != null) {
				if (cmd.hasOption("s")) {
					serverFile = cmd.getOptionValue("s");
				}
			}
		} catch (ParseException e) {
			logger.error(String.format("Start server exception: %s", e.getMessage()));
		}

		MessageServer server = new MessageServer(serverFile);
		server.start();

	}
}
