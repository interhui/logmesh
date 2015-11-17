package org.pinae.logmesh.receiver.custom;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.receiver.Receiver;

public class TextMessageLoader extends Receiver {

	private String path;
	private long cycle;

	private boolean isStop;

	public void init(Map<String, Object> config) {
		super.init(config);

		this.path = getParameter("path");
		try {
			this.cycle = Integer.parseInt(getParameter("cycle"));
		} catch (NumberFormatException e) {
			this.cycle = 30 * 1000;
		}
	}

	@Override
	public boolean isRunning() {
		return !this.isStop;
	}

	@Override
	public String getName() {
		return "Text Message Loader";
	}

	private List<String> getMessageFileList(String path) {
		List<String> messageFileList = new ArrayList<String>();

		File msgFileDir = new File(path);
		if (msgFileDir.isDirectory()) {
			String fileList[] = msgFileDir.list();

			for (String file : fileList) {
				if (file.endsWith("log")) {
					messageFileList.add(path + "\\" + file);
				}
			}
		}
		return messageFileList;
	}

	public void run() {

	}

	@Override
	public void start(String name) {
		super.start(name);

		List<String> messageFileList = getMessageFileList(this.path);
		try {
			for (String messageFile : messageFileList) {
				System.out.println(messageFile);
				BufferedReader br = new BufferedReader(new FileReader(messageFile));
				String message = "";
				while ((message = br.readLine()) != null) {
					if (StringUtils.isNotEmpty(message.trim())) {
						String msg = message.substring(message.indexOf(":") + 1, message.length());
						String ip = message.substring(0, message.indexOf(":"));

						addMessage(new Message(ip.trim(), msg.trim()));
					}
				}
				br.close();
			}
			Thread.sleep(cycle);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		this.isStop = true;

	}

	@Override
	public void stop() {

	}

}
