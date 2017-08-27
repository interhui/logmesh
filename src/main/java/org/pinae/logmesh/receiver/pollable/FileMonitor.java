package org.pinae.logmesh.receiver.pollable;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.pinae.logmesh.receiver.AbstractReceiver;
import org.pinae.logmesh.receiver.PollableReceiver;
import org.pinae.logmesh.util.FileReader;
import org.pinae.logmesh.util.FileUtils;

/**
 * 文件监控
 * 
 * @author Huiyugeng
 *
 */
public class FileMonitor extends AbstractReceiver implements PollableReceiver {
	private static Logger logger = Logger.getLogger(FileMonitor.class);

	private File file;
	private String codec;
	private String idxFilePath;
	
	private long watchCycle;
	
	public FileMonitor() {
		
	}
	
	public void initialize(Map<String, Object> config) {
		super.initialize(config);
		
		String filename = super.config.getString("file", null);

		if (StringUtils.isNoneBlank(filename)) {
			this.file = FileUtils.getFile(filename);
			if (file == null) {
				throw new NullPointerException(filename + " is NOT Found");
			}
		} else {
			throw new NullPointerException("Filename is null");
		}

		this.codec = super.config.getString("codec", "utf8");
		this.idxFilePath = super.config.getString("index", null);
		this.watchCycle = super.config.getLong("cycle", 1000);
		
	}

	public void start(String name) {
		super.start(name);

		try {
			new Thread(new FileWatcher()).start();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void stop() {
		isStop = true;
		logger.info("TCP Receiver STOP");
	}

	public String getName() {
		return "FileMonitor AT " + file.getName();
	}

	private class FileWatcher implements Runnable {

		private String localAddress;
		
		private long idxPos;
		
		private File idxFile;

		public FileWatcher() throws IOException {
			
			this.localAddress = InetAddress.getLocalHost().getHostAddress();
		}
		
		private long getPositionFromIndex() throws IOException {
			
			long pos = 0;
			
			if (idxFilePath == null) {
				idxFilePath = file.getName() + ".idx";
			}
			this.idxFile = FileUtils.getFile(idxFilePath);
			if (this.idxFile == null) {
				this.idxFile = new File(idxFilePath);
			}
			String posStr = FileUtils.readFileWithCreate(this.idxFile);
			if (StringUtils.isNoneBlank(posStr) && StringUtils.isNumeric(posStr)) {
				try {
					pos = Long.parseLong(posStr);
				} catch (NumberFormatException e) {
					pos = 0;
				}
			}
			
			return pos;
		}

		public void run() {

			while (!isStop) {
				
				if (file.exists() && file.isFile()) {
					try {
						this.idxPos = getPositionFromIndex();
					} catch (IOException e) {
						logger.info(String.format("Read IndexFile %s Fail: %s", this.idxFile.getPath(), e.getMessage()));
					}
					
					FileReader<String> reader = null;
					if (codec != null) {
						reader = FileReader.create(file, idxPos, codec);
					} else {
						reader = FileReader.create(file, idxPos);
					}
					
					Iterator<List<String>> iterContent = reader.iterator();
					while (iterContent.hasNext()) {
						List<String> rows = iterContent.next();
						for (String row : rows) {
							System.out.println(row.trim());
							//addMessage(new Message(localAddress, row.trim()));
						}
					}
					
					long pos = reader.getPosition();
					try {
						FileUtils.writeFile(this.idxFile, Long.toString(pos));
					} catch (IOException e) {
						logger.info(String.format("Write IndexFile %s Fail: %s", this.idxFile.getPath(), e.getMessage()));
					}
					reader.close();
				}
				
				try {
					Thread.sleep(watchCycle);
				} catch (InterruptedException e) {

				}
			}

		}

	}


}
