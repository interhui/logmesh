package org.pinae.logmesh.output.storer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.message.MessagePool;
import org.pinae.logmesh.message.MemoryMessageQueue;
import org.pinae.logmesh.processor.Processor;
import org.pinae.logmesh.processor.ProcessorFactory;
import org.pinae.logmesh.util.ConfigMap;

/**
 * 文件存储
 * 
 * @author Huiyugeng
 * 
 */
public class FileStorer implements Storer {
	private static Logger logger = Logger.getLogger(FileStorer.class);

	private String path; // 文件路径
	private String fileTitle; // 文件标题
	private String fileExt; // 文件扩展名
	private String encoding; // 消息编码
	private long cycle; // 写入周期

	private SimpleDateFormat dirPattern; // 通过日期构建的目录格式
	private SimpleDateFormat filePattern; // 通过日期构建的文件格式

	private FileSaver fileSaver; // 文件存储线程

	private ConfigMap<String, Object> config;
	private MemoryMessageQueue messageQueue;

	public FileStorer(Map<String, Object> config) {
		this(config, MessagePool.getQueue(config.containsKey("queue") ? (String)config.get("queue") : "FILE_STORE_QUEUE"));
	}

	public FileStorer(Map<String, Object> config, MemoryMessageQueue messageQueue) {
		if (config != null) {
			this.config = new ConfigMap<String, Object>(config);
		}
		this.messageQueue = messageQueue;
	}

	public void connect() throws StorerException {
		connect("FileStore");
	}

	public void connect(String name) throws StorerException {
		this.path = this.config.getString("path", "");
		this.fileTitle = this.config.getString("title", "message");
		this.fileExt = this.config.getString("ext", "log");
		this.encoding = this.config.getString("encoding", "utf8");

		this.dirPattern = new SimpleDateFormat(this.config.getString("dir", "yyyy-MM-dd"));
		this.filePattern = new SimpleDateFormat(this.config.getString("pattern", "yyyy-MM-dd-HH-mm"));

		this.cycle = this.config.getLong("cycle", 5000);

		if (messageQueue != null) {
			this.fileSaver = new FileSaver();
			this.fileSaver.start(name);
		} else {
			logger.error("FileStorer's MessageQueue is null");
		}
	}

	public void save(Message message) {
		if (this.messageQueue != null) {
			this.messageQueue.offer(message);
		}
	}

	public void close() throws StorerException {
		this.fileSaver.stop();
	}

	public String handleMessage(Message message) {
		if (message != null) {
			Object msgContent = message.getMessage();
			if (msgContent != null) {
				try {
					String msg = new String(msgContent.toString().getBytes(encoding), "utf-8");
					return msg.trim();
				} catch (UnsupportedEncodingException e) {
					logger.error(String.format("FileStorer Exception: exception=%s, encoding=%s", e.getMessage(), encoding));
				}
			}
		}
		return null;
	}

	private class FileSaver implements Processor {

		private boolean isStop = false; // 处理线程是否停止
		
		public FileSaver() throws StorerException {
			if (dirPattern != null) {
				File dir = new File(path);
				if (! dir.canWrite()) {
					throw new StorerException(String.format("Couldn't write directory %s", path));
				}
			}
			
		}

		public void run() {
			while (!isStop) {

				if (!messageQueue.isEmpty()) {
					try {
						// 生成文件保存路径
						String filePath = path;
						Date now = new Date();
						if (dirPattern != null) {
							filePath = path + "/" + dirPattern.format(now);
						}

						// 检查文件路径
						File path = new File(filePath);
						if (!path.exists()) {
							FileUtils.forceMkdir(path);
						}
						String filename = String.format("%s/%s_%s.%s", filePath, fileTitle, filePattern.format(now), fileExt);

						// 将消息写入文件
						OutputStreamWriter fileWriter = new OutputStreamWriter(new FileOutputStream(filename, true), "utf-8");
						while (!messageQueue.isEmpty()) {
							String message = handleMessage(messageQueue.poll());
							fileWriter.append(message + "\n");
							fileWriter.flush();
						}
						fileWriter.close();

					} catch (IOException e) {
						logger.error(String.format("FileStorer Exception: exception=%s", e.getMessage()));
					}
				}
				try {
					Thread.sleep(cycle);
				} catch (InterruptedException e) {
					logger.error(String.format("FileStorer Exception: exception=%s", e.getMessage()));
				}
			}
		}

		public void stop() {
			// 设置线程停止标志
			this.isStop = true; 
			logger.info("FileStorer STOP");
		}

		public void start(String name) {
			// 设置线程启动标志
			this.isStop = false; 
			// 文件存储线程启动
			ProcessorFactory.getThread(name, this).start(); 
		}
	}
}
