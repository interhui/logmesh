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
import org.pinae.logmesh.message.MessageQueue;
import org.pinae.logmesh.processor.Processor;
import org.pinae.logmesh.processor.ProcessorFactory;

/**
 * 文件存储
 * 
 * @author Huiyugeng
 * 
 * 
 */
public class FileStorer implements Storer {
	private static Logger log = Logger.getLogger(FileStorer.class);

	private String path; // 文件路径
	private String fileTitle; // 文件标题
	private String fileExt; // 文件扩展名
	private String encoding; // 消息编码
	private long cycle; // 写入周期

	private SimpleDateFormat dirPattern; // 通过日期构建的目录格式
	private SimpleDateFormat filePattern; // 通过日期构建的文件格式

	private FileCreator fileCreator = new FileCreator(); // 文件存储线程

	private Map<String, Object> config;
	private MessageQueue messageQueue;

	public FileStorer(Map<String, Object> config) {
		this(config, MessagePool.getMessageQueue(config.containsKey("queue") ? (String)config.get("queue") : "FILE_STORE_QUEUE"));
	}

	public FileStorer(Map<String, Object> config, MessageQueue messageQueue) {
		this.config = config;
		this.messageQueue = messageQueue;
	}

	public void connect() throws StorerException {
		connect("FileStore");
	}

	public void connect(String name) throws StorerException {
		this.path = config.containsKey("path") ? (String)config.get("path") : "";
		this.fileTitle = config.containsKey("title") ? (String)config.get("title") : "message";
		this.fileExt = config.containsKey("ext") ? (String)config.get("ext") : "log";
		this.encoding = config.containsKey("encoding") ? (String)config.get("encoding") : "utf8";

		if (config.containsKey("dir")) {
			this.dirPattern = new SimpleDateFormat((String)config.get("dir"));
		}
		this.filePattern = new SimpleDateFormat(config.containsKey("pattern") ? (String)config.get("pattern") : "yyyy-MM-dd-hh");

		this.cycle = config.containsKey("cycle") ? Integer.parseInt((String)config.get("cycle")) : 5000;

		if (messageQueue != null) {
			synchronized (messageQueue) {
				this.fileCreator.start(name);
			}
		} else {
			log.error("FileStore's MessageQueue is NULL");
		}
	}

	public void save(Message message) {
		if (messageQueue != null) {
			messageQueue.add(message);
		}
	}

	public void close() throws StorerException {
		fileCreator.stop();
	}

	public String handleMessage(Message message) {
		if (message != null) {
			Object msgContent = message.getMessage();
			if (msgContent != null) {
				try {
					String msg = new String(msgContent.toString().getBytes(encoding), "utf-8");
					return msg.trim();
				} catch (UnsupportedEncodingException e) {
					log.error(String.format("FileStorer Exception: exception=%s, encoding=%s", e.getMessage(), encoding));
				}
			}
		}
		return null;
	}

	private class FileCreator implements Processor {

		private boolean isStop = false; // 处理线程是否停止

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
						log.error(String.format("FileStorer Exception: exception=%s", e.getMessage()));
					}
				}
				try {
					Thread.sleep(cycle);
				} catch (InterruptedException e) {
					log.error(String.format("FileStorer Exception: exception=%s", e.getMessage()));
				}
			}
		}

		public void stop() {
			this.isStop = true; // 设置线程停止标志

			log.info("File Store STOP");
		}

		public void start(String name) {
			this.isStop = false; // 设置线程启动标志
			ProcessorFactory.getThread(name, this).start(); // 文件存储线程启动
		}
	}
}
