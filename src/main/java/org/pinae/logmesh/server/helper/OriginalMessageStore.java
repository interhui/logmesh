package org.pinae.logmesh.server.helper;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.message.MessagePool;
import org.pinae.logmesh.message.MessageQueue;
import org.pinae.logmesh.output.storer.FileStorer;
import org.pinae.logmesh.output.storer.StorerException;
import org.pinae.logmesh.processor.Processor;
import org.pinae.logmesh.processor.ProcessorFactory;
import org.pinae.logmesh.util.ZipUtils;

/**
 * 原始消息存储器
 * 
 * @author Huiyugeng
 * 
 */
public class OriginalMessageStore {
	private static Logger log = Logger.getLogger(OriginalMessageStore.class);

	private boolean enable = true; // 是否激活原始消息存储

	private String msgPattern; // 记录的原始消息样式
	private boolean isRecordTime = false; // 原始消息样式中包含日期信息
	private boolean isRecordIP = false; // 原始消息样式中包含IP信息
	private boolean isRecordMsg = false; // 原始消息样式中包含消息体

	private boolean isZip = false; // 是否进行消息压缩

	private String path; // 压缩路径
	private String dirPattern; // 压缩文件夹命名模式

	private Map<String, Object> config;

	public OriginalMessageStore(Map<String, Object> config) {
		this.config = config;
	}

	public void start() {

		if (config != null) {
			this.msgPattern = config.containsKey("msgPattern") ? (String)config.get("msgPattern") : "$time : $ip : $message";

			if (msgPattern.contains("$time")) {
				this.isRecordTime = true;
			}

			if (msgPattern.contains("$ip")) {
				this.isRecordIP = true;
			}

			if (msgPattern.contains("$message")) {
				this.isRecordMsg = true;
			}

			if (config.containsKey("zip") && ((String)config.get("zip")).equalsIgnoreCase("true")) {
				this.isZip = true;
			}

			this.path = config.containsKey("path") ? (String)config.get("path") : "";
			this.dirPattern = config.containsKey("dir") ? (String)config.get("dir") : "yyyy-MM-dd";
		}

		MessageStore messageStore = new MessageStore(config, MessagePool.ORIGINAL_QUEUE);
		try {
			messageStore.connect("OriginalMessageStore");
		} catch (StorerException e) {
			log.error(String.format("OriginalMessageStore Connect Fail: exception=%s", e.getMessage()));
		}

		if (this.isZip) {
			new MessageCompress().start("MessageCompress");
		}
	}

	/*
	 * 消息存储器
	 */
	private class MessageStore extends FileStorer {
		private SimpleDateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd hh:MM:ss");

		private String encoding = "utf8";

		public MessageStore(Map<String, Object> config, MessageQueue messageQueue) {
			super(config, messageQueue);
			if (config != null) {
				this.encoding = config.containsKey("encoding") ? (String) config.get("encoding") : "utf8";
			}
		}

		public String handleMessage(Message message) {
			if (!enable) {
				return null;
			}

			if (message != null) {
				Object msgContent = message.getMessage();
				if (msgContent instanceof byte[]) {
					try {
						msgContent = new String((byte[]) msgContent, this.encoding);
					} catch (UnsupportedEncodingException e) {
						msgContent = msgContent.toString();
					}
				}
				if (msgContent != null) {
					try {
						String formattedMsg = msgPattern;

						if (isRecordTime) {
							formattedMsg = msgPattern.replaceAll("\\$time", timeFormat.format(new Date(message.getTimestamp())));
						}
						if (isRecordIP) {
							formattedMsg = formattedMsg.replaceAll("\\$ip", message.getIP());
						}
						if (isRecordMsg) {
							formattedMsg = formattedMsg.replaceAll("\\$message", msgContent.toString());
						}
						return formattedMsg;

					} catch (Exception e) {
						log.error(String.format("OriginalMessageStore Exception: exception=%s, encoding=%s", e.getMessage(), this.encoding));
					}
				}
			}
			return null;
		}
	}

	private class MessageCompress implements Processor {
		private SimpleDateFormat dirFormat;

		private String lastZipDir = "";

		public MessageCompress() {
			dirFormat = new SimpleDateFormat(dirPattern);
		}

		private boolean isStop = false; // 处理线程是否停止

		public void run() {

			while (!isStop) {
				Date now = new Date();
				String zipDir = dirFormat.format(now);
				if (StringUtils.isEmpty(lastZipDir)) {
					lastZipDir = zipDir;
				} else {
					if (!zipDir.equals(lastZipDir) && isZip) {
						try {
							String zipFile = String.format("%s\\%s", path, lastZipDir);
							// 压缩目录
							ZipUtils.zip(zipFile + ".zip", zipFile);
							// 删除目录
							FileUtils.deleteDirectory(new File(zipFile));
						} catch (IOException e) {
							log.error(String.format("IO Exception: exception=%s", e.getMessage()));
						}
						lastZipDir = zipDir;
					}
				}

				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					log.error(String.format("Compress Exception: exception=%s", e.getMessage()));
				}
			}

		}

		public void stop() {
			this.isStop = true;

			log.info("Original Message Store STOP");
		}

		public void start(String name) {
			this.isStop = false; // 设置线程启动标志

			ProcessorFactory.getThread(name, this).start(); // 启动原始消息压缩线程
		}

	}

}
