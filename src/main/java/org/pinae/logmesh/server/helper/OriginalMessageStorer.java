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
import org.pinae.logmesh.output.storer.StorerException;
import org.pinae.logmesh.output.storer.TextFileStorer;
import org.pinae.logmesh.processor.Processor;
import org.pinae.logmesh.processor.ProcessorFactory;
import org.pinae.logmesh.util.ConfigMap;
import org.pinae.logmesh.util.ZipUtils;

/**
 * 原始消息存储器
 * 
 * @author Huiyugeng
 * 
 */
public class OriginalMessageStorer {
	private static Logger logger = Logger.getLogger(OriginalMessageStorer.class);

	/* 是否激活原始消息存储 */
	private boolean enable = true;

	/* 记录的原始消息样式 */
	private String msgPattern;
	
	/* 原始消息样式中包含日期信息 */
	private boolean isRecordTime = false;
	
	/* 原始消息样式中包含IP信息 */
	private boolean isRecordIP = false;
	
	/* 原始消息样式中包含消息体 */
	private boolean isRecordMsg = false;

	/* 是否进行消息压缩 */
	private boolean isZip = false;

	/* 压缩路径 */
	private String zipDirPath;
	
	/* 压缩文件夹命名模式 */
	private String zipDirPattern;

	private ConfigMap<String, Object> config;

	public OriginalMessageStorer(Map<String, Object> config) {
		if (config != null) {
			this.config = new ConfigMap<String, Object>(config);
		}
	}

	public void start() {

		if (config != null && config.getBoolean("enable", true)) {
			this.msgPattern = config.getString("msgPattern", "$time : $ip : $message");

			if (msgPattern.contains("$time")) {
				this.isRecordTime = true;
			}

			if (msgPattern.contains("$ip")) {
				this.isRecordIP = true;
			}

			if (msgPattern.contains("$message")) {
				this.isRecordMsg = true;
			}

			if (config.equalsIgnoreCase("zip", "true")) {
				this.isZip = true;
			}

			this.zipDirPath = config.getString("path", "");
			this.zipDirPattern = config.getString("dir", "yyyy-MM-dd");
			
			MessageStore messageStore = new MessageStore(config, MessagePool.ORIGINAL_QUEUE);
			try {
				messageStore.connect("OriginalMessageStore");
			} catch (StorerException e) {
				logger.error(String.format("OriginalMessageStore Connect Fail: exception=%s", e.getMessage()));
			}

			if (this.isZip) {
				new MessageCompress().start("MessageCompress");
			}
		} else {
			logger.info("Original Message Disable");
		}

	}

	/*
	 * 消息存储器
	 */
	private class MessageStore extends TextFileStorer {
		private SimpleDateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd hh:MM:ss");

		private String encoding = "utf8";

		public MessageStore(ConfigMap<String, Object> config, MessageQueue messageQueue) {
			super(config, messageQueue);
			if (config != null) {
				this.encoding = config.getString("encoding", "utf8");
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
						logger.error(String.format("OriginalMessageStore Exception: exception=%s, encoding=%s", e.getMessage(), this.encoding));
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
			dirFormat = new SimpleDateFormat(zipDirPattern);
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
							String zipFile = String.format("%s\\%s", zipDirPath, lastZipDir);
							// 压缩目录
							ZipUtils.zip(zipFile + ".zip", zipFile);
							// 删除目录
							FileUtils.deleteDirectory(new File(zipFile));
						} catch (IOException e) {
							logger.error(String.format("IO Exception: exception=%s", e.getMessage()));
						}
						lastZipDir = zipDir;
					}
				}

				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					logger.error(String.format("Compress Exception: exception=%s", e.getMessage()));
				}
			}

		}

		public void stop() {
			this.isStop = true;
			logger.info("Original Message Store STOP");
		}

		public void start(String name) {
			// 设置线程启动标志
			this.isStop = false; 
			// 启动原始消息压缩线程
			ProcessorFactory.getThread(name, this).start(); 
		}

	}

}
