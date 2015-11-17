package org.pinae.logmesh.output.storer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.message.MessagePool;
import org.pinae.logmesh.message.MessageQueue;
import org.pinae.logmesh.processor.Processor;
import org.pinae.logmesh.processor.ProcessorFactory;

/**
 * 数据库存储
 * 
 * @author Huiyugeng
 * 
 * 
 */
public class DBStorer implements Storer {
	private static Logger log = Logger.getLogger(FileStorer.class);

	private String driver; // JDBC驱动
	private String url; // 数据库连接地址
	private String username; // 数据库登录用户名
	private String password; // 数据库登录密码

	private String sqlTemplate; // SQL脚本
	private int batchSize = 20; // 批量存储数量
	private long cycle; // 存储周期

	private DBSaver dbSaver = null; // 数据库存储线程

	private Map<String, Object> config;
	private MessageQueue messageQueue;

	public DBStorer(Map<String, Object> config) {
		this(config, MessagePool.getMessageQueue(config.containsKey("queue") ? (String)config.get("queue") : "DB_STORE_QUEUE"));
	}

	public DBStorer(Map<String, Object> config, MessageQueue messageQueue) {
		this.config = config;
		this.messageQueue = messageQueue;
	}

	public void connect() throws StorerException {
		connect("DBSaver");
	}

	public void connect(String name) throws StorerException {
		this.driver = config.containsKey("driver") ? (String)config.get("driver") : "";
		this.url = config.containsKey("url") ? (String)config.get("url") : "";
		this.username = config.containsKey("username") ? (String)config.get("username") : "";
		this.password = config.containsKey("password") ? (String)config.get("password") : "";
		this.sqlTemplate = config.containsKey("sql") ? (String)config.get("sql") : "";

		try {
			this.cycle = config.containsKey("cycle") ? Long.parseLong((String)config.get("cycle")) : 5000;
		} catch (NumberFormatException e) {
			this.cycle = 5000;
		}

		try {
			this.batchSize = config.containsKey("batchSize") ? Integer.parseInt((String)config.get("batchSize")) : 20;
		} catch (NumberFormatException e) {
			this.batchSize = 20;
		}

		if (messageQueue != null) {
			this.dbSaver = new DBSaver();
			this.dbSaver.start(name);
		} else {
			log.error("DBStore's MessageQueue is NULL");
		}
	}

	public void save(Message message) {
		messageQueue.add(message);
	}

	public void close() throws StorerException {
		this.dbSaver.stop();
	}

	@SuppressWarnings("unchecked")
	public String handleMessage(Message message) {
		Object msgContent = message.getMessage();
		try {
			if (msgContent != null) {
				if (msgContent instanceof String) {
					return (String) msgContent;
				} else if (msgContent instanceof Map) {
					Map<Object, Object> paramMap = (Map<Object, Object>) msgContent;
					Set<Entry<Object, Object>> entrySet = paramMap.entrySet();

					String sql = new String(sqlTemplate);
					for (Entry<Object, Object> entry : entrySet) {
						String key = ":" + entry.getKey().toString();
						String value = entry.getValue().toString();
						sql = sql.replaceAll(key, value);
					}
					return sql;
				} else {
					return null;
				}
			} else {
				return null;
			}
		} catch (Exception e) {
			return null;
		}
	}

	private class DBSaver implements Processor {

		private boolean isStop = false; // 处理线程是否停止

		private Connection conn;
		private Statement stm;

		public DBSaver() {
			try {
				Class.forName(driver);
				conn = DriverManager.getConnection(url, username, password);
				stm = conn.createStatement();
			} catch (Exception e) {
				log.error(String.format("DBSaver Exception: exception=%s", e.getMessage()));
			}
		}

		public void run() {
			while (!isStop) {
				try {
					if (!messageQueue.isEmpty()) {
						try {
							while (!messageQueue.isEmpty()) {

								int count = 0;

								if (count < batchSize) {
									Message message = messageQueue.poll();
									stm.addBatch(handleMessage(message));
									count++;
								} else {
									stm.executeBatch();
									count = 0;
								}
							}

							stm.executeBatch();

						} catch (Exception e) {
							log.error(String.format("DBSaver Exception: exception=%s", e.getMessage()));
						}
					}

					Thread.sleep(cycle);
				} catch (InterruptedException e) {
					log.error(String.format("DBSaver Exception: exception=%s", e.getMessage()));
				}
			}

			try {
				if (stm != null) {
					stm.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (SQLException e) {
				log.error(String.format("DBSaver Exception: exception=%s", e.getMessage()));
			}
		}

		public void stop() {
			this.isStop = true; // 设置线程停止标志

			log.info("DB Store STOP");
		}

		public void start(String name) {
			this.isStop = false; // 设置线程启动标志
			ProcessorFactory.getThread(name, this).start(); // 数据库存储线程启动
		}

	}
}
