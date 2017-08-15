package org.pinae.logmesh.output.storer;

import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.context.Context;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.message.MessagePool;
import org.pinae.logmesh.message.MemoryMessageQueue;
import org.pinae.logmesh.processor.Processor;
import org.pinae.logmesh.processor.ProcessorFactory;
import org.pinae.logmesh.util.ConfigMap;

/**
 * 数据库存储
 * 
 * @author Huiyugeng
 * 
 * 
 */
public class DatabaseStorer implements Storer {
	private static Logger logger = Logger.getLogger(DatabaseStorer.class);

	// JDBC驱动
	private String driver;
	// 数据库连接地址
	private String url;
	// 数据库登录用户名
	private String username;
	// 数据库登录密码
	private String password;

	// SQL脚本
	private String sqlTemplate;
	// 批量存储数量
	private int batchSize = 100;
	// 存储周期
	private long cycle;

	// 数据库存储线程
	private DBSaver dbLogSaver = null;

	private ConfigMap<String, Object> config;
	private MemoryMessageQueue messageQueue;

	private String defaultSql = "insert into event(time, ip, owner, message) values ('${time}', '${ip}', '${owner}', '${message}')";

	public DatabaseStorer(Map<String, Object> config) {
		this(config, MessagePool.getQueue(config.containsKey("queue") ? (String) config.get("queue") : "DB_STORE_QUEUE"));
	}

	public DatabaseStorer(Map<String, Object> config, MemoryMessageQueue messageQueue) {
		if (config != null) {
			this.config = new ConfigMap<String, Object>(config);
		}
		this.messageQueue = messageQueue;
	}

	public void connect() throws StorerException {
		connect("DBStorer");
	}

	public void connect(String name) throws StorerException {
		this.driver = this.config.getString("driver", "com.mysql.jdbc.Driver");
		this.url = this.config.getString("url", "cjdbc:mysql://localhost:3306/log");
		this.username = this.config.getString("username", "log");
		this.password = this.config.getString("password", "log");

		this.sqlTemplate = this.config.getString("sql", defaultSql);

		this.cycle = this.config.getLong("cycle", 5000);
		this.batchSize = this.config.getInt("batchSize", 100);

		if (messageQueue != null) {
			this.dbLogSaver = new DBSaver();
			this.dbLogSaver.start(name);
		} else {
			logger.error("DBStorer's MessageQueue is null");
		}
	}

	public void save(Message message) {
		messageQueue.offer(message);
	}

	public void close() throws StorerException {
		this.dbLogSaver.stop();
	}

	@SuppressWarnings("unchecked")
	public String handleMessage(Message message) {
		Object msgContent = message.getMessage();
		try {
			if (msgContent != null) {
				if (msgContent instanceof String) {
					return (String) msgContent;
				} else if (msgContent instanceof Map) {
					Context context = new VelocityContext();
					StringWriter sw = new StringWriter();

					Map<Object, Object> msgMap = (Map<Object, Object>) msgContent;
					Set<Object> msgKeySet = msgMap.keySet();
					for (Object msgKey : msgKeySet) {
						Object msgValue = msgMap.get(msgKey);
						context.put(msgKey.toString(), msgValue.toString());
					}
					Velocity.evaluate(context, sw, "log", sqlTemplate);
					return sw.toString();
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
		// 处理线程是否停止
		private boolean isStop = false;

		private Connection conn;
		private Statement stm;

		public DBSaver() throws StorerException {
			try {
				Class.forName(driver);
				this.conn = DriverManager.getConnection(url, username, password);
				this.stm = conn.createStatement();
			} catch (Exception e) {
				logger.error(String.format("DBStorer Exception: exception=%s", e.getMessage()));
				throw new StorerException(e);
			}
		}

		public void run() {
			while (!this.isStop) {
				try {
					if (!messageQueue.isEmpty()) {
						try {
							while (!messageQueue.isEmpty()) {
								int count = 0;
								if (count < batchSize) {
									Message message = messageQueue.poll();
									this.stm.addBatch(handleMessage(message));
									count++;
								} else {
									this.stm.executeBatch();
									count = 0;
								}
							}
							this.stm.executeBatch();
						} catch (Exception e) {
							logger.error(String.format("DBSaver Exception: exception=%s", e.getMessage()));
						}
					}

					Thread.sleep(cycle);
				} catch (InterruptedException e) {
					logger.error(String.format("DBStorer Exception: exception=%s", e.getMessage()));
				}
			}

			try {
				if (this.stm != null) {
					this.stm.close();
				}
				if (this.conn != null) {
					this.conn.close();
				}
			} catch (SQLException e) {
				logger.error(String.format("DBStorer Exception: exception=%s", e.getMessage()));
			}
		}

		public void stop() {
			// 设置线程停止标志
			this.isStop = true;
			logger.info("DBStore STOP");
		}

		public void start(String name) {
			// 设置线程启动标志
			this.isStop = false;
			// 数据库存储线程启动
			ProcessorFactory.getThread(name, this).start();
		}

	}
}
