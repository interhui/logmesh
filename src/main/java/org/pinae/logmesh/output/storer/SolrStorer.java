package org.pinae.logmesh.output.storer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.message.MessagePool;
import org.pinae.logmesh.message.MemoryMessageQueue;
import org.pinae.logmesh.processor.Processor;
import org.pinae.logmesh.processor.ProcessorFactory;
import org.pinae.logmesh.util.ConfigMap;

/**
 * Solr存储
 * 
 * @author Huiyugeng
 * 
 */
public class SolrStorer implements Storer {
	private static Logger logger = Logger.getLogger(SolrStorer.class);

	/* Solr地址 */
	private String solrURL = "http://127.0.0.1:8983/solr/logmesh";
	/* Solr存储周期 */
	private long cycle;
	
	/* Solr存储线程 */
	private SolrPoster solrPoster;

	private ConfigMap<String, Object> config;
	private MemoryMessageQueue messageQueue;

	public SolrStorer(Map<String, Object> config) {
		this(config, MessagePool.getQueue(config.containsKey("queue") ? (String) config.get("queue") : "SOLR_STORE_QUEUE"));
	}

	public SolrStorer(Map<String, Object> config, MemoryMessageQueue messageQueue) {
		if (config != null) {
			this.config = new ConfigMap<String, Object>(config);
		}
		this.messageQueue = messageQueue;
	}

	public void connect() throws StorerException {
		connect("SolrPoster");
	}

	public void connect(String name) throws StorerException {
		this.solrURL = this.config.getString("url", "http://127.0.0.1:8983/solr");

		if (messageQueue != null) {
			this.solrPoster = new SolrPoster();
			this.solrPoster.start(name);
		} else {
			logger.error("SolrStore's MessageQueue is NULL");
		}
	}

	public void save(Message message) {
		if (messageQueue != null) {
			messageQueue.offer(message);
		}
	}

	public void close() throws StorerException {
		this.solrPoster.stop();
	}

	@SuppressWarnings("unchecked")
	public SolrInputDocument handleMessage(Message message) {

		if (message != null) {
			SolrInputDocument document = null;

			document = new SolrInputDocument();
			document.addField("id", Long.toString(message.getTimestamp()) + message.getIP()); // 使用时间戳和IP地址作为编号
			document.addField("ip", message.getIP());
			document.addField("type", message.getType());
			document.addField("owner", message.getOwner());
			document.addField("time", String.valueOf(message.getTimestamp()));
			
			Object msg = message.getMessage();
			if (msg instanceof Map) {
				Map<String, String> msgMap = (Map<String, String>)msg;
				Set<String> msgKeySet = msgMap.keySet();
				for (String msgKey : msgKeySet) {
					document.addField(msgKey, msgMap.get(msgKey));
				}
			} else {
				document.addField("message", msg.toString());
			}

			return document;
		}
		return null;

	}

	private class SolrPoster implements Processor {
		
		private HttpSolrClient solr;
		
		public SolrPoster() throws StorerException {
			try {
				this.solr = new HttpSolrClient(solrURL);
				SolrPingResponse ping = this.solr.ping();
				if (ping == null || 0 != ping.getStatus()) {
					throw new StorerException(String.format("Couldn't connect solr server %d", solrURL));
				}
			} catch (SolrServerException e) {
				logger.error(String.format("SolrStorer Exception: exception=%s", e.getMessage()));
				throw new StorerException(e);
			} catch (IOException e) {
				logger.error(String.format("SolrStorer Exception: exception=%s", e.getMessage()));
				throw new StorerException(e);
			}
		}
		
		/* 处理线程是否停止 */
		private boolean isStop = false;

		public void run() {
			while (!isStop) {
				try {
					if (!messageQueue.isEmpty()) {
						List<SolrInputDocument> docList = new ArrayList<SolrInputDocument>();
						while (!messageQueue.isEmpty()) {
							Message message = messageQueue.poll();
							SolrInputDocument doc = handleMessage(message);
							if (doc != null) {
								docList.add(doc);
							}
						}
						try {
							solr.add(docList);
							// 将消息提交到Solr
							UpdateResponse response = solr.commit();
							if (response.getStatus() == 0) {
								logger.debug(String.format("commit document %s succee. cost time is %sms", docList.toArray().toString(),
										response.getQTime()));
							} else {
								logger.debug(String.format("commit document %s failure. cost time is %sms", docList.toArray().toString(),
										response.getQTime()));
							}
						} catch (SolrServerException e) {
							logger.error(String.format("post Exception: exception=%s", e.getMessage()));
						} catch (IOException e) {
							logger.error(String.format("post Exception: exception=%s", e.getMessage()));
						}
					}

					Thread.sleep(cycle);
				} catch (InterruptedException e) {
					logger.error(String.format("SolrStorer Exception: exception=%s", e.getMessage()));
				}
			}
		}

		public void stop() {
			if (this.solr != null) {
				try {
					this.solr.close();
				} catch (IOException e) {
					logger.error(String.format("SolrStorer Exception: exception=%s", e.getMessage()));
				}
			}
			// 设置线程停止标志
			this.isStop = true;
			logger.info("Solr Store STOP");
		}

		public void start(String name) {
			// 设置线程启动标志
			this.isStop = false;
			// Solr存储线程启动
			ProcessorFactory.getThread(name, this).start();
		}

	}
}
