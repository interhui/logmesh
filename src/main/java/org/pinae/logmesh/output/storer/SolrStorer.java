package org.pinae.logmesh.output.storer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.message.MessagePool;
import org.pinae.logmesh.message.MessageQueue;
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

	private String solrURL = "http://127.0.0.1:8983/solr/logmesh"; // Solr地址
	private long cycle; // 存储周期

	private SolrPoster solrPoster = new SolrPoster(); // Solr存储线程

	private ConfigMap<String, Object> config;
	private MessageQueue messageQueue;

	public SolrStorer(Map<String, Object> config) {
		this(config, MessagePool.getQueue(config.containsKey("queue") ? (String) config.get("queue") : "SOLR_STORE_QUEUE"));
	}

	public SolrStorer(Map<String, Object> config, MessageQueue messageQueue) {
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
			synchronized (messageQueue) {
				solrPoster.start(name);
			}
		} else {
			logger.error("SolrStore's MessageQueue is NULL");
		}
	}

	public void save(Message message) {
		if (messageQueue != null) {
			messageQueue.add(message);
		}
	}

	public void close() throws StorerException {
		solrPoster.stop();
	}

	public SolrInputDocument handleMessage(Message message) {

		if (message != null) {
			SolrInputDocument document = null;

			document = new SolrInputDocument();
			document.addField("id", Long.toString(message.getTimestamp()) + message.getIP()); // 使用时间戳和IP地址作为编号
			document.addField("ip", message.getIP());
			document.addField("type", message.getType());
			document.addField("message", message.getMessage());
			document.addField("owner", message.getOwner());
			document.addField("time", String.valueOf(message.getTimestamp()));

			return document;
		}
		return null;

	}

	private class SolrPoster implements Processor {

		private boolean isStop = false; // 处理线程是否停止

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

						SolrClient solr = null;
						try {
							solr = new HttpSolrClient(solrURL);
							solr.add(docList);
							UpdateResponse response = solr.commit();// 将消息发送到Solr
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
						} finally {
							if (solr != null) {
								try {
									solr.close();
								} catch (IOException e) {

								}
							}
						}
					}

					Thread.sleep(cycle);
				} catch (InterruptedException e) {
					logger.error(String.format("SolrStorer Exception: exception=%s", e.getMessage()));
				}
			}
		}

		public void stop() {
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
