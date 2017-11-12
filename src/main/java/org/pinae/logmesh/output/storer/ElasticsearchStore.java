package org.pinae.logmesh.output.storer;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.message.MessagePool;
import org.pinae.logmesh.message.MessageQueue;
import org.pinae.logmesh.processor.Processor;
import org.pinae.logmesh.processor.ProcessorFactory;
import org.pinae.logmesh.util.ConfigMap;

import com.google.gson.GsonBuilder;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.Index;

public class ElasticsearchStore implements Storer {

	private static Logger logger = Logger.getLogger(ElasticsearchStore.class);

	private JestClientFactory factory = new JestClientFactory();

	/* ES 存储线程 */
	private ElasticsearchIndexer indexer;
	/* ES 索引名称 */
	private String esIndex;
	/* ES 索引类型 */
	private String esType;

	/* ES 存储线程数量 */
	private int threadNum;
	/* ES 存储周期 */
	private long cycle;
	/* ES 获取数量 */
	private long batchSize;

	private ConfigMap<String, Object> config;
	private MessageQueue messageQueue;

	public ElasticsearchStore(Map<String, Object> config) {
		this(config, MessagePool.getQueue(config.containsKey("queue") ? (String) config.get("queue") : "ES_STORE_QUEUE"));
	}

	public ElasticsearchStore(Map<String, Object> config, MessageQueue messageQueue) {
		if (config != null) {
			this.config = new ConfigMap<String, Object>(config);
		}
		this.messageQueue = messageQueue;
	}

	public void connect() throws StorerException {
		connect("ElasticsearchIndexer");
	}

	public void connect(String name) throws StorerException {
		
		this.esIndex = this.config.getString("esindex", "logmesh");
		this.esType = this.config.getString("estype", "logmesh");
		this.threadNum = this.config.getInt("thread", 1);
		this.cycle = this.config.getLong("cycle", 10);
		this.batchSize = this.config.getLong("batch", 500);
		
		String esUri = this.config.getString("url", "http://127.0.0.1:9200");
		HttpClientConfig clientConfig = new HttpClientConfig.Builder(esUri).gson(new GsonBuilder().setDateFormat("yyyy-MM-dd'T'hh:mm:ss").create())
				.connTimeout(1500).readTimeout(3000).multiThreaded(true).build();
		
		this.factory.setHttpClientConfig(clientConfig);
		
		if (messageQueue != null) {
			for (int i = 0; i < threadNum; i++) { 
				this.indexer = new ElasticsearchIndexer();
				this.indexer.start(name + "-" + Integer.toString(i));
			}
		} else {
			logger.error("ElasticsearchIndexer's MessageQueue is NULL");
		}
	}

	public void save(Message message) {
		if (messageQueue != null) {
			messageQueue.offer(message);
		}
	}

	public void close() throws StorerException {
		
	}

	public Map<String, Object> handleMessage(Message message) {
		if (message!= null) {
			Map<String, Object> doc = new HashMap<String, Object>();
	
			doc.put("ip", message.getIP());
			doc.put("type", message.getType());
			doc.put("owner", message.getOwner());
			doc.put("time", String.valueOf(message.getTimestamp()));
			doc.put("message", message.getMessage());
	
			return doc;
		}
		return null;
	}

	private class ElasticsearchIndexer implements Processor {

		private JestClient client;

		public ElasticsearchIndexer() throws StorerException {
			this.client = factory.getObject();
		}

		/* 处理线程是否停止 */
		private boolean isStop = false;

		public void run() {
			while (!isStop) {
				try {
					if (!messageQueue.isEmpty()) {
						
						String indexName = new StringBuffer().append(esIndex).append("-").append(new SimpleDateFormat("yyyy-MM-dd").format(new Date()))
								.toString();
						
						Bulk.Builder bulk = new Bulk.Builder().defaultIndex(indexName).defaultType(esType);  
				        
						int counter = 0;
						while (!messageQueue.isEmpty()) {
							if (counter < batchSize) {
								counter++;
								
								Message message = messageQueue.poll();
								Map<String, Object> doc = handleMessage(message);
			
								if (doc != null) {
									Index index = new Index.Builder(doc).build();
									bulk.addAction(index);
								}
							} else {
								break;
							}
							
						}

						BulkResult result = null;
						try {
							result = client.execute(bulk.build());
							if (result == null || !result.isSucceeded()) {
								logger.error(String.format("ElasticsearchIndexer Fail: exception=%s", result.getErrorMessage()));
							}
						} catch (IOException e) {
							logger.error(String.format("ElasticsearchIndexer Exception: exception=%s", e.getMessage()));
						}
						
					}
					Thread.sleep(cycle);
				} catch (InterruptedException e) {
					logger.error(String.format("ElasticsearchIndexer Exception: exception=%s", e.getMessage()));
				}
			}
		}

		public void stop() {
			if (this.client != null) {
				this.client.shutdownClient();
			}
			// 设置线程停止标志
			this.isStop = true;
			logger.info("Elasticsearch Store STOP");
		}

		public void start(String name) {
			// 设置线程启动标志
			this.isStop = false;
			// 存储线程启动
			ProcessorFactory.getThread(name, this).start();
		}

	}

}
