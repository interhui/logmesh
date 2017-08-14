package org.pinae.logmesh.output.storer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.message.MessagePool;
import org.pinae.logmesh.message.MessageQueue;
import org.pinae.logmesh.processor.Processor;
import org.pinae.logmesh.processor.ProcessorFactory;
import org.pinae.logmesh.util.ConfigMap;

public class ElasticsearchStore implements Storer {

	private static Logger logger = Logger.getLogger(ElasticsearchStore.class);

	private ElasticsearchIndexer esIndexer;
	
	/* ES 集群地址 */
	private TransportAddress esAddress[];
	
	/* ES 集群名称 */
	private String esCluster;
	/* ES 索引名称 */
	private String esIndex;
	/* ES 索引类型 */
	private String esType;
	
	/* ES 存储周期 */
	private long cycle;

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
		try {
			String esAddress = this.config.getString("address", "127.0.0.1:9300");
			String esAddresses[] = esAddress.split(";");
			if (esAddresses != null) {
				this.esAddress = new TransportAddress[esAddresses.length];
				for (int i = 0; i < esAddresses.length ; i++){
					String esAddr = esAddresses[i];
					String _addr[] = esAddr.split(":");
					if (_addr != null && _addr.length == 2 && StringUtils.isNumeric(_addr[1])) {
						this.esAddress[i] = new InetSocketTransportAddress(InetAddress.getByName(_addr[0]), Integer.parseInt(_addr[1]));
					}
				}
			}
			
		} catch (UnknownHostException e) {
			throw new StorerException(e);
		}
		

		this.esCluster = this.config.getString("cluster", "logmesh");
		this.esIndex = this.config.getString("index", "logmesh");
		this.esType = this.config.getString("type", "log");

		if (messageQueue != null) {
			this.esIndexer = new ElasticsearchIndexer();
			this.esIndexer.start(name);
		} else {
			logger.error("ElasticsearchStore's MessageQueue is NULL");
		}
	}

	public void save(Message message) {
		if (messageQueue != null) {
			messageQueue.offer(message);
		}
	}

	public void close() throws StorerException {
		this.esIndexer.stop();
	}

	public Map<String, Object> handleMessage(Message message) {
		Map<String, Object> doc = new HashMap<String, Object>();
		
		doc.put("ip", message.getIP());
		doc.put("type", message.getType());
		doc.put("owner", message.getOwner());
		doc.put("time", String.valueOf(message.getTimestamp()));
		doc.put("message", message.getMessage());
		
		return doc;
	}

	private class ElasticsearchIndexer implements Processor {

		private TransportClient client;

		@SuppressWarnings({ "unchecked", "resource" })
		public ElasticsearchIndexer() throws StorerException {
			Settings settings = Settings.builder().put("cluster.name", esCluster).build();
			this.client = new PreBuiltTransportClient(settings).addTransportAddresses(esAddress);
		}

		/* 处理线程是否停止 */
		private boolean isStop = false;

		public void run() {
			while (!isStop) {
				try {
					if (!messageQueue.isEmpty()) {

						BulkRequestBuilder bulkRequest = this.client.prepareBulk();

						while (!messageQueue.isEmpty()) {
							Message message = messageQueue.poll();
							
							Map<String, Object> doc = handleMessage(message);
							if (doc != null) {
								esIndex = new StringBuffer().append(esIndex).append("-").append(new SimpleDateFormat("yyyy-MM-dd").format(new Date())).toString();
								bulkRequest.add(this.client.prepareIndex(esIndex, esType).setSource());
							}
						}

						BulkResponse response = bulkRequest.execute().actionGet();
						if (response == null || response.hasFailures()) {
							logger.error(String.format("ElasticsearchStorer Fail: exception=%s", response.buildFailureMessage()));
						}
					}

					Thread.sleep(cycle);
				} catch (InterruptedException e) {
					logger.error(String.format("ElasticsearchStorer Exception: exception=%s", e.getMessage()));
				}
			}
		}

		public void stop() {
			if (this.client != null) {
				this.client.close();
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
