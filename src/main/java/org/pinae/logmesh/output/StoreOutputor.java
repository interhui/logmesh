package org.pinae.logmesh.output;

import org.apache.log4j.Logger;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.output.storer.DBStorer;
import org.pinae.logmesh.output.storer.FileStorer;
import org.pinae.logmesh.output.storer.SolrStorer;
import org.pinae.logmesh.output.storer.Storer;
import org.pinae.logmesh.output.storer.StorerException;
import org.pinae.logmesh.processor.ProcessorInfo;

/**
 * 日志存储输出
 * 
 * @author huiyugeng
 * 
 */
public class StoreOutputor extends ProcessorInfo implements MessageOutputor {

	private static Logger log = Logger.getLogger(ForwardOutputor.class);

	private Storer store;

	public void init() {

		String type = getStringValue("type", "file");

		try {
			if (type.equalsIgnoreCase("file")) {
				store = new FileStorer(getParameters());
			} else if (type.equalsIgnoreCase("db")) {
				store = new DBStorer(getParameters());
			} else if (type.equalsIgnoreCase("solr")) {
				store = new SolrStorer(getParameters());
			}

			if (store != null) {
				store.connect(); // 存储器连接
			}
		} catch (StorerException e) {
			log.error(String.format("StoreOutputor Exception: exception=%s", e.getMessage()));
		}
	}

	public void showMessage(Message message) {
		if (store != null && message != null) {
			store.save(message);
		}

	}

}
