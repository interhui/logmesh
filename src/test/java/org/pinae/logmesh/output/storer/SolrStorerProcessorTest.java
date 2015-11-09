package org.pinae.logmesh.output.storer;

import org.pinae.logmesh.component.MessageProcessor;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.output.storer.SolrStorer;
import org.pinae.logmesh.output.storer.Storer;
import org.pinae.logmesh.output.storer.StorerException;
import org.pinae.logmesh.processor.ProcessorInfo;

public class SolrStorerProcessorTest extends ProcessorInfo implements MessageProcessor {

	private Storer solrStore;

	public void init() {
		solrStore = new SolrStorer(super.getParameters());
		try {
			solrStore.connect();
		} catch (StorerException e) {
			e.printStackTrace();
		}
	}

	public void porcess(Message message) {
		solrStore.save(message);
	}

}
