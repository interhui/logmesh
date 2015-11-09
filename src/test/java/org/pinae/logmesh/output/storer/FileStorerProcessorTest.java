package org.pinae.logmesh.output.storer;

import org.pinae.logmesh.component.MessageProcessor;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.output.storer.FileStorer;
import org.pinae.logmesh.output.storer.Storer;
import org.pinae.logmesh.output.storer.StorerException;
import org.pinae.logmesh.processor.ProcessorInfo;

public class FileStorerProcessorTest extends ProcessorInfo implements MessageProcessor {

	private static Storer fileStore;

	public void init() {
		if (fileStore == null) {
			fileStore = new FileStorer(super.getParameters());
			try {
				fileStore.connect();
			} catch (StorerException e) {
				e.printStackTrace();
			}
		}

	}

	public void porcess(Message message) {
		fileStore.save(message);
	}

}
