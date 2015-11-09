package org.pinae.logmesh.output.storer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.pinae.logmesh.component.MessageProcessor;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.output.storer.DBStorer;
import org.pinae.logmesh.output.storer.Storer;
import org.pinae.logmesh.output.storer.StorerException;
import org.pinae.logmesh.processor.ProcessorInfo;

public class DBStorerProcessorTest extends ProcessorInfo implements MessageProcessor {

	private Storer dbStore;
	private static SimpleDateFormat time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static SimpleDateFormat date = new SimpleDateFormat("yyyyMM");

	public void init() {
		dbStore = new DBStorer(super.getParameters());
		try {
			dbStore.connect();
		} catch (StorerException e) {
			e.printStackTrace();
		}
	}

	public void porcess(Message message) {

		Map<String, String> msg = new HashMap<String, String>();

		Date now = new Date(message.getTimestamp());
		String msgContent = message.getMessage().toString().trim();
		msgContent = msgContent.replaceAll("'", "''");
		String ip = message.getIP();

		msg.put("date", date.format(now));
		msg.put("time", time.format(now));
		msg.put("ip", ip);
		msg.put("message", msgContent);

		message.setMessage(msg);

		dbStore.save(message);
	}

}
