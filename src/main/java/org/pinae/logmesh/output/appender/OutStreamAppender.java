package org.pinae.logmesh.output.appender;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.List;

import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.output.MessageOutputor;
import org.pinae.logmesh.processor.ProcessorPool;

/**
 * 输出流适配器
 * 
 * @author Huiyugeng
 * 
 */
public class OutStreamAppender extends PrintStream {

	private List<MessageOutputor> outputors = ProcessorPool.OUTPUTOR_LIST;

	public OutStreamAppender(OutputStream out) {
		super(out);
	}

	public void write(byte[] buf, int off, int len) {
		String message = new String(buf, off, len);

		if (message != null && outputors != null) {
			for (MessageOutputor outputor : outputors) {
				outputor.showMessage(new Message(message));
			}
		}
	}
}
