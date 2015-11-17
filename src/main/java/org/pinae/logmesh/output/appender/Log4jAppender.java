package org.pinae.logmesh.output.appender;

import java.util.List;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.output.MessageOutputor;
import org.pinae.logmesh.processor.ProcessorPool;

/**
 * Log4j输出适配器
 * 
 * @author Huiyugeng
 * 
 */
public class Log4jAppender extends AppenderSkeleton {

	private List<MessageOutputor> outputors = ProcessorPool.OUTPUTOR_LIST;

	@Override
	protected void append(LoggingEvent event) {

		String message = this.layout.format(event);

		if (message != null && this.outputors != null) {
			for (MessageOutputor outputor : this.outputors) {
				if (outputor != null && this.outputors != null) {
					outputor.showMessage(new Message(message));
				}
			}
		}
	}

	public void close() {

	}

	public boolean requiresLayout() {
		return true;
	}

}
