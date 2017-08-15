package org.pinae.logmesh.message;

import static org.junit.Assert.assertEquals;

import org.apache.log4j.Logger;
import org.junit.Test;

public class MessageQueueTest {
	private static Logger logger = Logger.getLogger(MessageQueueTest.class);
	
	@Test
	public void testQueue() {
		int testSize = 100000;
		int maxSize = 5;
		
		MemoryMessageQueue queue = new MemoryMessageQueue("Test", maxSize);
		long start = System.currentTimeMillis();
		for (int i = 0 ; i < testSize ; i++) {
			queue.offer(new Message(Integer.toString(i)));
		}
		long end = System.currentTimeMillis();
		logger.info("Time Used:" + Long.toString(end - start));
		assertEquals(queue.count(), testSize);
		assertEquals(queue.size(), maxSize);
		
		assertEquals(queue.poll().getMessage(), Integer.toString(testSize - maxSize));
	}

}
