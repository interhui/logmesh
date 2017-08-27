package org.pinae.logmesh.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import sun.nio.ch.FileChannelImpl; 

public class FileReader<T> implements Iterable<List<T>> {

	private static final byte LF = 10;

	private static final long CHUNK_SIZE = 4096;
	
	private File file;
	
	private long chunkPos = 0;
	private MappedByteBuffer buffer;
	private FileChannel channel;
	
	private String codec = "utf8";
	
	private FileReader(File file, long chunkPos, String codec) {
		this.file = file;
		this.chunkPos = chunkPos;
		this.codec = codec;
	}

	private FileReader(File file, long chunkPos) {
		this.file = file;
		this.chunkPos = chunkPos;
	}
	
	public static <T> FileReader<T> create(File file) {
		return new FileReader<T>(file, 0);
	}
	
	public static <T> FileReader<T> create(File file, long chunkPos) {
		return new FileReader<T>(file, chunkPos);
	}
	
	public static <T> FileReader<T> create(File file, long chunkPos, String codec) {
		return new FileReader<T>(file, chunkPos, codec);
	}

	public Iterator<List<T>> iterator() {
		return new Iterator<List<T>>() {
			
			private List<T> entries;

			public boolean hasNext() {
				if (buffer == null || !buffer.hasRemaining()) {
					buffer = nextBuffer(chunkPos);
					if (buffer == null) {
						return false;
					}
				}
				T result = null;
				while ((result = (T) parseLine(buffer)) != null) {
					if (entries == null) {
						entries = new ArrayList<T>();
					}
					entries.add(result);
				}
				// set next MappedByteBuffer chunk
				chunkPos += buffer.position();
				if (entries != null) {
					return true;
				} else {
					return false;
				}
			}

			private MappedByteBuffer nextBuffer(long position) {
				
				long chunkSize = CHUNK_SIZE;
				try {
					if (channel == null) {
						channel = new RandomAccessFile(file, "r").getChannel();
					}
					long fileSize = channel.size();
					if (fileSize < position) {
						position = fileSize;
						chunkPos = fileSize;
					}
					if (fileSize - position < chunkSize) {
						chunkSize = fileSize - position;
					}
					return channel.map(FileChannel.MapMode.READ_ONLY, chunkPos, chunkSize);
				} catch (IOException e) {
//					chunkPos = 0;
//					try {
//						if (channel.size() - position < chunkSize) {
//							chunkSize = channel.size() - position;
//						}
//						return channel.map(FileChannel.MapMode.READ_ONLY, 0, chunkSize);
//					} catch (IOException e1) {
//						close();
//						throw new RuntimeException(e1);
//					}
					e.printStackTrace();
					throw new RuntimeException(e);
				}
			}

			public List<T> next() {
				List<T> res = entries;
				entries = null;
				return res;
			}

			public void remove() {
				throw new UnsupportedOperationException();
			}

			public String parseLine(ByteBuffer buffer) {

				int lineStartPos = buffer.position();
				int limit = buffer.limit();
				while (buffer.hasRemaining()) {
					byte b = buffer.get();
					if (b == LF) { // reached line feed so parse line
						int lineEndPos = buffer.position();
						// set positions for one row duplication
						if (buffer.limit() < lineEndPos + 1) {
							buffer.position(lineStartPos).limit(lineEndPos);
						} else {
							buffer.position(lineStartPos).limit(lineEndPos + 1);
						}
						
						byte[] row = new byte[lineEndPos - lineStartPos - 1];
						for (int i = 0; i < row.length; i++) {
							row[i] = buffer.get();
						}
						
						if (row != null) {
							// reset main buffer
							buffer.position(lineEndPos);
							buffer.limit(limit);
							// set start after LF
							lineStartPos = lineEndPos;
						}
						
						String line = null;
						try {
							line = new String(row, codec);
						} catch (UnsupportedEncodingException e) {
							
						}
						return line;
					}
				}
				buffer.position(lineStartPos);
				return null;
			}

		};

	}
	
	public long getPosition() {
		return chunkPos;
	}
	
	public void close() {
		try {
			closeBuffer();
			closeChannel();
		} catch (Exception e) {
			
		}
	}
	
	private void closeChannel() throws Exception {
		if (channel != null) {
			channel.close();
			channel = null;
		}
	}
	
	@SuppressWarnings("restriction")
	private void closeBuffer() throws Exception {
		Method m = FileChannelImpl.class.getDeclaredMethod("unmap",  
                MappedByteBuffer.class);  
        m.setAccessible(true);  
        m.invoke(FileChannelImpl.class, buffer);
        buffer = null;
	}

}