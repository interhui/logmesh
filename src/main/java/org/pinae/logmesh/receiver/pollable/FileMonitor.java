package org.pinae.logmesh.receiver.pollable;

import org.pinae.logmesh.receiver.Receiver;

/**
 * 文件监控
 * 
 * @author Huiyugeng
 *
 */
public class FileMonitor implements Receiver {
	/* 需要监控内容的文件 */
	private String filename;
	
	public FileMonitor(String filename) {
		this.filename = filename;
	}

	public void run() {

	}

	public void start(String name) {

	}

	public void stop() {
	}

	public boolean isRunning() {
		return false;
	}

	public String getName() {
		return null;
	}
	
}
