package org.pinae.logmesh.processor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.pinae.logmesh.output.MessageOutputor;

/**
 * 处理器线程池
 * 
 * @author huiyugeng
 * 
 */
public class ProcessorPool {
	
	/**
	 * 消息展示器列表
	 */
	public static List<MessageOutputor> OUTPUTOR_LIST = Collections.synchronizedList(new ArrayList<MessageOutputor>());

	private static Map<String, Processor> PROCESSOR_MAP = new HashMap<String, Processor>();

	/**
	 * 添加处理器
	 * 
	 * @param name 处理器名称
	 * @param processor 处理器
	 */
	public static void addProcessor(String name, Processor processor) {
		if (PROCESSOR_MAP.containsKey(name)) {
			name = name + "_" + Long.toString(System.currentTimeMillis());
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
			}
		}
		PROCESSOR_MAP.put(name, processor);
	}

	/**
	 * 停止指定名称的处理器，如果处理器名称后含有*则停止此一类处理器
	 * 
	 * @param name 处理器名称
	 */
	public static void stop(String name) {
		if (name.endsWith("*")) {
			name = StringUtils.substringBefore(name, "*");
			Set<String> processorNameSet = PROCESSOR_MAP.keySet();
			for (String processorName : processorNameSet) {
				if (processorName.startsWith(name)) {
					Processor processor = PROCESSOR_MAP.get(processorName);
					if (processor != null) {
						processor.stop();
					}
				}
			}
		} else {
			Processor processor = PROCESSOR_MAP.get(name);
			if (processor != null) {
				processor.stop();
			}
		}

	}

	/**
	 * 停止所有处理器
	 * 
	 */
	public static void stopAll() {
		Collection<Processor> processorList = PROCESSOR_MAP.values();
		for (Processor processor : processorList) {
			processor.stop();
		}
	}

	/**
	 * 获取所有处理器
	 * 
	 * @return 处理器信息
	 */
	public static Map<String, Processor> getAllProcessors() {
		return PROCESSOR_MAP;
	}

	/**
	 * 获取指定名称的处理器，如果处理器名称后含有*则获取此一类处理器
	 * 
	 * @param name 处理器名称
	 * 
	 * @return 处理器列表
	 */
	public static List<Processor> getProcessors(String name) {
		List<Processor> processorList = new ArrayList<Processor>();

		if (name.endsWith("*")) {
			name = StringUtils.substringBefore(name, "*");
			Set<String> processorNameSet = PROCESSOR_MAP.keySet();
			for (String processorName : processorNameSet) {
				if (processorName.startsWith(name)) {
					Processor processor = PROCESSOR_MAP.get(processorName);
					if (processor != null) {
						processorList.add(processor);
					}
				}
			}
		} else {
			Processor processor = PROCESSOR_MAP.get(name);
			if (processor != null) {
				processorList.add(processor);
			}
		}

		return processorList;
	}
}
