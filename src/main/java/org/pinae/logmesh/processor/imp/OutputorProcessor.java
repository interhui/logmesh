package org.pinae.logmesh.processor.imp;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.pinae.logmesh.output.MessageOutputor;
import org.pinae.logmesh.processor.Processor;
import org.pinae.logmesh.processor.ProcessorFactory;
import org.pinae.logmesh.processor.ProcessorPool;
import org.pinae.ndb.Ndb;

/**
 * 
 * 消息输出器线程
 * 
 * @author Huiyugeng
 * 
 */
public class OutputorProcessor implements Processor {

	/* 消息输出器配置信息 */
	private Map<String, Object> config;

	/* 消息输出组件列表 */
	private List<MessageOutputor> outputorList = new ArrayList<MessageOutputor>();

	public OutputorProcessor(Map<String, Object> config) {
		this.config = config;
	}

	public void run() {

	}

	public void stop() {

	}

	/**
	 * 载入消息输出器列表
	 * 
	 * @return 消息输出器列表
	 */
	@SuppressWarnings("unchecked")
	public static List<MessageOutputor> create(Map<String, Object> config) {
		List<MessageOutputor> outputorList = new ArrayList<MessageOutputor>();

		List<Map<String, Object>> outputConfigList = (List<Map<String, Object>>) Ndb.execute(config,
				"select:output->enable:true");

		for (Map<String, Object> outputConfig : outputConfigList) {

			Object outputorObject = ProcessorFactory.create(outputConfig);

			if (outputorObject != null && outputorObject instanceof MessageOutputor) {
				MessageOutputor outputor = (MessageOutputor) outputorObject;
				outputor.initialize();

				outputorList.add(outputor);
			}
		}

		return outputorList;
	}

	public void start(String name) {

		this.outputorList = create(this.config);

		for (MessageOutputor outputor : this.outputorList) {
			ProcessorPool.OUTPUTOR_LIST.add(outputor);
		}
	}

}
