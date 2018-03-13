package org.pinae.logmesh.component.filter;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.util.FileUtils;
import org.pinae.logmesh.util.MatchUtils;
import org.pinae.ndb.Ndb;

/**
 * 消息分类过滤器
 * 
 * @author Huiyugeng
 * 
 * 
 */
public class ClassifyFilter extends AbstractFilter {
	private static Logger logger = Logger.getLogger(ClassifyFilter.class);

	private String classifyTypes[] = { "ip", "time", "owner", "content" };
	private Map<String, Map<String, String>> classifyMap = new HashMap<String, Map<String, String>>(); // 分类信息列表

	public ClassifyFilter() {
		// 初始化分类列表
		for (String classifyType : classifyTypes) {
			classifyMap.put(classifyType, new HashMap<String, String>());
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void initialize() {
		if (hasParameter("file")) {
			String filterFilename = getStringValue("file", "filter/classify_filter.xml");
			if (StringUtils.isNoneEmpty(filterFilename)) {
				File filterFile = FileUtils.getFile(filterFilename);
				if (filterFile != null) {
					load(filterFile);
				} else {
					logger.error(String.format("ClassifyFilter Load Exception: exception=File doesn't extis, file=%s", filterFilename));
				}
			}
		} else if (hasParameter("filter")) {
			Object filter = getValue("filter");
			if (filter != null && filter instanceof Map) {
				this.classifyMap = (Map<String, Map<String, String>>) filter;
			}
		}
	}

	@SuppressWarnings("unchecked")
	private void load(File filterFile) {
		Map<String, Object> filterConfig = loadConfig(filterFile);

		if (filterConfig != null && filterConfig.containsKey("import")) {
			List<String> importFilenameList = (List<String>) Ndb.execute(filterConfig, "select:import->file");
			for (String importFilename : importFilenameList) {
				if (StringUtils.isNotEmpty(importFilename)) {
					File importFile = FileUtils.getFile(filterFile.getParent(), importFilename);
					if (importFile != null) {
						loadConfig(importFile);
					} else {
						logger.error(String.format("ClassifyFilter Load Exception: exception=File doesn't extis, source=%s, import=%s/%s",
								filterFile.getPath(), filterFile.getAbsolutePath(), importFilename));
					}
				}
			}
		}

		if (filterConfig != null && filterConfig.containsKey("filter")) {
			List<Object> classifyFilterList = (List<Object>) Ndb.execute(filterConfig, "select:filter");
			for (Object classifyFilter : classifyFilterList) {
				String label = (String) Ndb.execute(classifyFilter, "one:label");

				String type = (String) Ndb.execute(classifyFilter, "one:type");
				List<String> valueList = (List<String>) Ndb.execute(classifyFilter, "select:value");

				if (label != null) {
					label = label.toLowerCase();
					if (classifyMap.containsKey(label)) {
						Map<String, String> classifyList = classifyMap.get(label);
						for (String value : valueList) {
							classifyList.put(value, type);
						}
					}
				}
			}
		}
	}

	@Override
	public Message filter(Message message) {

		List<String> msgTypeList = new ArrayList<String>();

		Object msgContent = message.getMessage();
		String msgIP = message.getIP();
		String msgOwner = message.getOwner();
		long msgTimestamp = message.getTimestamp();

		for (String classifyType : classifyTypes) {
			Map<String, String> classifyList = classifyMap.get(classifyType);

			Set<Entry<String, String>> classifyEntrySet = classifyList.entrySet();
			for (Entry<String, String> classifyEntry : classifyEntrySet) {
				String pattern = classifyEntry.getKey();
				String type = classifyEntry.getValue();

				if (classifyType.endsWith("ip")) {
					if (MatchUtils.matchString(pattern, msgIP)) {
						msgTypeList.add(type);
					}
				} else if (StringUtils.isNotEmpty(msgOwner) && classifyType.endsWith("owner")) {
					if (MatchUtils.matchString(pattern, msgOwner)) {
						msgTypeList.add(type);
					}
				} else if (msgContent != null && StringUtils.isNotEmpty(msgContent.toString()) && classifyType.endsWith("content")) {
					if (MatchUtils.matchString(pattern, msgContent.toString())) {
						msgTypeList.add(type);
					}
				} else if (classifyType.endsWith("time")) {
					SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					String logTime = dateFormat.format(new Date(msgTimestamp));
					if (MatchUtils.matchTime(pattern, logTime)) {
						msgTypeList.add(type);
					}
				}
			}
		}

		if (msgTypeList.size() == 0) {
			msgTypeList.add("unknown");
		}
		message.setType(StringUtils.join(msgTypeList, "|"));

		return message;

	}

}
