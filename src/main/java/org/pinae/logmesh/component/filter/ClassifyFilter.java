package org.pinae.logmesh.component.filter;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.util.ClassLoaderUtils;
import org.pinae.logmesh.util.MatchUtils;
import org.pinae.ndb.Statement;

/**
 * 日志分类过滤器
 * 
 * @author Huiyugeng
 * 
 * 
 */
public class ClassifyFilter extends AbstractFilter {

	private Statement statement = new Statement();
	
	private String classifyTypes[] = {"ip", "time", "owner", "content"};
	private Map<String, Map<String, String>> classifyMap = new HashMap<String, Map<String, String>>(); // 分类信息列表

	public ClassifyFilter() {
		//初始化分类列表
		for (String classifyType : classifyTypes) {
			classifyMap.put(classifyType, new HashMap<String, String>());
		}
	}

	@Override
	public void init() {
		String path = ClassLoaderUtils.getResourcePath("");
		String classifyFile = getParameter("file");

		load(path, classifyFile);
	}

	@SuppressWarnings("unchecked")
	private void load(String path, String filename) {
		Map<String, Object> classifyFilterConfig = loadConfig(path, filename);

		if (classifyFilterConfig != null && classifyFilterConfig.containsKey("import")) {
			List<String> importList = (List<String>) statement.execute(classifyFilterConfig, "select:import->file");
			for (String file : importList) {
				if (StringUtils.isNotEmpty(file)) {
					load(path, file);
				}
			}
		}

		if (classifyFilterConfig != null && classifyFilterConfig.containsKey("filter")) {
			List<Object> classifyFilterList = (List<Object>)statement.execute(classifyFilterConfig, "select:filter");
			for (Object classifyFilter : classifyFilterList) {
				String label = (String) statement.execute(classifyFilter, "one:label");
				
				String type = (String) statement.execute(classifyFilter, "one:type");
				List<String> valueList = (List<String>) statement.execute(classifyFilter, "select:value");
				
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
			
			Set<Entry<String, String>> classifyEntrySet =  classifyList.entrySet();
			for (Entry<String, String> classifyEntry : classifyEntrySet) {
				String pattern = classifyEntry.getKey();
				String type = classifyEntry.getValue();
				
				if (classifyType.endsWith("ip")){
					if (MatchUtils.matchString(pattern, msgIP)) {
						msgTypeList.add(type);
					}
				} else if (StringUtils.isNotEmpty(msgOwner) && classifyType.endsWith("owner")){
					if (MatchUtils.matchString(pattern, msgOwner)) {
						msgTypeList.add(type);
					}
				} else if (msgContent != null && StringUtils.isNotEmpty(msgContent.toString()) && classifyType.endsWith("content")){
					if (MatchUtils.matchString(pattern, msgContent.toString())) {
						msgTypeList.add(type);
					}
				} else if (classifyType.endsWith("time")){
					SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					String logTime = dateFormat.format(new Date(msgTimestamp));
					if (MatchUtils.matchTime(pattern, logTime)) {
						msgTypeList.add(type);
					}
				}
			}
		}
		
		if (msgTypeList.size() == 0) {
			msgTypeList.add("unknow");
		}
		message.setType(StringUtils.join(msgTypeList, "|"));
		
		return message;

	}
	
}
