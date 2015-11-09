package org.pinae.logmesh.component.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.util.ClassLoaderUtils;
import org.pinae.ndb.Statement;

/**
 * IP地址过滤器
 * 
 * @author Huiyugeng
 * 
 * 
 */
public class IPFilter extends BasicFilter {

	private Statement statement = new Statement();
	private List<String> ipList = new ArrayList<String>(); // IP地址列表

	private boolean pass = true; // 匹配通过

	public IPFilter() {

	}

	@Override
	public void init() {
		String path = ClassLoaderUtils.getResourcePath("");
		String ipFile = getParameter("file");

		try {
			this.pass = Boolean.parseBoolean(getParameter("pass"));
		} catch (Exception e) {
			this.pass = true;
		}

		load(path, ipFile);
	}

	@SuppressWarnings("unchecked")
	private void load(String path, String filename) {
		Map<String, Object> ipFilterConfig = loadConfig(path, filename);

		if (ipFilterConfig != null && ipFilterConfig.containsKey("import")) {
			List<String> importList = (List<String>) statement.execute(ipFilterConfig, "select:import->file");
			for (String file : importList) {
				if (StringUtils.isNotEmpty(file)) {
					load(path, file);
				}
			}
		}

		if (ipFilterConfig != null && ipFilterConfig.containsKey("filter")) {
			ipList = (List<String>) statement.execute(ipFilterConfig, "select:filter->ip");
		}
	}

	@Override
	public Message filter(Message message) {
		Object msgContent = message.getMessage();
		String msgIP = message.getIP();

		if (msgContent != null && msgIP != null) {

			boolean matched = false;
			for (String ip : ipList) {
				if (msgIP.matches(ip)) {
					matched = true;
					break;
				}
			}

			if (pass) {
				return matched ? message : null;
			} else {
				return matched ? null : message;
			}
		} else {
			return null;
		}

	}

}
