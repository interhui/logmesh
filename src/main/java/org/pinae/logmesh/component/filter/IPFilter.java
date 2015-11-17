package org.pinae.logmesh.component.filter;

import java.util.ArrayList;
import java.util.Arrays;
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
	
	/* IP地址列表 */
	private List<String> ipList = new ArrayList<String>(); 

	/* true:匹配通过; false:匹配阻断 */
	private boolean pass = true; // 

	public IPFilter() {

	}

	@SuppressWarnings("unchecked")
	@Override
	public void init() {

		this.pass = getBooleanValue("pass", true);
		
		if (hasParameter("file")) {
			String path = ClassLoaderUtils.getResourcePath("");
			String ipFile = getStringValue("file", "filter/ip_filter.xml");
			if (StringUtils.isNoneEmpty(ipFile)) {
				load(path, ipFile);
			}
		} else if (hasParameter("filter")) {
			Object filter = getValue("filter");
			if (filter != null) {
				if (filter instanceof String) {
					String filterStr = (String)filter;
					if (StringUtils.isNoneEmpty(filterStr)) {
						this.ipList = Arrays.asList(filterStr.split("\\|"));
					}
				} else if (filter instanceof List) {
					this.ipList = (List<String>)filter;
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	private void load(String path, String filename) {
		Map<String, Object> filterConfig = loadConfig(path, filename);

		if (filterConfig != null && filterConfig.containsKey("import")) {
			List<String> importList = (List<String>) statement.execute(filterConfig, "select:import->file");
			for (String file : importList) {
				if (StringUtils.isNotEmpty(file)) {
					load(path, file);
				}
			}
		}

		if (filterConfig != null && filterConfig.containsKey("filter")) {
			this.ipList = (List<String>) statement.execute(filterConfig, "select:filter->ip");
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
