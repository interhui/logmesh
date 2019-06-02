package org.pinae.logmesh.component.filter.impl;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.util.FileUtils;
import org.pinae.ndb.Ndb;

/**
 * IP地址过滤器
 * 
 * @author Huiyugeng
 * 
 * 
 */
public class IPFilter extends BasicFilter {
	private static Logger logger = Logger.getLogger(IPFilter.class);
	
	/* IP地址列表 */
	private List<String> ipList = new ArrayList<String>(); 

	/* true:匹配通过; false:匹配阻断 */
	private boolean pass = true; // 

	public IPFilter() {

	}

	@SuppressWarnings("unchecked")
	@Override
	public void initialize() {

		this.pass = getBooleanValue("pass", true);
		
		if (hasParameter("file")) {
			String filterFilename = getStringValue("file", "filter/ip_filter.xml");
			if (StringUtils.isNoneEmpty(filterFilename)) {
				File filterFile = FileUtils.getFile(filterFilename);
				if (filterFile != null) {
					load(filterFile);
				} else {
					logger.error(String.format("IPFilter Load Exception: exception=File doesn't extis, file=%s", filterFilename));
				}
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
	private void load(File filterFile) {
		Map<String, Object> filterConfig = loadConfig(filterFile);

		if (filterConfig != null && filterConfig.containsKey("import")) {
			List<String> importList = (List<String>) Ndb.execute(filterConfig, "select:import->file");
			for (String importFilename : importList) {
				if (StringUtils.isNotEmpty(importFilename)) {
					File importFile = FileUtils.getFile(filterFile.getParent(), importFilename);
					if (importFile != null) {
						loadConfig(importFile);
					} else {
						logger.error(String.format("IPFilter Load Exception: exception=File doesn't extis, source=%s, import=%s/%s",
								filterFile.getPath(), filterFile.getAbsolutePath(), importFilename));
					}
				}
			}
		}

		if (filterConfig != null && filterConfig.containsKey("filter")) {
			this.ipList = (List<String>) Ndb.execute(filterConfig, "select:filter->ip");
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
