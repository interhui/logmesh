package org.pinae.logmesh.component.filter.impl;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.pinae.logmesh.component.filter.AbstractFilter;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.util.FileUtils;
import org.pinae.ndb.Ndb;

/**
 * 关键字过滤器
 * 
 * @author Huiyugeng
 * 
 * 
 */
public class KeywordFilter extends AbstractFilter {
	private static Logger logger = Logger.getLogger(KeywordFilter.class);

	private List<String> keywordList = new ArrayList<String>(); // 关键字列表

	private boolean pass = true; // 匹配通过

	public KeywordFilter() {

	}

	@SuppressWarnings("unchecked")
	@Override
	public void initialize() {
		
		this.pass = getBooleanValue("pass", true);
		
		if (hasParameter("file")) {
			String filterFilename = getStringValue("file", "filter/keyword_filter.xml");
			if (StringUtils.isNoneEmpty(filterFilename)) {
				File filterFile = FileUtils.getFile(filterFilename);
				if (filterFile != null) {
					load(filterFile);
				} else {
					logger.error(String.format("KeywordFilter Load Exception: exception=File doesn't extis, file=%s", filterFilename));
				}
			}
		} else if (hasParameter("filter")) {
			Object filter = getValue("filter");
			if (filter != null) {
				if (filter instanceof String) {
					String filterStr = (String)filter;
					if (StringUtils.isNoneEmpty(filterStr)) {
						this.keywordList.addAll(Arrays.asList(filterStr.split("\\|")));
					}
				} else if (filter instanceof List) {
					this.keywordList = (List<String>)filter;
				}
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
						logger.error(String.format("KeywordFilter Load Exception: exception=File doesn't extis, source=%s, import=%s/%s",
								filterFile.getPath(), filterFile.getAbsolutePath(), importFilename));
					}
				}
			}
		}

		if (filterConfig != null && filterConfig.containsKey("filter")) {
			this.keywordList = (List<String>) Ndb.execute(filterConfig, "select:filter->keyword");
		}
	}

	@Override
	public Message filter(Message message) {

		Object msgContent = message.getMessage();

		if (msgContent != null) {

			boolean matched = false;

			if (msgContent instanceof byte[]) {
				try {
					msgContent = new String((byte[]) msgContent, "utf8");
				} catch (UnsupportedEncodingException e) {
					return null;
				}
			}

			for (String keyword : keywordList) {
				if (msgContent.toString().contains(keyword)) {
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
