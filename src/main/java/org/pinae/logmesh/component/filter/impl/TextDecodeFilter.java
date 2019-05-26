package org.pinae.logmesh.component.filter.impl;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.pinae.logmesh.component.filter.AbstractFilter;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.util.FileUtils;
import org.pinae.ndb.Ndb;

/**
 * 消息解码过滤器
 * 
 * @author Huiyugeng
 * 
 * 
 */
public class TextDecodeFilter extends AbstractFilter {
	private static Logger logger = Logger.getLogger(TextDecodeFilter.class);

	/* IP地址-解码对应 */
	private Map<String, String> decodeMap = new HashMap<String, String>();

	public TextDecodeFilter() {

	}

	@SuppressWarnings("unchecked")
	@Override
	public void initialize() {
		if (hasParameter("file")) {
			String filterFilename = getStringValue("file", "filter/text_decode_filter.xml");
			if (StringUtils.isNoneEmpty(filterFilename)) {
				File filterFile = FileUtils.getFile(filterFilename);
				if (filterFile != null) {
					load(filterFile);
				} else {
					logger.error(String.format("RegexFilter Load Exception: exception=File doesn't extis, file=%s", filterFilename));
				}
			}
		} else if (hasParameter("filter")) {
			Object filter = getValue("filter");
			if (filter != null) {
				if (filter instanceof String) {
					String filterStr = (String)filter;
					if (StringUtils.isNoneEmpty(filterStr)) {
						String decodePairs[] = filterStr.split("\\|");
						for (String decodePair : decodePairs) {
							String decode[] = decodePair.split("=");
							if (decode.length == 2 && decode[0] != null && decode[1] != null) {
								this.decodeMap.put(decode[0].trim(), decode[1].trim());
							}
						}
					}
				} else if (filter instanceof Map) {
					this.decodeMap = (Map<String,String>)filter;
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
						logger.error(String.format("RegexFilter Load Exception: exception=File doesn't extis, source=%s, import=%s/%s",
								filterFile.getPath(), filterFile.getAbsolutePath(), importFilename));
					}
				}
			}
		}

		if (filterConfig != null && filterConfig.containsKey("filter")) {
			List<Map<String, Object>> filterList = (List<Map<String, Object>>) Ndb.execute(filterConfig, "select:filter");
			for (Map<String, Object> filter : filterList) {
				String code = filter.containsKey("code") ? (String) filter.get("code") : "utf8";
				List<String> ipList = (List<String>) Ndb.execute(filter, "select:ip");
				for (String ip : ipList) {
					this.decodeMap.put(ip, code);
				}
			}
		}
	}

	@Override
	public Message filter(Message message) {
		Object msgContent = message.getMessage();
		String msgIP = message.getIP();

		if (msgContent != null && msgIP != null) {

			Set<String> ipSet = this.decodeMap.keySet();

			for (String ip : ipSet) {
				if (msgIP.matches(ip)) {
					String code = this.decodeMap.get(ip);
					if (StringUtils.isNotEmpty(code)) {
						try {
							if (msgContent instanceof byte[]) {
								msgContent = new String((byte[]) msgContent, code);
							}
						} catch (UnsupportedEncodingException e) {

						}
						break;
					}

				}
			}

			if (msgContent instanceof byte[]) {
				try {
					msgContent = new String((byte[]) msgContent, "UTF-8");
				} catch (UnsupportedEncodingException e) {

				}
			}
			message.setMessage(msgContent);
		} else {
			return null;
		}

		return message;

	}

}
