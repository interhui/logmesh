package org.pinae.logmesh.component.filter;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.util.ClassLoaderUtils;
import org.pinae.ndb.Statement;

/**
 * 消息解码过滤器
 * 
 * @author Huiyugeng
 * 
 * 
 */
public class TextDecodeFilter extends AbstractFilter {

	private Statement statement = new Statement();

	/* IP地址-解码对应 */
	private Map<String, String> decodeMap = new HashMap<String, String>();

	public TextDecodeFilter() {

	}

	@SuppressWarnings("unchecked")
	@Override
	public void init() {
		if (hasParameter("file")) {
			String path = ClassLoaderUtils.getResourcePath("");
			String decodeFile = getStringValue("file", "filter/text_decode_filter.xml");
			if (StringUtils.isNoneEmpty(decodeFile)) {
				load(path, decodeFile);
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
			List<Map<String, Object>> filterList = (List<Map<String, Object>>) statement.execute(filterConfig, "select:filter");
			for (Map<String, Object> filter : filterList) {
				String code = filter.containsKey("code") ? (String) filter.get("code") : "utf8";
				List<String> ipList = (List<String>) statement.execute(filter, "select:ip");
				for (String ip : ipList) {
					decodeMap.put(ip, code);
				}
			}
		}
	}

	@Override
	public Message filter(Message message) {
		Object msgContent = message.getMessage();
		String msgIP = message.getIP();

		if (msgContent != null && msgIP != null) {

			Set<String> ipSet = decodeMap.keySet();

			for (String ip : ipSet) {
				if (msgIP.matches(ip)) {
					String code = decodeMap.get(ip);
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
