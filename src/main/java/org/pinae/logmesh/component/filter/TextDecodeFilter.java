package org.pinae.logmesh.component.filter;

import java.io.UnsupportedEncodingException;
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
	
	private Map<String, String> decodeMap = new HashMap<String, String>(); // IP地址-解码对应

	public TextDecodeFilter() {

	}

	@Override
	public void init() {
		String path = ClassLoaderUtils.getResourcePath("");
		String ipFile = getParameter("file");

		load(path, ipFile);
	}

	@SuppressWarnings("unchecked")
	private void load(String path, String filename) {
		Map<String, Object> decodeConfig = loadConfig(path, filename);

		if (decodeConfig != null && decodeConfig.containsKey("import")) {
			List<String> importList = (List<String>) statement.execute(decodeConfig, "select:import->file");
			for (String file : importList) {
				if (StringUtils.isNotEmpty(file)) {
					load(path, file);
				}
			}
		}

		if (decodeConfig != null && decodeConfig.containsKey("filter")) {
			List<Map<String, Object>> filterList = (List<Map<String, Object>>) statement.execute(decodeConfig,
					"select:filter");
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
