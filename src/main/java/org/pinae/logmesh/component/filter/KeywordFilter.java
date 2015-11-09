package org.pinae.logmesh.component.filter;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.util.ClassLoaderUtils;
import org.pinae.ndb.Statement;

/**
 * 关键字过滤器
 * 
 * @author Huiyugeng
 * 
 * 
 */
public class KeywordFilter extends AbstractFilter {

	private Statement statement = new Statement();
	
	private List<String> keywordList = new ArrayList<String>(); // 关键字列表

	private boolean pass = true; // 匹配通过

	public KeywordFilter() {

	}

	@Override
	public void init() {
		String path = ClassLoaderUtils.getResourcePath("");
		String keywordFile = getParameter("file");

		try {
			this.pass = Boolean.parseBoolean(getParameter("pass"));
		} catch (Exception e) {
			this.pass = true;
		}

		load(path, keywordFile);
	}

	@SuppressWarnings("unchecked")
	private void load(String path, String filename) {
		Map<String, Object> keywordFilterConfig = loadConfig(path, filename);

		if (keywordFilterConfig != null && keywordFilterConfig.containsKey("import")) {
			List<String> importList = (List<String>) statement.execute(keywordFilterConfig, "select:import->file");
			for (String file : importList) {
				if (StringUtils.isNotEmpty(file)) {
					load(path, file);
				}
			}
		}

		if (keywordFilterConfig != null && keywordFilterConfig.containsKey("filter")) {
			this.keywordList = (List<String>) statement.execute(keywordFilterConfig, "select:filter->keyword");
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
