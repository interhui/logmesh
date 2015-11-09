package org.pinae.logmesh.component.filter;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.util.ClassLoaderUtils;
import org.pinae.ndb.Statement;

/**
 * 正则表达式过滤器
 * 
 * @author Huiyugeng
 * 
 * 
 */
public class RegexFilter extends AbstractFilter {

	private Statement statement = new Statement();
	
	private List<Pattern> patternList = new ArrayList<Pattern>(); // 正则表达式列表

	private boolean pass = true; // 匹配通过

	public RegexFilter() {

	}

	@Override
	public void init() {
		String path = ClassLoaderUtils.getResourcePath("");
		String regexFile = getParameter("file");

		try {
			this.pass = Boolean.parseBoolean(getParameter("pass"));
		} catch (Exception e) {
			this.pass = true;
		}

		load(path, regexFile);
	}

	@SuppressWarnings("unchecked")
	private void load(String path, String filename) {
		Map<String, Object> regexFilterConfig = loadConfig(path, filename);

		if (regexFilterConfig != null && regexFilterConfig.containsKey("import")) {
			List<String> importList = (List<String>) statement.execute(regexFilterConfig, "select:import->file");
			for (String file : importList) {
				if (StringUtils.isNotEmpty(file)) {
					load(path, file);
				}
			}
		}

		if (regexFilterConfig != null && regexFilterConfig.containsKey("filter")) {
			List<String> patterns = (List<String>) statement.execute(regexFilterConfig, "select:filter->pattern");
			for (String pattern : patterns) {
				this.patternList.add(Pattern.compile(pattern));
			}
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

			for (Pattern pattern : patternList) {
				Matcher matcher = pattern.matcher(msgContent.toString());
				if (matcher.find()) {
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
