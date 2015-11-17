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

	/* 正则表达式列表 */
	private List<Pattern> patternList = new ArrayList<Pattern>();

	/* true:匹配通过; false:匹配阻断 */
	private boolean pass = true;

	public RegexFilter() {

	}

	@Override
	public void init() {

		this.pass = getBooleanValue("pass", true);

		if (hasParameter("file")) {
			String path = ClassLoaderUtils.getResourcePath("");
			String regexFile = getStringValue("file", "filter/regex_filter.xml");
			if (StringUtils.isNoneEmpty(regexFile)) {
				load(path, regexFile);
			}
		} else if (hasParameter("filter")) {
			Object filter = getValue("filter");
			if (filter != null) {
				if (filter instanceof String) {
					String filterStr = (String)filter;
					if (StringUtils.isNoneEmpty(filterStr)) {
						String patterns[] = filterStr.split("\\|");
						for (String pattern : patterns) {
							this.patternList.add(Pattern.compile(pattern));
						}

					}
				} else if (filter instanceof List) {
					List<?> patterns = (List<?>)filter;
					for (Object pattern : patterns) {
						if (pattern != null) {
							if (pattern instanceof String) {
								this.patternList.add(Pattern.compile((String)pattern));
							} else if (pattern instanceof Pattern) {
								this.patternList.add((Pattern)pattern);
							}
						}
					}
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
			List<String> patterns = (List<String>) statement.execute(filterConfig, "select:filter->pattern");
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
