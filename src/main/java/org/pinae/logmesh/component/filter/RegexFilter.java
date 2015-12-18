package org.pinae.logmesh.component.filter;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.util.ClassLoaderUtils;
import org.pinae.logmesh.util.FileUtils;
import org.pinae.ndb.Statement;

/**
 * 正则表达式过滤器
 * 
 * @author Huiyugeng
 * 
 * 
 */
public class RegexFilter extends AbstractFilter {
	private static Logger logger = Logger.getLogger(RegexFilter.class);
	
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
			String filterFilename = getStringValue("file", "filter/regex_filter.xml");
			if (StringUtils.isNoneEmpty(filterFilename)) {
				File filterFile = FileUtils.getFile(path, filterFilename);
				if (filterFile != null) {
					load(filterFile);
				} else {
					logger.error(String.format("RegexFilter Load Exception: exception=File doesn't extis, file=%s/%s", path, filterFilename));
				}
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
	private void load(File filterFile) {
		Map<String, Object> filterConfig = loadConfig(filterFile);

		if (filterConfig != null && filterConfig.containsKey("import")) {
			List<String> importFilenameList = (List<String>) statement.execute(filterConfig, "select:import->file");
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
