package org.pinae.logmesh.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

/**
 * 匹配工具集
 * 
 * @author Huiyugeng
 *
 */
public class MatchUtils {
	/**
	 * 字符串匹配，如果规则字符串为空则判断条件忽略，但当规则不为空时日志字符串为空，则判断不匹配
	 **/
	public static boolean matchString(String rule, String log) {
		if (StringUtils.isBlank(rule)) {
			return true;
		}
		if (StringUtils.isBlank(log)) {
			return false;
		}
		if (rule.equals("*")) {
			return true;
		}
		if (log.matches(rule)) {
			return true;
		}
		return false;
	}

	/**
	 * 匹配日志发生时间
	 **/
	public static boolean matchTime(String ruleTimeRange, String logTime) {

		if (StringUtils.isBlank(ruleTimeRange) || ruleTimeRange.trim().equals("*")) {
			return true;
		}

		if (StringUtils.isBlank(logTime)) {
			return false;
		}

		String absoluteTimeFormat = "(\\d+-\\d+-\\d+)\\s+(\\d+:\\d+:\\d+)\\s*-\\s*(\\d+/\\d+-\\d+)\\s+(\\d+:\\d+:\\d+)";
		String periodTimeFormat = "(\\d+:\\d+:\\d+)\\s*-\\s*(\\d+:\\d+:\\d+)";

		try {
			String startTime = "";
			String endTime = "";

			Date now = new Date();
			Date startDate = new Date();
			Date endDate = new Date();
			Date logDate = new Date();

			if (ruleTimeRange.matches(absoluteTimeFormat)) {
				Pattern pattern = Pattern.compile(absoluteTimeFormat);
				java.util.regex.Matcher matcher = pattern.matcher(ruleTimeRange);
				if (matcher.find() && matcher.groupCount() == 4) {
					startTime = String.format("%s %s", matcher.group(1), matcher.group(2));
					endTime = String.format("%s %s", matcher.group(3), matcher.group(4));
				}
			} else if (ruleTimeRange.matches(periodTimeFormat)) {
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
				String date = dateFormat.format(now);

				Pattern pattern = Pattern.compile(periodTimeFormat);
				java.util.regex.Matcher matcher = pattern.matcher(ruleTimeRange);

				if (matcher.find() && matcher.groupCount() == 2) {
					startTime = String.format("%s %s", date, matcher.group(1));
					endTime = String.format("%s %s", date, matcher.group(2));
				}
			}

			// 验证格式字符串并进行转换
			String timeFormat = "\\d+-\\d+-\\d+\\s+\\d+:\\d+:\\d+";
			if (logTime.matches(timeFormat) && startTime.matches(timeFormat) && endTime.matches(timeFormat)) {
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				logDate = dateFormat.parse(logTime);
				startDate = dateFormat.parse(startTime);
				endDate = dateFormat.parse(endTime);
			}

			// 判断时间
			if (logDate.getTime() > startDate.getTime() && logDate.getTime() < endDate.getTime()) {
				return true;
			}

		} catch (ParseException e) {

		}
		return false;
	}
}
