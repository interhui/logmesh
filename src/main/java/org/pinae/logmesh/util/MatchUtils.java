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
	 * 字符串匹配
	 * 
	 * @param rule 匹配规则, 支持正则表达式, 如果为空则判定字符串匹配
	 * @param str 需要匹配的字符串
	 * 
	 * @return 字符串是否匹配
	 * 
	 **/
	public static boolean matchString(String rule, String str) {
		if (StringUtils.isBlank(rule)) {
			return true;
		}
		if (StringUtils.isBlank(str)) {
			return false;
		}
		if (rule.equals("*")) {
			return true;
		}
		if (str.matches(rule)) {
			return true;
		}
		return false;
	}

	/**
	 * 时间匹配
	 * 
	 * @param timeRange 时间区域包括绝对时间和相对时间
	 * 					绝对时间: yyyy-mm-dd HH:MM:SS (开始时间) - yyyy-mm-dd HH:MM:SS (结束时间) 
	 * 					相对时间: HH:MM:SS (开始时间) - HH:MM:SS (结束时间)
	 * @param time 需要匹配的时间, 格式为: yyyy-mm-dd HH:MM:SS
	 * 
	 * @return 需要匹配的时间是否在时间区域内
	 * 
	 **/
	public static boolean matchTime(String timeRange, String time) {

		if (StringUtils.isBlank(timeRange) || timeRange.trim().equals("*")) {
			return true;
		}

		if (StringUtils.isBlank(time)) {
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

			if (timeRange.matches(absoluteTimeFormat)) {
				Pattern pattern = Pattern.compile(absoluteTimeFormat);
				java.util.regex.Matcher matcher = pattern.matcher(timeRange);
				if (matcher.find() && matcher.groupCount() == 4) {
					startTime = String.format("%s %s", matcher.group(1), matcher.group(2));
					endTime = String.format("%s %s", matcher.group(3), matcher.group(4));
				}
			} else if (timeRange.matches(periodTimeFormat)) {
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
				String date = dateFormat.format(now);

				Pattern pattern = Pattern.compile(periodTimeFormat);
				java.util.regex.Matcher matcher = pattern.matcher(timeRange);

				if (matcher.find() && matcher.groupCount() == 2) {
					startTime = String.format("%s %s", date, matcher.group(1));
					endTime = String.format("%s %s", date, matcher.group(2));
				}
			}

			// 验证格式字符串并进行转换
			String timeFormat = "\\d+-\\d+-\\d+\\s+\\d+:\\d+:\\d+";
			if (time.matches(timeFormat) && startTime.matches(timeFormat) && endTime.matches(timeFormat)) {
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				logDate = dateFormat.parse(time);
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
