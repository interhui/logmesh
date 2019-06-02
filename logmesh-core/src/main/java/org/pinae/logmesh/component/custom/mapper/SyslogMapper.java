package org.pinae.logmesh.component.custom.mapper;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

/**
 * 根据RFC3164对Syslog消息进行映射
 * 
 * @author Huiyugeng
 * 
 * 
 */
public class SyslogMapper implements Mapper {

	private static Map<Integer, String> FACILITY_CODE = new HashMap<Integer, String>();
	private static Map<Integer, String> SEVERITY_CODE = new HashMap<Integer, String>();

	static {
		// 程序模块（Facility）
		FACILITY_CODE.put(0, "kernel messages");
		FACILITY_CODE.put(1, "user-level messages");
		FACILITY_CODE.put(2, "mail system");
		FACILITY_CODE.put(3, "system daemons");
		FACILITY_CODE.put(4, "security/authorization messages (note 1)");
		FACILITY_CODE.put(5, "messages generated internally by syslogd");
		FACILITY_CODE.put(6, "line printer subsystem");
		FACILITY_CODE.put(7, "network news subsystem");
		FACILITY_CODE.put(8, "UUCP subsystem");
		FACILITY_CODE.put(9, "clock daemon (note 2)");
		FACILITY_CODE.put(10, "security/authorization messages (note 1)");
		FACILITY_CODE.put(11, "FTP daemon");
		FACILITY_CODE.put(12, "NTP subsystem");
		FACILITY_CODE.put(13, "log audit (note 1)");
		FACILITY_CODE.put(14, "log alert (note 1)");
		FACILITY_CODE.put(15, "clock daemon (note 2)");
		FACILITY_CODE.put(16, "local use 0  (local0)");
		FACILITY_CODE.put(17, "local use 1  (local1)");
		FACILITY_CODE.put(18, "local use 2  (local2)");
		FACILITY_CODE.put(19, "local use 3  (local3)");
		FACILITY_CODE.put(20, "local use 4  (local4)");
		FACILITY_CODE.put(21, "local use 5  (local5)");
		FACILITY_CODE.put(22, "local use 6  (local6)");
		FACILITY_CODE.put(23, "local use 7  (local7)");

		// 严重性（Severity）
		SEVERITY_CODE.put(0, "Emergency: system is unusable");
		SEVERITY_CODE.put(1, "Alert: action must be taken immediately");
		SEVERITY_CODE.put(2, "Critical: critical conditions");
		SEVERITY_CODE.put(3, "Error: error conditions");
		SEVERITY_CODE.put(4, "Warning: warning conditions");
		SEVERITY_CODE.put(5, "Notice: normal but significant condition");
		SEVERITY_CODE.put(6, "Informational: informational messages");
		SEVERITY_CODE.put(7, "Debug: debug-level messages");
	}

	/**
	 * 根据RFC3164对Syslog进行映射
	 * 
	 * @param message syslog消息
	 * 
	 * @return 映射后的消息
	 */
	public Map<String, String> map(String message) {
		Map<String, String> itemRefMap = new HashMap<String, String>();

		String logPattern = "<\\d+>.*";
		if (message.matches(logPattern)) {
			String strPRI = StringUtils.substringBetween(message, "<", ">");
			int facility = 0;
			int severity = 0;

			if (StringUtils.isNotEmpty(strPRI) && StringUtils.isAlphanumeric(strPRI)) {
				int pri = Integer.parseInt(strPRI);
				facility = pri / 8;
				severity = pri % 8;
			}
			itemRefMap.put("facilityCode", Integer.toString(facility));
			itemRefMap.put("facilityDescription", FACILITY_CODE.containsKey(facility) ? FACILITY_CODE.get(facility)
					: "Unknown");
			itemRefMap.put("severityCode", Integer.toString(severity));
			itemRefMap.put("severityDescription", SEVERITY_CODE.containsKey(severity) ? SEVERITY_CODE.get(severity)
					: "Unknown");

			message = StringUtils.substringAfter(message, ">");
		}
		itemRefMap.put("message", message);
		return itemRefMap;
	}

}
