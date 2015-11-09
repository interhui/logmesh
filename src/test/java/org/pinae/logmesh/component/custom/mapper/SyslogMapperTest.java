package org.pinae.logmesh.component.custom.mapper;

import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.junit.Test;
import org.pinae.logmesh.component.custom.mapper.SyslogMapper;

public class SyslogMapperTest {

	@Test
	public void testMap() {
		SyslogMapper syslogMap = new SyslogMapper();
		Map<String, String> result = syslogMap
				.map("<190>2013-12-04 05:56:32 DXWG-FW-E1000-01 %%01SYSTATE/6/HEALTH(l): cpu=34 totalmemory=585162220 curmemory=238983984");

		assertEquals(result.get("facilityCode"), "23");
		assertEquals(result.get("severityCode"), "6");
	}
}
