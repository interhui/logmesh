package org.pinae.logmesh.receiver.udp;

import java.util.HashMap;
import java.util.Map;

import org.pinae.logmesh.receiver.AbstractReceiver;
import org.pinae.logmesh.receiver.event.UDPReceiver;

/**
 * UDP消息接收器测试类
 * 
 * @author Huiyugeng
 * 
 * 
 */
public class UDPRecevierTest {
	public static void main(String arg[]) {
		Map<String, Object> parameters = new HashMap<String, Object>();
		parameters.put("por", "514");

		AbstractReceiver receiver = new UDPReceiver();
		receiver.initialize(parameters);
		receiver.start("UDPReceiver");
	}
}
