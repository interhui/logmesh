package org.pinae.logmesh.receiver.udp;

import java.util.HashMap;
import java.util.Map;

import org.pinae.logmesh.receiver.Receiver;
import org.pinae.logmesh.receiver.UDPReceiver;

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

		Receiver receiver = new UDPReceiver();
		receiver.init(parameters);
		receiver.start("UDPReceiver");
	}
}
