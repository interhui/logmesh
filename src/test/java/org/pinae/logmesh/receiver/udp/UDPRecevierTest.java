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
		Map<String, String> config = new HashMap<String, String>();
		config.put("por", "514");

		Receiver receiver = new UDPReceiver();
		receiver.init(config);
		receiver.start("UDPReceiver");
	}
}
