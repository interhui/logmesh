package org.pinae.logmesh.receiver.tcp;

import java.util.HashMap;
import java.util.Map;

import org.pinae.logmesh.receiver.Receiver;
import org.pinae.logmesh.receiver.TCPReceiver;

/**
 * TCP消息接收器测试类
 * 
 * @author Huiyugeng
 * 
 * 
 */
public class TCPRecevierTest {
	public static void main(String arg[]) {
		Map<String, String> config = new HashMap<String, String>();
		config.put("por", "514");

		Receiver receiver = new TCPReceiver();
		receiver.init(config);
		receiver.start("TCPReceiver");
	}
}
