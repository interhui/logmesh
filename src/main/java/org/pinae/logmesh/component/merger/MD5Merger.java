package org.pinae.logmesh.component.merger;

import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.util.MessageDigestUtils;

public class MD5Merger extends AbstractMerger {

	public void init() {
		
	}

	public void add(Message message) {
		if (message != null) {
			Object msgContent = message.getMessage();
			if (msgContent instanceof String) {
				String key = MessageDigestUtils.MD5((String)msgContent);
				
				addToMergerPool(key, message);
			}
		}
	}

}
