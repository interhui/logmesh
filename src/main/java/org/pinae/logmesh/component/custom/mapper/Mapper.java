package org.pinae.logmesh.component.custom.mapper;

import java.util.Map;


/**
 * 消息解析与映射
 * 
 * @author Huiyugeng
 *
 */
public interface Mapper {

	public Map<String, String> map(String message);
	
}
