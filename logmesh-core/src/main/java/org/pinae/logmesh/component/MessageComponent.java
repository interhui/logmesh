package org.pinae.logmesh.component;

/**
 * 消息处理组件
 * 
 * 包括: 过滤器/归并器/自定义处理器/路由器
 * 
 * @author Huiyugeng
 *
 */
public interface MessageComponent extends Cloneable {
	/**
	 * 初始化消息组件
	 */
	public void initialize();
}
