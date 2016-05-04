package org.pinae.logmesh.sender;

/**
 * 消息发送异常
 * 
 * @author Huiyugeng
 * 
 */
public class SendException extends Exception {

	private static final long serialVersionUID = 1382827099008979076L;

	/**
	 * 构造函数
	 * 
	 */
	public SendException() {
		super();
	}

	/**
	 * 构造函数
	 * 
	 * @param message 异常提示
	 * @param cause 异常引发原因
	 */
	public SendException(String msg, Throwable cause) {
		super(msg, cause);
	}

	/**
	 * 构造函数
	 * 
	 * @param message 异常提示
	 */
	public SendException(String msg) {
		super(msg);
	}

	/**
	 * 构造函数
	 * 
	 * @param cause 异常引发原因
	 */
	public SendException(Throwable cause) {
		super(cause);
	}
}
