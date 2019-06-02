package org.pinae.logmesh.output.storer;

/**
 * 存储异常
 * 
 * @author Huiyugeng
 * 
 */
public class StorerException extends Exception {

	private static final long serialVersionUID = 2988098109127935876L;

	/**
	 * 构造函数
	 * 
	 */
	public StorerException() {
		super();
	}

	/**
	 * 构造函数
	 * 
	 * @param msg 异常提示
	 * @param cause 异常引发原因
	 */
	public StorerException(String msg, Throwable cause) {
		super(msg, cause);
	}

	/**
	 * 构造函数
	 * 
	 * @param msg 异常提示
	 */
	public StorerException(String msg) {
		super(msg);
	}

	/**
	 * 构造函数
	 * 
	 * @param cause 异常引发原因
	 */
	public StorerException(Throwable cause) {
		super(cause);
	}
}
