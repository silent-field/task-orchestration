package com.slient.task.orchestration.exception;

/**
 * @author gy
 * @version 1.0
 * @date 2021/3/17.
 * @description:
 */
public class DagRuntimeException extends RuntimeException {
	public DagRuntimeException() {
		super();
	}

	public DagRuntimeException(String message) {
		super(message);
	}

	public DagRuntimeException(String message, Throwable cause) {
		super(message, cause);
	}

}
