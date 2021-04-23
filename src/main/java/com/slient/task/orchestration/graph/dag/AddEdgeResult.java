package com.slient.task.orchestration.graph.dag;


import com.slient.task.orchestration.dict.Dict;

/**
 * @author gy
 * @version 1.0
 * @date 2021/2/25.
 * @description:
 */
public enum AddEdgeResult implements Dict<Integer, String> {
	/**
	 * 添加边成功
	 */
	SUCCESS(1, "添加边成功"),
	/**
	 * 添加边失败
	 */
	FAIL(-1, "添加边失败"),
	/**
	 * 添加边出现异常
	 */
	EXCEPTION(-2, "添加边出现异常");

	private int code;
	private String desc;

	AddEdgeResult(int code, String desc) {
		this.code = code;
		this.desc = desc;
	}

	@Override
	public Integer getCode() {
		return code;
	}

	@Override
	public String getDesc() {
		return desc;
	}
}
