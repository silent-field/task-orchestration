package com.slient.task.orchestration.consts;


import com.slient.task.orchestration.dict.Dict;

/**
 * @author gy
 * @version 1.0
 * @date 2021/2/25.
 * @description:
 */
public enum MakeupTaskPoolStatusEnum implements Dict<Integer, String> {
	/**
	 * 待处理
	 */
	TODO(1, "待处理"),
	/**
	 * 执行完成
	 */
	FINISH(2, "执行完成");

	private int code;
	private String desc;

	MakeupTaskPoolStatusEnum(int code, String desc) {
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
