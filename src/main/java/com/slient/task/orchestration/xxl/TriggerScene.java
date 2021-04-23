package com.slient.task.orchestration.xxl;


import com.slient.task.orchestration.dict.Dict;


/**
 * @author gy
 * @version 1.0
 * @date 2021/3/18.
 * @description: 触发场景，NORMAL-普通；MAKE_UP-补偿，并补偿下游；MAKE_UP_ONLY-补偿，且只补偿当前任务
 */
enum TriggerScene implements Dict<String, String> {
	/**
	 * 普通
	 */
	NORMAL("1", "普通"),
	/**
	 * 补偿，并补偿下游
	 */
	MAKE_UP("2", "补偿，并补偿下游"),
	/**
	 * 补偿，且只补偿自己
	 */
	MAKE_UP_ONLY("3", "补偿，且只补偿自己");

	private String code;
	private String desc;

	TriggerScene(String code, String desc) {
		this.code = code;
		this.desc = desc;
	}

	@Override
	public String getCode() {
		return code;
	}

	@Override
	public String getDesc() {
		return desc;
	}

	public static TriggerScene getByCode(String code) {
		for (TriggerScene p : TriggerScene.values()) {
			if (p.getCode().equals(code)) {
				return p;
			}
		}
		return null;
	}
}
