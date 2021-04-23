package com.slient.task.orchestration.dict;

/**
 * @author gy
 *
 * 字典类接口
 */
public interface Dict<C, D> {
	/**
	 * 标识
	 *
	 * @return
	 */
	C getCode();

	/**
	 * 描述
	 *
	 * @return
	 */
	D getDesc();
}
