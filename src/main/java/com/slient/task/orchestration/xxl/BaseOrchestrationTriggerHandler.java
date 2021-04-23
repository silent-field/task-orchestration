package com.slient.task.orchestration.xxl;

/**
 * @author gy
 * @version 1.0
 * @date 2021/3/16.
 * @description:
 */
public abstract class BaseOrchestrationTriggerHandler {
	protected OrchestrationContext context;

	public BaseOrchestrationTriggerHandler(OrchestrationContext context) {
		this.context = context;
	}

	/**
	 * 是否可执行
	 *
	 * @return
	 */
	public abstract boolean canExecute();

	/**
	 * 执行完成后操作
	 *
	 * @return
	 */
	public abstract int whenExecuteFinish();

	/**
	 * 修改任务参数
	 *
	 * @param origin
	 * @return
	 */
	public abstract String modifyXxlJobParams(String origin);
}
