package com.slient.task.orchestration.xxl;

import com.slient.task.orchestration.model.TaskLog;
import lombok.extern.slf4j.Slf4j;

/**
 * @author gy
 * @version 1.0
 * @date 2021/3/16.
 * @description:
 */
@Slf4j
public class OrchestrationTriggerMakeupOnlyHandler extends BaseOrchestrationTriggerHandler {
	public OrchestrationTriggerMakeupOnlyHandler(OrchestrationContext context) {
		super(context);
	}

	@Override
	public boolean canExecute() {
		log.info("补偿任务(只执行当前任务)可执行");
		return true;
	}

	@Override
	public int whenExecuteFinish() {
		// 不更新版本，无需新增补偿任务池
		long endTime = System.currentTimeMillis();
		int row = 0;

		TaskLog taskLog = BeanHelper.convertToTaskLog(context.getCurrentTaskNode(), TriggerScene.MAKE_UP_ONLY,
				context.prettyPrintDag(false)
				, "{}"
				, context.getTriggerParam().getExecutorParams(), context.getTriggerParam().getExecutorParams(),
				context.getStartTime(), endTime);
		taskLog.setVersion(0);
		taskLog.setPreVersion(0);

		row += context.getDefaultTaskDAGStore().insertTaskLog(taskLog);

		log.info("添加补偿任务(只执行当前任务)执行日志");

		return row;
	}

	@Override
	public String modifyXxlJobParams(String origin) {
		return origin;
	}
}
