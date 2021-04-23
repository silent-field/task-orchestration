package com.slient.task.orchestration.xxl;

import com.slient.task.orchestration.store.DefaultTaskDAGStore;
import lombok.extern.slf4j.Slf4j;

/**
 * @author gy
 * @version 1.0
 * @date 2021/3/8.
 * @description: xxl job 任务编排
 */
@Slf4j
public class OrchestrationHelper {
	private BaseOrchestrationTriggerHandler handler;

	public OrchestrationHelper(String xxlJobId, DefaultTaskDAGStore defaultTaskDAGStore) {
		OrchestrationContext context = new OrchestrationContext(xxlJobId, defaultTaskDAGStore);

		if (context.getTriggerScene() == TriggerScene.NORMAL) {
			handler = new OrchestrationTriggerNormalHandler(context);
		} else if (context.getTriggerScene() == TriggerScene.MAKE_UP) {
			handler = new OrchestrationTriggerMakeupHandler(context);
		} else {
			handler = new OrchestrationTriggerMakeupOnlyHandler(context);
		}
	}

	public boolean canExecute() {
		return handler.canExecute();
	}

	public int updateWhenExecuteFinish() {
		return handler.whenExecuteFinish();
	}

	public String modifyParams(String params) {
		return handler.modifyXxlJobParams(params);
	}
}
