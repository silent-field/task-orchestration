package com.slient.task.orchestration.xxl;

import com.slient.task.orchestration.model.TaskLog;
import com.slient.task.orchestration.model.TaskNode;

/**
 * @author gy
 * @version 1.0
 * @date 2021/3/18.
 * @description:
 */
public class BeanHelper {
	private BeanHelper() {
	}

	public static TaskLog convertToTaskLog(TaskNode taskNode, TriggerScene triggerScene, String dag, String upstreamVersion
			, String xxlParams, String executeParams, long startTime, long endTime) {
		TaskLog taskLog = TaskLog.builder()
				.taskId(taskNode.getTaskId()).dag(dag)
				.taskGroupId(taskNode.getTaskGroupId())
				.version(taskNode.getVersion() + 1).preVersion(taskNode.getVersion())
				.upstreamVersion(upstreamVersion).scene(Integer.valueOf(triggerScene.getCode())).
						xxlParams(xxlParams).executeParams(executeParams)
				.startTime(startTime).endTime(endTime).costTime(endTime - startTime).build();

		return taskLog;
	}
}
