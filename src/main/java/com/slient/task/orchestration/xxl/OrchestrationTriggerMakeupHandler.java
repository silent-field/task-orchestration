package com.slient.task.orchestration.xxl;

import com.google.common.base.Joiner;
import com.slient.task.orchestration.consts.MakeupTaskPoolStatusEnum;
import com.slient.task.orchestration.consts.OrchestrationServiceConstants;
import com.slient.task.orchestration.model.MakeupRecord;
import com.slient.task.orchestration.model.MakeupTaskPool;
import com.slient.task.orchestration.model.TaskLog;
import com.slient.task.orchestration.model.TaskNode;
import com.slient.task.orchestration.params.ParamHelper;
import com.slient.task.orchestration.util.GsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author gy
 * @version 1.0
 * @date 2021/3/16.
 * @description:
 */
@Slf4j
public class OrchestrationTriggerMakeupHandler extends BaseOrchestrationTriggerHandler {
	public OrchestrationTriggerMakeupHandler(OrchestrationContext context) {
		super(context);
	}

	@Override
	public boolean canExecute() {
		log.info("补偿任务(触发补偿所有下游)可执行");
		return true;
	}

	@Override
	public int whenExecuteFinish() {
		// 不更新版本，写补偿任务池
		long endTime = System.currentTimeMillis();

		int row = 0;

		TaskLog taskLog = BeanHelper.convertToTaskLog(context.getCurrentTaskNode(), TriggerScene.MAKE_UP,
				context.prettyPrintDag(false)
				, "{}"
				, context.getTriggerParam().getExecutorParams(), context.getTriggerParam().getExecutorParams(),
				context.getStartTime(), endTime);
		taskLog.setVersion(0);
		taskLog.setPreVersion(0);

		row += context.getDefaultTaskDAGStore().insertTaskLog(taskLog);

		log.info("添加补偿任务(触发补偿所有下游)执行日志");

		// 记录下游任务到补偿表
		List<TaskNode> toMakeupTaskNodes = context.getAllDownstreamTaskNodes();
		String makeupParams = getMakeupParams();
		if (CollectionUtils.isNotEmpty(toMakeupTaskNodes)) {
			List<String> makeupTaskIds = toMakeupTaskNodes.stream().map(taskNode -> taskNode.getTaskId()).collect(Collectors.toList());

			// 记录makeup_record表，获取补偿版本号(记录id)
			MakeupRecord makeupRecord = MakeupRecord.builder()
					.taskId(context.getCurrentTaskNode().getTaskId())
					.taskGroupId(context.getCurrentTaskNode().getTaskGroupId())
					.downstreamTasks(Joiner.on(",").join(makeupTaskIds)).makeupParams(makeupParams)
					.build();
			int makeupVersion = context.getDefaultTaskDAGStore().insertMakeupRecord(makeupRecord);

			for (TaskNode toMakeupTaskNode : toMakeupTaskNodes) {
				MakeupTaskPool makeupTaskPool = MakeupTaskPool.builder()
						.makeupVersion(makeupVersion)
						.taskId(toMakeupTaskNode.getTaskId())
						.taskGroupId(toMakeupTaskNode.getTaskGroupId())
						.upstreamTask(context.getCurrentTaskNode().getTaskId())
						.status(MakeupTaskPoolStatusEnum.TODO.getCode())
						.makeupParams(makeupParams)
						.build();
				row += context.getDefaultTaskDAGStore().insertMakeupPool(makeupTaskPool);
			}
			log.info("添加补偿表记录,taskIds:{}", GsonUtils.toJson(makeupTaskIds));
		}

		return row;
	}

	/**
	 * 获取可以传入下游的补偿参数
	 *
	 * @return
	 */
	private String getMakeupParams() {
		Map<String, String> paramMap = ParamHelper.getParamMap(context.getTriggerParam().getExecutorParams());

		StringBuilder result = new StringBuilder();
		for (String key : OrchestrationServiceConstants.MAKE_UP_PARAM_KEYS) {
			if (paramMap.containsKey(key)) {
				result.append(key).append("=").append(paramMap.get(key)).append(",");
			}
		}

		return result.length() == 0 ? "" : result.substring(0, result.length() - 1);
	}

	@Override
	public String modifyXxlJobParams(String origin) {
		return origin;
	}
}
