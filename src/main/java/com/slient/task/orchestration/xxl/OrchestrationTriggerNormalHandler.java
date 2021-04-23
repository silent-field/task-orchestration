package com.slient.task.orchestration.xxl;

import com.slient.task.orchestration.model.MakeupTaskPool;
import com.slient.task.orchestration.model.TaskLog;
import com.slient.task.orchestration.model.TaskPool;
import com.slient.task.orchestration.params.ParamHelper;
import com.slient.task.orchestration.util.GsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * @author gy
 * @version 1.0
 * @date 2021/3/16.
 * @description:
 */
@Slf4j
public class OrchestrationTriggerNormalHandler extends BaseOrchestrationTriggerHandler {
	private CanExecute canExecute = CanExecute.TODO;

	/**
	 * 关联的补偿任务，不一定存在
	 */
	private MakeupTaskPool attachMakeupTask;

	private String editedJobParams = StringUtils.EMPTY;

	public OrchestrationTriggerNormalHandler(OrchestrationContext context) {
		super(context);
	}

	@Override
	public boolean canExecute() {
		if (canExecute == CanExecute.TODO) {
			if (null == context.getCurrentTaskNode()) {
				log.info("xxlJobId:{}没有对应的dag配置，不可执行", context.getXxlJobId());
				canExecute = CanExecute.CAN_NOT;
			} else if (CollectionUtils.isEmpty(context.getUpstreams())) {
				log.info("xxlJobId:{}没有依赖上游，可执行", context.getXxlJobId());
				canExecute = CanExecute.CAN;
			} else {
				// 分析补偿任务池，如果存在补偿任务，使用补偿任务参数进行补偿
				attachMakeupTask = context.findMakeupTask();
				if (null != attachMakeupTask) {
					canExecute = CanExecute.CAN;
					log.info("xxlJobId:{}存在补偿任务id:{}，可执行", context.getXxlJobId(), attachMakeupTask.getId());
					return true;
				}

				// 不存在补偿任务，按普通任务场景处理
				/** 上游任务版本号比上一次的都高则允许执行 */
				boolean allUpstreamUpdate = true;
				for (String upstreamTaskId : context.getUpstreamTaskIdList()) {
					int currentVersion = context.getCurrentUpstreamTaskId2Version().containsKey(upstreamTaskId) ?
							context.getCurrentUpstreamTaskId2Version().get(upstreamTaskId) : 0;

					int preVersion = context.getPreUpstreamTaskId2Version().containsKey(upstreamTaskId) ?
							context.getPreUpstreamTaskId2Version().get(upstreamTaskId) : 0;

					if (currentVersion <= preVersion) {
						allUpstreamUpdate = false;
						break;
					}
				}

				/** 所有的上游都更新才可执行*/
				if (allUpstreamUpdate) {
					log.info("xxlJobId:{}上游节点当前版本：{} 比上一轮版本： {} 高，可执行",
							context.getXxlJobId(), GsonUtils.toJson(context.getCurrentUpstreamTaskId2Version()),
							GsonUtils.toJson(context.getPreUpstreamTaskId2Version()));
					canExecute = CanExecute.CAN;
				} else {
					log.info("xxlJobId:{}上游节点当前版本：{} 没有比上一轮版本： {} 高，不可执行",
							context.getXxlJobId(), GsonUtils.toJson(context.getCurrentUpstreamTaskId2Version()),
							GsonUtils.toJson(context.getPreUpstreamTaskId2Version()));
					canExecute = CanExecute.CAN_NOT;
				}
			}
		}

		return canExecute == CanExecute.CAN;
	}

	@Override
	public String modifyXxlJobParams(String origin) {
		if (null == attachMakeupTask) {
			return origin;
		}

		Map<String, String> paramMap = ParamHelper.getParamMap(origin);
		Map<String, String> makeupParamMap = ParamHelper.getParamMap(attachMakeupTask.getMakeupParams());

		for (Map.Entry<String, String> entry : makeupParamMap.entrySet()) {
			paramMap.put(entry.getKey(), entry.getValue());
		}
		editedJobParams = ParamHelper.getParamString(paramMap);
		return editedJobParams;
	}

	@Override
	public int whenExecuteFinish() {
		if (!canExecute()) {
			return -1;
		}

		if (null != attachMakeupTask) {
			return whenMakeupExecuteFinish();
		} else {
			return whenNormalExecuteFinish();
		}
	}

	/**
	 * 补偿触发的任务，更新补偿任务状态，添加执行日志
	 *
	 * @return
	 */
	private int whenMakeupExecuteFinish() {
		int row = context.getDefaultTaskDAGStore().updateMakeupTaskFinish(attachMakeupTask.getId());

		if (row > 0) {
			long endTime = System.currentTimeMillis();

			TaskLog taskLog = BeanHelper.convertToTaskLog(context.getCurrentTaskNode(), TriggerScene.MAKE_UP, context.prettyPrintDag(false)
					, MapUtils.isNotEmpty(context.getCurrentUpstreamTaskId2Version()) ? GsonUtils.toJson(context.getCurrentUpstreamTaskId2Version()) : "{}"
					, context.getTriggerParam().getExecutorParams(), editedJobParams,
					context.getStartTime(), endTime);

			row += context.getDefaultTaskDAGStore().insertTaskLog(taskLog);
		}

		log.info("更新补偿任务id:{},taskId:{}为已完成", attachMakeupTask.getId(), attachMakeupTask.getTaskId());
		return row;
	}

	/**
	 * 正常任务，更新版本号，添加执行日志，任务池任务
	 *
	 * @return
	 */
	private int whenNormalExecuteFinish() {
		int updateVersion = context.getCurrentTaskNode().getVersion() + 1;
		String updateUpstreamVersion = MapUtils.isNotEmpty(context.getCurrentUpstreamTaskId2Version()) ?
				GsonUtils.toJson(context.getCurrentUpstreamTaskId2Version()) : "{}";
		int row = context.getDefaultTaskDAGStore().incrTaskVersion(context.getCurrentTaskNode().getId(),
				context.getCurrentTaskNode().getVersion(), updateVersion, updateUpstreamVersion);

		if (row > 0) {
			long endTime = System.currentTimeMillis();

			TaskLog taskLog = BeanHelper.convertToTaskLog(context.getCurrentTaskNode(), TriggerScene.NORMAL, context.prettyPrintDag(false)
					, MapUtils.isNotEmpty(context.getCurrentUpstreamTaskId2Version()) ? GsonUtils.toJson(context.getCurrentUpstreamTaskId2Version()) : "{}"
					, context.getTriggerParam().getExecutorParams(), context.getTriggerParam().getExecutorParams(),
					context.getStartTime(), endTime);

			row += context.getDefaultTaskDAGStore().insertTaskLog(taskLog);

			// 记录到任务池
			TaskPool taskPool = TaskPool.builder()
					.taskId(context.getCurrentTaskNode().getTaskId()).taskGroupId(context.getCurrentTaskNode().getTaskGroupId())
					.upstreamTasks(context.getCurrentTaskNode().getUpstreamTasks()).version(updateVersion)
					.upstreamVersion(updateUpstreamVersion)
					.build();

			row += context.getDefaultTaskDAGStore().insertTaskPool(taskPool);
		}

		log.info("task_id:{}执行任务完成，更新版本号", context.getCurrentTaskNode().getTaskId());
		return row;
	}

	enum CanExecute {
		/**
		 * 待判断
		 */
		TODO,
		/**
		 * 可执行
		 */
		CAN,
		/**
		 * 不可执行
		 */
		CAN_NOT;
	}
}
