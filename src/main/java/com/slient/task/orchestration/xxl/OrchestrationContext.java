package com.slient.task.orchestration.xxl;

import com.google.gson.reflect.TypeToken;
import com.slient.task.orchestration.graph.dag.DAG;
import com.slient.task.orchestration.model.MakeupTaskPool;
import com.slient.task.orchestration.model.TaskNode;
import com.slient.task.orchestration.model.TaskNodeRelation;
import com.slient.task.orchestration.params.ParamHelper;
import com.slient.task.orchestration.store.DefaultTaskDAGStore;
import com.slient.task.orchestration.util.GsonUtils;
import com.xxl.job.core.biz.model.TriggerParam;
import com.xxl.job.core.context.XxlJobContext;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

/**
 * @author gy
 * @version 1.0
 * @date 2021/3/16.
 * @description:
 */
@Slf4j
@Data
public class OrchestrationContext {
	private DefaultTaskDAGStore defaultTaskDAGStore;

	/**
	 * xxl job id，对应task_meta表task_id字段
	 */
	private String xxlJobId;

	/**
	 * xxlJobId 对应的 task_meta 实例
	 */
	private TaskNode currentTaskNode;
	/**
	 * xxlJobId 对应的dag图
	 */
	private DAG<TaskNode, TaskNodeRelation<TaskNode>> dag;

	/**
	 * 任务xxlJobId的上游节点
	 */
	private Set<TaskNode> upstreams;
	/**
	 * 上游节点id集合
	 */
	private List<String> upstreamTaskIdList;
	/**
	 * xxlJobId 上一轮执行时上游节点版本
	 */
	private Map<String, Integer> preUpstreamTaskId2Version = new HashMap<>();
	/**
	 * xxlJobId 上游节点当前版本号
	 */
	private Map<String, Integer> currentUpstreamTaskId2Version = new HashMap<>();
	/**
	 * 任务执行开始时间
	 */
	private long startTime;

	private TriggerParam triggerParam;

	private TriggerScene triggerScene = TriggerScene.NORMAL;

	private static final String TRIGGER_TYPE_KEY = "triggerType";

	public OrchestrationContext(String xxlJobId, DefaultTaskDAGStore defaultTaskDAGStore) {
		this.xxlJobId = xxlJobId;
		this.defaultTaskDAGStore = defaultTaskDAGStore;

		init();
	}

	void init() {
		String xxlJobParam = XxlJobContext.getXxlJobContext().getJobParam();
		if (StringUtils.isNotBlank(xxlJobParam)) {
			triggerParam = GsonUtils.fromJson(xxlJobParam, TriggerParam.class);
		}


		Map<String, String> paramMap = ParamHelper.getParamMap(triggerParam.getExecutorParams());

		// 根据入参判断触发类型
		if (paramMap.containsKey(TRIGGER_TYPE_KEY)) {
			TriggerScene temp = TriggerScene.getByCode(paramMap.get(TRIGGER_TYPE_KEY));
			if (null != temp) {
				triggerScene = temp;
			}
		}

		log.info("triggerParam:{}, 触发类型：{}", triggerParam, triggerScene.getDesc());

		// 通过task_id找寻对应的dag图
		Pair<TaskNode, DAG<TaskNode, TaskNodeRelation<TaskNode>>> pair = defaultTaskDAGStore.loadByTaskId(xxlJobId);

		if (null == pair || null == pair.getLeft() || null == pair.getRight()) {
			log.error("无法找到xxlJobId:{}对应的task_meta配置", xxlJobId);
			return;
		}

		currentTaskNode = pair.getLeft();
		dag = pair.getRight();

		upstreams = dag.getUpstreamNodes(currentTaskNode);

		String upstreamVersion = currentTaskNode.getUpstreamVersion();
		upstreamTaskIdList = currentTaskNode.getUpstreamTaskList();

		if (CollectionUtils.isNotEmpty(upstreams)) {
			/** 上一轮执行时候，依赖的上游版本 */
			if (StringUtils.isBlank(upstreamVersion)) {
				for (String upstreamTaskId : upstreamTaskIdList) {
					preUpstreamTaskId2Version.put(upstreamTaskId, 0);
				}
			} else {
				preUpstreamTaskId2Version = GsonUtils.fromJson(upstreamVersion, new TypeToken<Map<String, Integer>>() {
				}.getType());
			}

			/** 依赖的上游任务当前的版本号 */
			for (TaskNode upstreamTaskNode : upstreams) {
				currentUpstreamTaskId2Version.put(upstreamTaskNode.getTaskId(), upstreamTaskNode.getVersion());
			}
		}

		startTime = System.currentTimeMillis();
		log.info("XxlJobOrchestrationHelper初始化完成。startTime:{}," +
						"XxlJobId:{},TaskNode:{},upstreamTaskId:{},preUpstreamTaskId2Version:{},currentUpstreamTaskId2Version:{}"
				, startTime, xxlJobId, currentTaskNode, upstreamTaskIdList, preUpstreamTaskId2Version, currentUpstreamTaskId2Version);
	}

	/**
	 * 构造dag简易依赖
	 *
	 * @param withTaskGroupId
	 * @return
	 */
	public String prettyPrintDag(boolean withTaskGroupId) {
		Set<TaskNode> nodes = dag.snapshot().getLeft();
		List<Map<String, Object>> result = new ArrayList<>(nodes.size());
		for (TaskNode node : nodes) {
			Map<String, Object> single = new HashMap<>();
			single.put("taskId", node.getTaskId());
			if (withTaskGroupId) {
				single.put("taskGroupId", node.getTaskGroupId());
			}
			single.put("upstreamTasks", node.getUpstreamTasks());

			result.add(single);
		}
		return GsonUtils.toJson(result);
	}

	public List<TaskNode> getAllDownstreamTaskNodes() {
		return dag.getAllDownstreamNodes(currentTaskNode);
	}

	public MakeupTaskPool findMakeupTask() {
		// 如果存在补偿任务，还得判断相同补偿版本号前面的任务是否已执行
		MakeupTaskPool toCheck = defaultTaskDAGStore.findMakeupTask(currentTaskNode.getTaskId(), currentTaskNode.getTaskGroupId());

		if (null != toCheck) {
			log.info("存在补偿任务(待检查)，id:{}", toCheck.getId());

			MakeupTaskPool compare = defaultTaskDAGStore.findFirstMakeupByMakeupVersion(toCheck.getMakeupVersion());

			if (compare != null && toCheck.getId() == compare.getId()) {
				return toCheck;
			} else {
				log.info("存在补偿任务id:{}，不是第一个补偿任务，第一个补偿任务id：{}", toCheck.getTaskId(), compare == null ? "null" : compare.getTaskId());
			}

			return null;
		} else {
			log.info("不存在补偿任务");
			return null;
		}
	}
}
