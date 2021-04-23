package com.slient.task.orchestration.store;

import com.slient.task.orchestration.consts.MakeupTaskPoolStatusEnum;
import com.slient.task.orchestration.db.*;
import com.slient.task.orchestration.graph.dag.DAG;
import com.slient.task.orchestration.model.*;
import com.slient.task.orchestration.util.GsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author gy
 * @version 1.0
 * @date 2021/2/27.
 * @description:
 */
@Slf4j
@Component
public class DefaultTaskDAGStore {
	@Autowired
	private TaskMetaDao taskMetaDao;

	@Autowired
	private TaskExecuteLogDao taskExecuteLogDao;

	@Autowired
	private MakeupTaskPoolDao makeupTaskPoolDao;

	@Autowired
	private MakeupRecordDao makeupRecordDao;

	@Autowired
	private TaskPoolDao taskPoolDao;

	/**
	 * 任务执行后更新版本
	 *
	 * @param id
	 * @param currentVersion
	 * @param upstreamTaskId2Version
	 * @return
	 */
	public int incrTaskVersion(long id, int currentVersion, int updateVersion, String upstreamTaskId2Version) {
		return taskMetaDao.incrVersion(id, updateVersion, currentVersion, upstreamTaskId2Version);
	}

	/**
	 * 添加执行日志
	 *
	 * @param taskLog
	 * @return
	 * @
	 */
	public int insertTaskLog(TaskLog taskLog) {
		return taskExecuteLogDao.insert(taskLog);
	}

	/**
	 * 新增补偿事件记录
	 *
	 * @param makeupRecord
	 * @return
	 * @
	 */
	public int insertMakeupRecord(MakeupRecord makeupRecord) {
		return makeupRecordDao.insert(makeupRecord);
	}


	/**
	 * 添加补偿任务
	 *
	 * @param makeupTaskPool
	 * @return
	 * @
	 */
	public int insertMakeupPool(MakeupTaskPool makeupTaskPool) {
		return makeupTaskPoolDao.insert(makeupTaskPool);
	}

	/**
	 * 添加任务
	 *
	 * @param taskPool
	 * @return
	 * @
	 */
	public int insertTaskPool(TaskPool taskPool) {
		return taskPoolDao.insert(taskPool);
	}

	public MakeupTaskPool findMakeupTask(String taskId, String taskGroupId) {
		List<MakeupTaskPool> makeupTaskPools = makeupTaskPoolDao.selectValidMakeupTask(taskId, taskGroupId);
		return CollectionUtils.isEmpty(makeupTaskPools) ? null : makeupTaskPools.get(0);
	}

	public MakeupTaskPool findFirstMakeupByMakeupVersion(int makeupVersion) {
		List<MakeupTaskPool> makeupTaskPools = makeupTaskPoolDao.selectFirstTaskByMakeupVersion(makeupVersion);
		return CollectionUtils.isEmpty(makeupTaskPools) ? null : makeupTaskPools.get(0);
	}

	public int updateMakeupTaskFinish(long makeupTaskPoolId) {
		return makeupTaskPoolDao.updateStatus(makeupTaskPoolId, MakeupTaskPoolStatusEnum.FINISH.getCode(), MakeupTaskPoolStatusEnum.TODO.getCode());
	}

	/**
	 * ----------- 加载DAG --------------
	 */
	public Pair<TaskNode, DAG<TaskNode, TaskNodeRelation<TaskNode>>> loadByTaskId(String taskId) {
		List<TaskNode> taskNodes = taskMetaDao.selectByTaskId(taskId);

		if (CollectionUtils.isEmpty(taskNodes)) {
			return null;
		}

		TaskNode currentNode = taskNodes.get(0);
		return ImmutablePair.of(currentNode, loadByTaskGroupId(currentNode.getTaskGroupId()));
	}

	public DAG<TaskNode, TaskNodeRelation<TaskNode>> loadByTaskGroupId(String taskGroupId) {
		DAG<TaskNode, TaskNodeRelation<TaskNode>> dag = new DAG<>();

		List<TaskNode> taskNodes = taskMetaDao.selectByTaskGroupId(taskGroupId);
		checkConstraint(taskNodes);

		List<TaskNode> hasUpstreamTaskNodes = new ArrayList<>();
		Map<String, TaskNode> taskId2TaskNode = new HashMap<>();
		if (CollectionUtils.isNotEmpty(taskNodes)) {
			for (TaskNode taskNode : taskNodes) {
				dag.addNode(taskNode);
				taskId2TaskNode.put(taskNode.getTaskId(), taskNode);

				if (CollectionUtils.isNotEmpty(taskNode.getUpstreamTaskList())) {
					hasUpstreamTaskNodes.add(taskNode);
				}
			}
		}

		if (CollectionUtils.isNotEmpty(hasUpstreamTaskNodes)) {
			for (TaskNode hasUpstreamTaskNode : hasUpstreamTaskNodes) {
				for (String upstreamTaskId : hasUpstreamTaskNode.getUpstreamTaskList()) {
					if (taskId2TaskNode.containsKey(upstreamTaskId)) {
						dag.addEdge(taskId2TaskNode.get(upstreamTaskId), hasUpstreamTaskNode);
					} else {
						log.warn("任务DAG图存在依赖失效, task : {}, upstreamTaskId : {}", GsonUtils.toJson(hasUpstreamTaskNode), upstreamTaskId);
					}
				}
			}
		}

		return dag;
	}

	/**
	 * 检查约束
	 *
	 * @param taskNodes
	 */
	private void checkConstraint(List<TaskNode> taskNodes) {
		// 1. 上游节点必须存在
		Map<String, TaskNode> taskId2TaskNode = taskNodes.stream().collect(Collectors.toMap(TaskNode::getTaskId, Function.identity()));

		for (TaskNode taskNode : taskNodes) {
			if (CollectionUtils.isNotEmpty(taskNode.getUpstreamTaskList())) {
				for (String upstreamTaskId : taskNode.getUpstreamTaskList()) {
					if (!taskId2TaskNode.containsKey(upstreamTaskId)) {
						throw new IllegalArgumentException(String.format("taskId:%s 依赖的上游taskId:%s 不存在", taskNode.getTaskId(), upstreamTaskId));
					}
				}
			}
		}
	}


	/**
	 * -------------------- 保存DAG --------------
	 **/
	public int[] save(DAG<TaskNode, TaskNodeRelation<TaskNode>> dag) {
		Set<TaskNode> nodeSnapshot = dag.nodeSnapshot();

		if (CollectionUtils.isEmpty(nodeSnapshot)) {
			return null;
		}

		/**
		 * 完整性检查
		 */
		Set<String> taskSignSet = new HashSet<>();
		for (TaskNode taskNode : nodeSnapshot) {
			// 1. taskId、taskGroupId、name不允许为空
			if (StringUtils.isAnyBlank(taskNode.getTaskId(), taskNode.getTaskGroupId(), taskNode.getName())) {
				throw new IllegalArgumentException("taskId/taskGroupId/name不能为空");
			}

			// 2. taskId + taskGroupId 不能重复
			String taskSign = taskNode.getNodeSign();
			if (taskSignSet.contains(taskSign)) {
				throw new IllegalArgumentException(String.format("任务标识%s必须唯一", taskSign));
			}
			taskSignSet.add(taskSign);
		}

		/**
		 * 持久化
		 */
		return save(nodeSnapshot);
	}

	/**
	 * 任务DAG图的持久化需要事务
	 */
	private int[] save(Collection<TaskNode> nodes) {
		return taskMetaDao.batchInsert(new ArrayList<>(nodes));
	}
}
