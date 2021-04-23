package com.slient.task.orchestration.factory;

import com.slient.task.orchestration.graph.dag.DAG;
import com.slient.task.orchestration.model.TaskNode;
import com.slient.task.orchestration.model.TaskNodeRelation;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Set;

/**
 * @author gy
 * @version 1.0
 * @date 2021/2/27.
 * @description: 工厂类，用于构造任务DAG图
 */
@Slf4j
public abstract class AbstractTaskDAGFactory<T extends TaskNode, R extends TaskNodeRelation<T>> {
	/**
	 * 使用任务组ID构造DAG图
	 *
	 * @param taskGroupId
	 * @return
	 */
	public DAG<T, R> constructTaskDAG(String taskGroupId) {
		if (StringUtils.isEmpty(taskGroupId)) {
			log.error("taskGroupId is null");
			return null;
		}

		Pair<Set<T>, Set<R>> pair = queryByTaskGroupId(taskGroupId);

		if (CollectionUtils.isEmpty(pair.getLeft())) {
			// 任务节点集不允许为空，极端情况下DAG节点的入度可以都为0
			log.error("taskGroupId:{}下没有任务节点", taskGroupId);
			return null;
		}

		if (CollectionUtils.isNotEmpty(pair.getRight())) {
			for (R relation : pair.getRight()) {
				if (null == relation.getSourceNode() || null == relation.getTargetNode()) {
					// TaskNodeRelation的源节点跟目标节点都不允许为空
					log.error("TaskNodeRelation:{}的源节点或目标节点为空", relation);
					return null;
				}
			}
		}

		DAG<T, R> dag = new DAG<>();

		for (T taskNode : pair.getLeft()) {
			dag.addNode(taskNode);
		}

		for (R taskNodeRelation : pair.getRight()) {
			dag.addEdge(taskNodeRelation.getSourceNode(), taskNodeRelation.getTargetNode(), taskNodeRelation, false);
		}

		return dag;
	}

	/**
	 * 通过任务组ID得到任务节点与任务关系
	 *
	 * @param taskGroupId
	 * @return
	 */
	protected abstract Pair<Set<T>, Set<R>> queryByTaskGroupId(String taskGroupId);
}
