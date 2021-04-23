package com.slient.task.orchestration.model;

import com.slient.task.orchestration.graph.dag.Edge;

import java.util.Objects;

/**
 * @author gy
 * @version 1.0
 * @date 2021/2/26.
 * @description: 任务节点关系
 */
public class TaskNodeRelation<T extends TaskNode> implements Edge<TaskNode> {
	private T sourceNode;

	private T targetNode;

	public TaskNodeRelation(T sourceNode, T targetNode) {
		if (null == sourceNode || null == targetNode) {
			throw new IllegalArgumentException("sourceNode or targetNode can not be null!");
		}

		this.sourceNode = sourceNode;
		this.targetNode = targetNode;
	}

	@Override
	public T getSourceNode() {
		return sourceNode;
	}

	@Override
	public T getTargetNode() {
		return targetNode;
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof TaskNodeRelation)) {
			return false;
		}
		TaskNodeRelation relation = (TaskNodeRelation) o;
		return (relation.getSourceNode().equals(this.sourceNode) && relation.getTargetNode().equals(this.targetNode));
	}

	@Override
	public String toString() {
		return "TaskNodeRelation{" +
				"startNode='" + sourceNode + '\'' +
				", endNode='" + targetNode + '\'' +
				'}';
	}

	@Override
	public int hashCode() {
		return Objects.hash(sourceNode, targetNode);
	}
}
