package com.slient.task.orchestration.model;

import com.google.common.base.Splitter;
import com.slient.task.orchestration.graph.dag.Node;
import lombok.*;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * @author gy
 * @version 1.0
 * @date 2021/2/26.
 * @description: 任务节点对应task_meta一条记录
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TaskNode implements Node<String> {
	/**
	 * 任务ID
	 */
	private long id;

	/**
	 * 任务标识
	 */
	private String taskId;

	/**
	 * 任务名
	 */
	private String name;

	/**
	 * 任务描述
	 */
	private String description;

	/**
	 * 任务组ID
	 */
	private String taskGroupId;

	/**
	 * 任务完整标识 taskGroupId + "-" taskId
	 */
	@Setter(AccessLevel.NONE)
	@Getter(AccessLevel.NONE)
	private String taskSign;

	/**
	 * 上游任务ID列表字符串，用","隔开
	 */
	private String upstreamTasks;

	/**
	 * 上游任务ID列表
	 */
	@Setter(AccessLevel.NONE)
	@Getter(AccessLevel.NONE)
	private List<String> upstreamTaskList;

	private int version;

	private int preVersion;

	private String upstreamVersion;

	private String extra;

	private long createTime;

	private long updateTime;

	public TaskNode(String taskId, String taskGroupId) {
		this.taskId = taskId;
		this.taskGroupId = taskGroupId;
		this.taskSign = taskGroupId + "-" + taskId;
	}

	@Override
	public String getNodeSign() {
		if (StringUtils.isEmpty(taskSign)) {
			taskSign = taskGroupId + "-" + taskId;
		}
		return taskSign;
	}

	public List<String> getUpstreamTaskList() {
		if (StringUtils.isEmpty(upstreamTasks)) {
			return Collections.emptyList();
		}

		if (null == upstreamTaskList) {
			upstreamTaskList = Splitter.on(",").splitToList(upstreamTasks);
		}
		return upstreamTaskList;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		TaskNode taskNode = (TaskNode) o;
		return Objects.equals(taskId, taskNode.taskId) &&
				Objects.equals(taskGroupId, taskNode.taskGroupId);
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, taskGroupId);
	}

	@Override
	public String toString() {
		return "TaskNode{" +
				"id=" + id +
				", taskId='" + taskId + '\'' +
				", name='" + name + '\'' +
				", description='" + description + '\'' +
				", taskGroupId='" + taskGroupId + '\'' +
				", taskSign='" + taskSign + '\'' +
				", upstreamTasks='" + upstreamTasks + '\'' +
				", upstreamTaskList=" + upstreamTaskList +
				", version=" + version +
				", preVersion=" + preVersion +
				", upstreamVersion='" + upstreamVersion + '\'' +
				", extra='" + extra + '\'' +
				", createTime=" + createTime +
				", updateTime=" + updateTime +
				'}';
	}
}
