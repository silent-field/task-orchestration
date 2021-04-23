package com.slient.task.orchestration.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author gy
 * @version 1.0
 * @date 2021/3/12.
 * @description:
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TaskPool {
	private int id;

	/**
	 * 任务标识
	 */
	private String taskId;

	/**
	 * 任务组ID
	 */
	private String taskGroupId;

	/**
	 * 上游任务ID列表字符串，用","隔开
	 */
	private String upstreamTasks;

	/**
	 * 当前任务执行版本
	 */
	private int version;

	/**
	 * 上游任务标识以及触发任务时上游版本集合
	 */
	private String upstreamVersion;

	private long createTime;

	private long updateTime;
}
