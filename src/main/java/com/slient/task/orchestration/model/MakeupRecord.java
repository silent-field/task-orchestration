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
public class MakeupRecord {
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
	private String downstreamTasks;

	/**
	 * 补偿参数
	 */
	private String makeupParams;

	private long createTime;

	private long updateTime;
}
