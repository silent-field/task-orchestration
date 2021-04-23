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
public class MakeupTaskPool {
	private int id;

	private int makeupVersion;

	/**
	 * 任务标识
	 */
	private String taskId;

	/**
	 * 任务组ID
	 */
	private String taskGroupId;

	/**
	 * 触发的上游任务task id
	 */
	private String upstreamTask;

	/**
	 * 状态
	 */
	private int status;

	/**
	 * 补偿参数
	 */
	private String makeupParams;

	private long createTime;

	private long updateTime;
}
