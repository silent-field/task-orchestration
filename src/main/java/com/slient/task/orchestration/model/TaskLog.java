package com.slient.task.orchestration.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author gy
 * @version 1.0
 * @date 2021/2/26.
 * @description: 任务操作日志对应task_execute_log表的一条记录
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TaskLog {
	/**
	 * 任务ID
	 */
	private long id;

	/**
	 * dag图
	 */
	private String dag;

	/**
	 * 任务标识
	 */
	private String taskId;

	/**
	 * 任务组ID
	 */
	private String taskGroupId;

	/**
	 * 当前任务执行版本
	 */
	private int version;

	/**
	 * 任务上一个执行版本
	 */
	private int preVersion;

	/**
	 * 上游任务标识以及触发任务时上游版本集合
	 */
	private String upstreamVersion;

	/**
	 * 任务触发场景
	 */
	private int scene;

	/**
	 * xxl参数
	 */
	private String xxlParams;

	/**
	 * 执行的实际参数
	 */
	private String executeParams;
	
	/**
	 * 任务执行开始时间
	 */
	private long startTime;

	/**
	 * 任务执行完成时间
	 */
	private long endTime;

	/**
	 * 耗时
	 */
	private long costTime;
}
