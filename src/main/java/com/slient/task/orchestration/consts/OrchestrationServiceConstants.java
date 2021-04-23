package com.slient.task.orchestration.consts;

import java.util.HashSet;
import java.util.Set;

/**
 * @author gy
 * @version 1.0
 * @date 2021/3/17.
 * @description:
 */
public class OrchestrationServiceConstants {
	/**
	 * 补偿任务可以带到下游任务的参数
	 */
	public static final Set<String> MAKE_UP_PARAM_KEYS = new HashSet<>();

	static {
		MAKE_UP_PARAM_KEYS.add("startDate");
		MAKE_UP_PARAM_KEYS.add("endDate");
	}
}
