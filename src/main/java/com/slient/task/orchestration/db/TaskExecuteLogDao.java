package com.slient.task.orchestration.db;

import com.slient.task.orchestration.model.TaskLog;
import io.netty.util.internal.StringUtil;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * @author gy
 * @version 1.0
 * @date 2021/3/15.
 * @description:
 */
@Component
public class TaskExecuteLogDao {
	@Autowired
	private JdbcTemplate dagJdbcTemplate;

	public static final String INSERT_SQL = "INSERT INTO `task_execute_log`" +
			"(`dag`, `task_id`, `task_group_id`, `version`, `pre_version`, `upstream_version`, `scene`, " +
			"`xxl_params`, `execute_params`, `start_time`, `end_time`, `cost_time`) " +
			"VALUES " +
			"(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

	public int insert(TaskLog record) {
		if (null == record) {
			return -1;
		}
		return dagJdbcTemplate.update(INSERT_SQL,
				record.getDag(), record.getTaskId(), record.getTaskGroupId(), record.getVersion(),
				record.getPreVersion(), record.getUpstreamVersion(), record.getScene(),
				StringUtils.isBlank(record.getXxlParams()) ? "" : record.getXxlParams(),
				StringUtils.isBlank(record.getExecuteParams()) ? "" : record.getExecuteParams(),
				record.getStartTime(), record.getEndTime(),
				record.getCostTime()
		);
	}
}
