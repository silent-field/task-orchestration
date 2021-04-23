package com.slient.task.orchestration.db;

import com.slient.task.orchestration.model.TaskPool;
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
public class TaskPoolDao {
	@Autowired
	private JdbcTemplate dagJdbcTemplate;

	private static final String INSERT_SQL = "INSERT INTO `task_pool`" +
			"(`task_id`, `task_group_id`, `upstream_tasks`, `version`, `upstream_version`) " +
			"VALUES " +
			"(?, ?, ?, ?, ?);";

	public int insert(TaskPool record) {
		if (null == record) {
			return -1;
		}
		return dagJdbcTemplate.update(INSERT_SQL,
				record.getTaskId(), record.getTaskGroupId(), record.getUpstreamTasks(), record.getVersion(), record.getUpstreamVersion()
		);
	}
}
