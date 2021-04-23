package com.slient.task.orchestration.db;

import com.google.common.collect.ImmutableList;
import com.slient.task.orchestration.model.TaskNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.beans.PropertyDescriptor;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * @author gy
 * @version 1.0
 * @date 2021/3/15.
 * @description:
 */
@Component
public class TaskMetaDao {
	@Autowired
	private JdbcTemplate dagJdbcTemplate;

	private static final String INSERT_SQL = "INSERT INTO `task_meta`" +
			"(`task_id`, `name`, `description`, `task_group_id`, `upstream_tasks`, `version`, `pre_version`," +
			" `upstream_version`, `extra`) " +
			" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

	private static final String SELECT_BY_TASK_ID = "SELECT * from `task_meta` where `task_id` = ? limit 1";

	private static final String SELECT_BY_TASK_GROUP_ID = "SELECT * from `task_meta` where `task_group_id` = ?";

	private static final String UPDATE_VERSION = "UPDATE `task_meta` SET " +
			" `version` = ?, `pre_version` = ?,`upstream_version` = ? " +
			" WHERE `id` = ? and `version` = ?";

	private static final List<String> DATE_NAME_LIST = ImmutableList.of("createTime", "updateTime");

	private static final BeanPropertyRowMapper<TaskNode> TASK_NODE_BEAN_PROPERTY_ROW_MAPPER = new BeanPropertyRowMapper<TaskNode>(TaskNode.class) {
		@Override
		protected Object getColumnValue(ResultSet rs, int index, PropertyDescriptor pd) throws SQLException {
			if (DATE_NAME_LIST.contains(pd.getName())) {
				return rs.getTimestamp(index).getTime();
			}
			return super.getColumnValue(rs, index, pd);
		}
	};


	public int insert(TaskNode record) {
		if (null == record) {
			return -1;
		}
		return dagJdbcTemplate.update(INSERT_SQL,
				record.getTaskId(), record.getName(), record.getDescription(), record.getTaskGroupId(),
				record.getUpstreamTasks(), record.getVersion(), record.getPreVersion(),
				record.getUpstreamVersion(), record.getExtra()
		);
	}

	public int[] batchInsert(List<TaskNode> records) {
		return dagJdbcTemplate.batchUpdate(INSERT_SQL, new BatchPreparedStatementSetter() {
			@Override
			public void setValues(PreparedStatement ps, int i) throws SQLException {
				ps.setString(1, records.get(i).getTaskId());
				ps.setString(2, records.get(i).getName());
				ps.setString(3, records.get(i).getDescription());
				ps.setString(4, records.get(i).getTaskGroupId());
				ps.setString(5, records.get(i).getUpstreamTasks());
				ps.setInt(6, records.get(i).getVersion());
				ps.setInt(7, records.get(i).getPreVersion());
				ps.setString(8, records.get(i).getUpstreamVersion());
				ps.setString(9, records.get(i).getExtra());
			}

			@Override
			public int getBatchSize() {
				return records.size();
			}
		});
	}

	public List<TaskNode> selectByTaskId(String taskId) {
		return dagJdbcTemplate.query(SELECT_BY_TASK_ID, new Object[]{taskId}, TASK_NODE_BEAN_PROPERTY_ROW_MAPPER);
	}

	public List<TaskNode> selectByTaskGroupId(String taskGroupId) {
		return dagJdbcTemplate.query(SELECT_BY_TASK_GROUP_ID, new Object[]{taskGroupId}, TASK_NODE_BEAN_PROPERTY_ROW_MAPPER);
	}

	public int incrVersion(long id, int version, int preVersion, String upstreamVersion) {
		return dagJdbcTemplate.update(UPDATE_VERSION, new Object[]{version, preVersion, upstreamVersion, id, preVersion});
	}
}
