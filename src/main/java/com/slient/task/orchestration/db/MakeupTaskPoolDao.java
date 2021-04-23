package com.slient.task.orchestration.db;

import com.google.common.collect.ImmutableList;
import com.slient.task.orchestration.model.MakeupTaskPool;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.beans.PropertyDescriptor;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * @author gy
 * @version 1.0
 * @date 2021/3/15.
 * @description: makeup_task_pool 表 dao
 */
@Component
public class MakeupTaskPoolDao {
	@Autowired
	private JdbcTemplate dagJdbcTemplate;

	public static final String INSERT_SQL = "INSERT INTO `makeup_task_pool`" +
			"(`makeup_version`,`task_id`, `task_group_id`, `status`, `makeup_params`)" +
			" VALUES " +
			"(?, ?, ?, ?, ?)";

	private static final String SELECT_BY_TASK_ID = "SELECT * from `makeup_task_pool` where `task_id` = ? and `task_group_id` = ? and `status` = 1 order by create_time limit 1";

	private static final String SELECT_FIRST_TASK_BY_MAKEUP_VERSION = "SELECT * from `makeup_task_pool` where `makeup_version` = ? and `status` = 1 order by create_time limit 1";

	private static final String UPDATE_STATUS = "UPDATE `makeup_task_pool` SET " +
			" `status` = ? " +
			" WHERE `id` = ? and `status` = ?";

	private static final List<String> DATE_NAME_LIST = ImmutableList.of("createTime", "updateTime");

	private static final BeanPropertyRowMapper<MakeupTaskPool> MAKEUP_TASK_POOL_BEAN_PROPERTY_ROW_MAPPER =
			new BeanPropertyRowMapper<MakeupTaskPool>(MakeupTaskPool.class) {
				@Override
				protected Object getColumnValue(ResultSet rs, int index, PropertyDescriptor pd) throws SQLException {
					if (DATE_NAME_LIST.contains(pd.getName())) {
						return rs.getTimestamp(index).getTime();
					}
					return super.getColumnValue(rs, index, pd);
				}
			};

	public int insert(MakeupTaskPool record) {
		if (null == record) {
			return -1;
		}
		return dagJdbcTemplate.update(INSERT_SQL,
				record.getMakeupVersion(), record.getTaskId(), record.getTaskGroupId(), record.getStatus(), record.getMakeupParams()
		);
	}

	public List<MakeupTaskPool> selectValidMakeupTask(String taskId, String taskGroupId) {
		return dagJdbcTemplate.query(SELECT_BY_TASK_ID, new Object[]{taskId, taskGroupId}, MAKEUP_TASK_POOL_BEAN_PROPERTY_ROW_MAPPER);
	}

	public List<MakeupTaskPool> selectFirstTaskByMakeupVersion(int makeupVersion) {
		return dagJdbcTemplate.query(SELECT_FIRST_TASK_BY_MAKEUP_VERSION,
				new Object[]{makeupVersion}, MAKEUP_TASK_POOL_BEAN_PROPERTY_ROW_MAPPER);
	}

	public int updateStatus(long id, int status, int preStatus) {
		return dagJdbcTemplate.update(UPDATE_STATUS, new Object[]{status, id, preStatus});
	}
}
