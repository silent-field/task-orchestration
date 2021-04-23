package com.slient.task.orchestration.db;

import com.slient.task.orchestration.model.MakeupRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author gy
 * @version 1.0
 * @date 2021/3/15.
 * @description: makeup_record 表 dao
 */
@Component
public class MakeupRecordDao {
	@Autowired
	private JdbcTemplate dagJdbcTemplate;

	public static final String INSERT_SQL = "INSERT INTO `makeup_record`" +
			"(`task_id`, `task_group_id`, `downstream_makeup_tasks`, `makeup_params`)" +
			" VALUES " +
			"(?, ?, ?, ?)";

	public int insert(MakeupRecord record) {
		if (null == record) {
			return -1;
		}

		KeyHolder keyHolder = new GeneratedKeyHolder();
		dagJdbcTemplate.update(new PreparedStatementCreator() {
			@Override
			public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
				PreparedStatement ps = connection.prepareStatement(INSERT_SQL, PreparedStatement.RETURN_GENERATED_KEYS);
				ps.setString(1, record.getTaskId());
				ps.setString(2, record.getTaskGroupId());
				ps.setString(3, record.getDownstreamTasks());
				ps.setString(4, record.getMakeupParams());

				return ps;
			}
		}, keyHolder);

		return keyHolder.getKey().intValue();
	}
}
