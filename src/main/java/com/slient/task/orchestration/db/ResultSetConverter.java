package com.slient.task.orchestration.db;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author gy
 * @version 1.0
 * @date 2021/3/2.
 * @description:
 */
public interface ResultSetConverter<T> {
	/**
	 * 将数据库记录转成java bean
	 *
	 * @param resultSet
	 * @return
	 * @throws SQLException
	 */
	T convert(ResultSet resultSet) throws SQLException;
}
