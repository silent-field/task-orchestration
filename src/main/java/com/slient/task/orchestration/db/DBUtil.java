package com.slient.task.orchestration.db;

import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * @author gy
 */
@Slf4j
public class DBUtil {
	private DBUtil(){}
	
	public static int update(Connection conn, String tableName, ColumnValueHolder setColumnValueHolder, ColumnValueHolder whereColumnValueHolder) {
		if (null == conn || StringUtils.isEmpty(tableName)
				|| null == setColumnValueHolder || CollectionUtils.isEmpty(setColumnValueHolder.getColumnValues())
				|| null == whereColumnValueHolder || CollectionUtils.isEmpty(whereColumnValueHolder.getColumnValues())) {
			log.error("Connection / tableName / setColumnValueHolder / whereColumnValueHolder 不能为空");
			return -1;
		}

		PreparedStatement preparedStatement = null;

		try {
			/**更新字段*/
			StringBuilder setParamSql = new StringBuilder();
			for (int i = 0; i < setColumnValueHolder.getColumnValues().size(); i++) {
				setParamSql.append("`").append(setColumnValueHolder.getColumnValues().get(i).getColumnName()).append("` = ?");
				if (i != setColumnValueHolder.getColumnValues().size() - 1) {
					setParamSql.append(", ");
				}
			}

			/**where条件*/
			StringBuilder whereParamSql = new StringBuilder();
			for (int j = 0; j < whereColumnValueHolder.getColumnValues().size(); j++) {
				if (j > 0) {
					whereParamSql.append(" and ");
				}
				whereParamSql.append("`").append(whereColumnValueHolder.getColumnValues().get(j).getColumnName()).append("` = ?");
			}

			/** 拼接更新sql语句 **/
			StringBuilder updateSql = new StringBuilder();
			updateSql.append("UPDATE `" + tableName + "`");
			updateSql.append(" set ");
			updateSql.append(setParamSql.toString());
			updateSql.append(" where ");
			updateSql.append(whereParamSql.toString());

			preparedStatement = conn.prepareStatement(updateSql.toString());

			int placeholderIndex = 1;
			for (ColumnValue setColumnValue : setColumnValueHolder.getColumnValues()) {
				preparedStatement.setObject(placeholderIndex, setColumnValue.getColumnValue());
				placeholderIndex++;
			}

			for (ColumnValue whereColumnValue : whereColumnValueHolder.getColumnValues()) {
				preparedStatement.setObject(placeholderIndex, whereColumnValue.getColumnValue());
				placeholderIndex++;
			}

			return preparedStatement.executeUpdate();
		} catch (Exception e) {
			log.error("Exception ", e);
		} finally {
			releaseResource(preparedStatement, conn);
		}

		return -1;
	}

	/**
	 * 查询DB并将结果集转换为T的集合
	 *
	 * @param conn
	 * @param tableName
	 * @param columnValueHolder
	 * @param converter
	 * @param <T>
	 * @return
	 */
	public static <T> List<T> query(Connection conn, String tableName, ColumnValueHolder columnValueHolder, ResultSetConverter<T> converter) {
		List<T> recordList = new ArrayList<>();

		Map<String, Object> columnValueMap = columnValueHolder.getColumnValueMap();
		if (null == conn || StringUtils.isEmpty(tableName) || MapUtils.isEmpty(columnValueMap) || null == converter) {
			log.error("Connection / selectSql / columnValueMap / converter 不能为空");
			return recordList;
		}

		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		try {
			/**拼接插入的sql语句**/
			StringBuilder selectSql = new StringBuilder();
			selectSql.append("SELECT * FROM `");
			selectSql.append(tableName + "` where ");

			int i = 0;
			List<Object> whereParamValues = new ArrayList<>();
			StringBuilder whereSql = new StringBuilder();
			for (Map.Entry<String, Object> columnValue : columnValueMap.entrySet()) {
				whereSql.append("`" + columnValue.getKey() + "` = ?");
				whereSql.append(i == columnValueMap.size() - 1 ? "" : ", ");

				whereParamValues.add(columnValue.getValue());
				i++;
			}

			selectSql.append(whereSql);

			preparedStatement = conn.prepareStatement(selectSql.toString());

			for (int j = 0; j < i; j++) {
				preparedStatement.setObject(j + 1, whereParamValues.get(j));
			}

			rs = preparedStatement.executeQuery();

			while (rs.next()) {
				T record = converter.convert(rs);
				recordList.add(record);
			}
		} catch (SQLException e) {
			log.error("Exception ", e);
		} finally {
			releaseResource(rs, preparedStatement, conn);
		}
		return recordList;
	}


	/**
	 * 批量插入
	 *
	 * @param tableName          表名
	 * @param columnValueHolders 字段-值映射关系列表
	 * @return
	 * @throws SQLException
	 */
	public static int batchInsert(Connection conn, String tableName, List<ColumnValueHolder> columnValueHolders) throws SQLException {
		if (null == conn || StringUtils.isEmpty(tableName) || CollectionUtils.isEmpty(columnValueHolders)) {
			log.error("Connection / tableName / columnValueHolders 不能为空");
			return -1;
		}

		/**影响的行数**/
		int affectRowCount;
		PreparedStatement preparedStatement = null;
		try {
			/**设置不自动提交，以便于在出现异常的时候数据库回滚**/
			conn.setAutoCommit(false);

			Map<String, Object> columnValueMap = columnValueHolders.get(0).getColumnValueMap();

			InsertSqlParts insertSqlParts = analyzeInsertParams(columnValueMap, null);

			Object[] keys = insertSqlParts.getKeys();
			/**拼接插入的sql语句**/
			StringBuilder sql = new StringBuilder();
			sql.append("INSERT INTO `");
			sql.append(tableName + "`");
			sql.append(" (");
			sql.append(insertSqlParts.getColumnSql());
			sql.append(" )  VALUES (");
			sql.append(insertSqlParts.getUnknownMarkSql());
			sql.append(" )");

			/**执行SQL预编译**/
			preparedStatement = conn.prepareStatement(sql.toString());
			log.info(sql.toString());
			for (int j = 0; j < columnValueHolders.size(); j++) {
				Map<String, Object> toInsertColumnValueMap = columnValueHolders.get(j).getColumnValueMap();
				int placeholderIndex = 1;
				for (int k = 0; k < keys.length; k++) {
					preparedStatement.setObject(placeholderIndex, toInsertColumnValueMap.get(keys[k]));
					placeholderIndex++;
				}
				preparedStatement.addBatch();
			}
			int[] arr = preparedStatement.executeBatch();
			conn.commit();
			affectRowCount = arr.length;
			log.info("成功了插入了{}行", affectRowCount);
		} catch (Exception e) {
			if (conn != null) {
				conn.rollback();
			}
			log.error("batchInsert",e);
			throw e;
		} finally {
			releaseResource(preparedStatement, conn);
		}
		return affectRowCount;
	}

	/**
	 * 批量on duplicate key update插入
	 *
	 * @param tableName          表名
	 * @param columnValueHolders 字段-值映射关系列表
	 * @return
	 * @throws SQLException
	 */
	public static int batchInsertOnDuplicateUpdate(
			Connection conn, String tableName,
			List<ColumnValueHolder> columnValueHolders,
			Set<String> onDuplicateUpdateKeySet) throws SQLException {
		if (null == conn || StringUtils.isEmpty(tableName) ||
				CollectionUtils.isEmpty(columnValueHolders) || CollectionUtils.isEmpty(onDuplicateUpdateKeySet)) {
			log.error("Connection / tableName / columnValueHolders / onDuplicateUpdateKeys 不能为空");
			return -1;
		}

		/**影响的行数**/
		int affectRowCount;
		PreparedStatement preparedStatement = null;
		try {
			/**设置不自动提交，以便于在出现异常的时候数据库回滚**/
			conn.setAutoCommit(false);

			Map<String, Object> columnValueMap = columnValueHolders.get(0).getColumnValueMap();

			InsertSqlParts insertSqlParts = analyzeInsertParams(columnValueMap, onDuplicateUpdateKeySet);

			String[] keys = insertSqlParts.getKeys();
			String[] onDuplicateUpdateKeys = insertSqlParts.getOnDuplicateUpdateKeys();

			/**拼接插入的sql语句**/
			StringBuilder sql = new StringBuilder();
			sql.append("INSERT INTO `");
			sql.append(tableName + "`");
			sql.append(" (");
			sql.append(insertSqlParts.getColumnSql());
			sql.append(" )  VALUES (");
			sql.append(insertSqlParts.getUnknownMarkSql());
			sql.append(" ) ");
			sql.append(" ON DUPLICATE KEY UPDATE ");
			sql.append(insertSqlParts.getOnDuplicateUpdateSql());

			/**执行SQL预编译**/
			preparedStatement = conn.prepareStatement(sql.toString());

			log.info(sql.toString());

			for (int j = 0; j < columnValueHolders.size(); j++) {
				Map<String, Object> toInsertColumnValueMap = columnValueHolders.get(j).getColumnValueMap();
				int placeholderIndex = 1;
				for (int k = 0; k < keys.length; k++) {
					preparedStatement.setObject(placeholderIndex, toInsertColumnValueMap.get(keys[k]));
					placeholderIndex++;
				}

				for (int x = 0; x < onDuplicateUpdateKeys.length; x++) {
					preparedStatement.setObject(placeholderIndex, toInsertColumnValueMap.get(onDuplicateUpdateKeys[x]));
					placeholderIndex++;
				}
				preparedStatement.addBatch();
			}
			int[] arr = preparedStatement.executeBatch();
			conn.commit();
			affectRowCount = arr.length;
			log.info("成功batchInsertOnDuplicateUpdate了{}行", affectRowCount);
		} catch (Exception e) {
			if (conn != null) {
				conn.rollback();
			}
			log.error("batchInsertOnDuplicateUpdate", e);
			throw e;
		} finally {
			releaseResource(preparedStatement, conn);
		}
		return affectRowCount;
	}

	/**
	 * 分析insert/insert on duplicate key update 语句需要的信息
	 *
	 * @param columnValueMap
	 * @param onDuplicateUpdateKeySet
	 * @return
	 */
	protected static InsertSqlParts analyzeInsertParams(
			Map<String, Object> columnValueMap, Set<String> onDuplicateUpdateKeySet) {
		/**需要插入列名**/
		Set<String> keySet = columnValueMap.keySet();
		Iterator<String> iterator = keySet.iterator();
		/**要插入的字段sql，其实就是用key拼起来的**/
		StringBuilder columnSql = new StringBuilder();
		/**要插入的字段值，其实就是？**/
		StringBuilder unknownMarkSql = new StringBuilder();

		String[] keys = new String[columnValueMap.size()];
		int i = 0;
		while (iterator.hasNext()) {
			String key = iterator.next();
			keys[i] = key;
			columnSql.append(i == 0 ? "`" : ",`");
			columnSql.append(key).append("`");

			unknownMarkSql.append(i == 0 ? "" : ",");
			unknownMarkSql.append("?");
			i++;
		}

		StringBuilder onDuplicateUpdateSql = null;
		String[] onDuplicateUpdateKeys = null;

		if (CollectionUtils.isNotEmpty(onDuplicateUpdateKeySet)) {
			onDuplicateUpdateSql = new StringBuilder();
			onDuplicateUpdateKeys = new String[onDuplicateUpdateKeySet.size()];

			Iterator<String> onDuplicateUpdateKeysIt = onDuplicateUpdateKeySet.iterator();
			int onDuplicateUpdateKeysSize = onDuplicateUpdateKeySet.size();
			int j = 0;
			while (onDuplicateUpdateKeysIt.hasNext()) {
				String onDuplicateUpdateKey = onDuplicateUpdateKeysIt.next();
				if (!columnValueMap.containsKey(onDuplicateUpdateKey)) {
					log.error("onDuplicateUpdateKey {} 不存在对应的字段值", onDuplicateUpdateKey);
				}

				onDuplicateUpdateSql.append("`" + onDuplicateUpdateKey + "`");
				onDuplicateUpdateSql.append(" = ?");
				onDuplicateUpdateSql.append(j == onDuplicateUpdateKeysSize - 1 ? "" : ",");

				onDuplicateUpdateKeys[j] = onDuplicateUpdateKey;
				j++;
			}
		}

		return new InsertSqlParts(keys, columnSql, unknownMarkSql, onDuplicateUpdateKeys, onDuplicateUpdateSql);
	}

	/**
	 * 释放资源
	 *
	 * @param resources
	 */
	public static void releaseResource(AutoCloseable... resources) {

		if (resources == null || resources.length == 0) {
			return;
		}
		Arrays.stream(resources).filter(Objects::nonNull)
				.forEach(resource -> {
					try {
						resource.close();
					} catch (Exception e) {
						log.error(e.getMessage(), e);
					}
				});
	}

	/**
	 * insert / insert on duplicate key update 需要的信息类
	 */
	@Data
	protected static class InsertSqlParts {
		public InsertSqlParts(String[] keys, StringBuilder columnSql, StringBuilder unknownMarkSql, String[] onDuplicateUpdateKeys, StringBuilder onDuplicateUpdateSql) {
			this.keys = keys;
			this.columnSql = columnSql;
			this.unknownMarkSql = unknownMarkSql;
			this.onDuplicateUpdateKeys = onDuplicateUpdateKeys;
			this.onDuplicateUpdateSql = onDuplicateUpdateSql;
		}

		private String[] keys;
		private StringBuilder columnSql;
		private StringBuilder unknownMarkSql;
		private String[] onDuplicateUpdateKeys;
		private StringBuilder onDuplicateUpdateSql;
	}

	public static class ColumnValueHolder {
		private List<ColumnValue> columnValues = new ArrayList<>();
		private Map<String, Object> columnValueMap = new HashMap<>();

		public ColumnValueHolder append(String columnName, Object columnValue) {
			if (StringUtils.isEmpty(columnName) || null == columnValue) {
				throw new IllegalArgumentException("columnName或者columnValue为空");
			}

			columnValues.add(new ColumnValue(columnName, columnValue));
			columnValueMap.put(columnName, columnValue);
			return this;
		}

		public List<ColumnValue> getColumnValues() {
			return columnValues;
		}

		public Map<String, Object> getColumnValueMap() {
			return columnValueMap;
		}
	}


	public static class ColumnValue {
		@Getter
		private String columnName;
		@Getter
		private Object columnValue;

		public ColumnValue(String columnName, Object columnValue) {
			this.columnName = columnName;
			this.columnValue = columnValue;
		}
	}
}