package com.slient.task.orchestration.config;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

/**
 * @author gy
 * @version 1.0
 * @date 2021/3/10.
 * @description:
 */
@Configuration
public class JdbcTemplateAutoConfig {
	@Bean(name = "dagDataSource")
	public DataSource dagDataSource() {
		DataSourceBuilder<HikariDataSource> builder = DataSourceBuilder.create().type(HikariDataSource.class);
		builder.driverClassName(System.getProperty("dag_jdbc_driverClassName", "com.mysql.cj.jdbc.Driver"));
		builder.username(System.getProperty("dag_jdbc_username"));
		builder.password(System.getProperty("dag_jdbc_password"));
		builder.url(System.getProperty("dag_jdbc_url"));
		HikariDataSource dataSource = builder.build();
		dataSource.setConnectionTimeout(Long.valueOf(System.getProperty("jdbc_connect_timeout", "15000")));
		dataSource.setIdleTimeout(Long.valueOf(System.getProperty("jdbc_idle_timeout", "30000")));
		dataSource.setMaximumPoolSize(Integer.valueOf(System.getProperty("jdbc_max_pool_size", "5")));
		dataSource.setMinimumIdle(Integer.valueOf(System.getProperty("jdbc_min_idle", "1")));
		dataSource.setConnectionTestQuery("SELECT 1");
		dataSource.setAutoCommit(true);

		return dataSource;
	}

	@Bean(name = "dagJdbcTemplate")
	public JdbcTemplate dagJdbcTemplate(@Qualifier("dagDataSource") DataSource dagDataSource) {
		return new JdbcTemplate(dagDataSource);
	}
}
