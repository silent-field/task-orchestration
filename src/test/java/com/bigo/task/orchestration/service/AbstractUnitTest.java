package com.bigo.task.orchestration.service;

import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * 启动参数
 * -Ddag_jdbc_username=root
 * -Ddag_jdbc_password=123456
 * -Ddag_jdbc_url=jdbc:mysql://127.0.0.1:3306/dag_test?characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useAffectedRows=true&useSSL=false&rewriteBatchedStatements=true&autoReconnect=true&serverTimezone=GMT%2B8
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:applicationContext.xml"})
public abstract class AbstractUnitTest {

}