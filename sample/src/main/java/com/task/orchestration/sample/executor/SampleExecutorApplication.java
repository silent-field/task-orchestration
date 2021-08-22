package com.task.orchestration.sample.executor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
@ComponentScan(value = {"com.task.orchestration.sample.executor","com.slient.task.orchestration"})
public class SampleExecutorApplication {
    public static void main(String[] args) {
        SpringApplication.run(SampleExecutorApplication.class, args);
    }
}