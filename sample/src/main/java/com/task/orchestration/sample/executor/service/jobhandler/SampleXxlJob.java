package com.task.orchestration.sample.executor.service.jobhandler;

import com.slient.task.orchestration.annotation.XxlJobTaskOrchestration;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.annotation.XxlJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
@Component
public class SampleXxlJob {
    private static Logger logger = LoggerFactory.getLogger(SampleXxlJob.class);

    @XxlJob("dag-test-1")
    @XxlJobTaskOrchestration
    public void dagTest1() throws Exception {
        XxlJobHelper.log("dag-test-1, Hello World.");

        for (int i = 0; i < 5; i++) {
            XxlJobHelper.log("beat at:" + i);
            TimeUnit.SECONDS.sleep(2);
        }
    }

    @XxlJob("dag-test-2")
    @XxlJobTaskOrchestration
    public void dagTest2() throws Exception {
        XxlJobHelper.log("dag-test-2, Hello World.");

        for (int i = 0; i < 5; i++) {
            XxlJobHelper.log("beat at:" + i);
            TimeUnit.SECONDS.sleep(2);
        }
    }

    @XxlJob("dag-test-3")
    @XxlJobTaskOrchestration
    public void dagTest3() throws Exception {
        XxlJobHelper.log("dag-test-3, Hello World.");

        for (int i = 0; i < 5; i++) {
            XxlJobHelper.log("beat at:" + i);
            TimeUnit.SECONDS.sleep(2);
        }
    }

    @XxlJob("dag-test-4")
    @XxlJobTaskOrchestration
    public void dagTest4() throws Exception {
        XxlJobHelper.log("dag-test-4, Hello World.");

        for (int i = 0; i < 5; i++) {
            XxlJobHelper.log("beat at:" + i);
            TimeUnit.SECONDS.sleep(2);
        }
    }

    @XxlJob("dag-test-5")
    @XxlJobTaskOrchestration
    public void dagTest5() throws Exception {
        XxlJobHelper.log("dag-test-5, Hello World.");

        for (int i = 0; i < 5; i++) {
            XxlJobHelper.log("beat at:" + i);
            TimeUnit.SECONDS.sleep(2);
        }
    }

    @XxlJob("dag-test-6")
    @XxlJobTaskOrchestration
    public void dagTest6() throws Exception {
        XxlJobHelper.log("dag-test-6, Hello World.");

        for (int i = 0; i < 5; i++) {
            XxlJobHelper.log("beat at:" + i);
            TimeUnit.SECONDS.sleep(2);
        }
    }
}
