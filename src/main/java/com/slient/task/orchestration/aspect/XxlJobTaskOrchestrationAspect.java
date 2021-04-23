package com.slient.task.orchestration.aspect;

import com.slient.task.orchestration.store.DefaultTaskDAGStore;
import com.slient.task.orchestration.xxl.OrchestrationHelper;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.context.XxlJobContext;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author gy
 */
@Slf4j
@Aspect
@Component
public class XxlJobTaskOrchestrationAspect {
	@Autowired
	private DefaultTaskDAGStore defaultTaskDAGStore;

	@Setter
	private boolean aspectSwitch = true;

	@Around("@annotation(com.slient.task.orchestration.annotation.XxlJobTaskOrchestration)")
	public Object doAround(ProceedingJoinPoint jp) throws Throwable {
		log.info("进入 XxlJobTaskOrchestrationHelper doAround");

		OrchestrationHelper helper = null;
		// 如果XxlJobOrchestrationHelper#canExecute出现异常，那么也认为可执行
		boolean canExecute = true;
		try {
			if (aspectSwitch) {
				helper = new OrchestrationHelper(XxlJobContext.getXxlJobContext().getJobId() + "", defaultTaskDAGStore);

				canExecute = helper.canExecute();

				log.info("XxlJobId:{},是否可执行:{}", XxlJobContext.getXxlJobContext().getJobId(), canExecute);
			} else {
				log.info("XxlJobTaskOrchestrationHelper 开关关闭，不判断是否可执行");
			}
		} catch (Exception e) {
			log.error("XxlJobTaskOrchestrationAspect", e);
		}

		Object ret = null;
		Throwable err = null;
		if (canExecute) {
			try {
				Object[] args = jp.getArgs();
				if (null != args && args.length > 0 && args[0] instanceof String) {
					args[0] = helper.modifyParams((String) args[0]);

					ret = jp.proceed(args);
				} else {
					ret = jp.proceed();
				}
			} catch (Throwable t) {
				err = t;
			}
		} else {
			log.info("DAG依赖判定不允许执行");
			return ReturnT.SUCCESS;
		}

		if (err != null) {
			log.error("XxlJobTaskOrchestrationAspect proceed", err);
			throw err;
		}

		finish(canExecute, helper);

		return ret;
	}

	private void finish(boolean canExecute, OrchestrationHelper helper) {
		try {
			if (aspectSwitch && canExecute && null != helper) {
				helper.updateWhenExecuteFinish();
			}
		} catch (Exception e) {
			log.error("XxlJobTaskOrchestrationAspect updateWhenExecuteFinish", e);
		}
	}
}