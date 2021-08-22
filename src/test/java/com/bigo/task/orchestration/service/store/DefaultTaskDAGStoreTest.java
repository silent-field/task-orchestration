package com.bigo.task.orchestration.service.store;

import com.alibaba.fastjson.JSON;
import com.bigo.task.orchestration.service.AbstractUnitTest;
import com.slient.task.orchestration.graph.dag.DAG;
import com.slient.task.orchestration.model.TaskNode;
import com.slient.task.orchestration.model.TaskNodeRelation;
import com.slient.task.orchestration.store.DefaultTaskDAGStore;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.List;

/**
 * @author gy
 * @version 1.0
 * @date 2021/3/2.
 * @description:
 */
@Slf4j
public class DefaultTaskDAGStoreTest extends AbstractUnitTest {
	@Autowired
	private DefaultTaskDAGStore defaultTaskDAGStore;

	@Test
	public void testLoad() {
		/**
		 * 		3		4
		 * 		 \	   /
		 * 			2		5
		 *		  /	  \   /
		 * 		6		1
		 */
		Pair<String, DAG<TaskNode, TaskNodeRelation<TaskNode>>> pair = buildDAG();

		int[] result = defaultTaskDAGStore.save(pair.getRight());
		Assert.assertTrue(result.length > 0);

		DAG<TaskNode, TaskNodeRelation<TaskNode>> dbDAG =
				defaultTaskDAGStore.loadByTaskGroupId(pair.getLeft());

		log.info("DAG : " + JSON.toJSONString(dbDAG));
	}

	@Test
	public void testSave() {
		/**
		 * 		3		4
		 * 		 \	   /
		 * 			2		5
		 *		  /	  \   /
		 * 		6		1
		 */
		DAG<TaskNode, TaskNodeRelation<TaskNode>> dag = buildDAG().getRight();

		log.info(JSON.toJSONString(dag) + "\r\n");

		int[] result = defaultTaskDAGStore.save(dag);
		Assert.assertTrue(result.length > 0);
	}

	// ------------------- common function
	private Pair<String, DAG<TaskNode, TaskNodeRelation<TaskNode>>> buildDAG() {
		DAG<TaskNode, TaskNodeRelation<TaskNode>> dag = new DAG<>();

		/**
		 * 		3		4
		 * 		 \	   /
		 * 			2		5
		 *		  /	  \   /
		 * 		6		1
		 */
		List<TaskNode> nodes = new ArrayList<>();
		long currentTime = System.currentTimeMillis();
		for (int i = 0; i < 6; i++) {
			String taskId = currentTime + "-" + (i + 1);

			TaskNode taskNode = new TaskNode(taskId, String.valueOf(currentTime));
			taskNode.setName("task" + taskId);

			nodes.add(taskNode);
			dag.addNode(taskNode);
		}

		nodes.get(1).setUpstreamTasks(nodes.get(2).getTaskId() + "," + nodes.get(3).getTaskId());
		nodes.get(5).setUpstreamTasks(nodes.get(1).getTaskId());
		nodes.get(0).setUpstreamTasks(nodes.get(1).getTaskId() + "," + nodes.get(4).getTaskId());

		dag.addEdge(nodes.get(2), nodes.get(1));
		dag.addEdge(nodes.get(3), nodes.get(1));
		dag.addEdge(nodes.get(1), nodes.get(5));
		dag.addEdge(nodes.get(1), nodes.get(0));
		dag.addEdge(nodes.get(4), nodes.get(0));

		return ImmutablePair.of(currentTime + "", dag);
	}
}
