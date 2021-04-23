package com.slient.task.orchestration.graph.dag;

/**
 * @author gy
 * @version 1.0
 * @date 2021/2/25.
 * @description: DAG节点
 */
public interface Node<K> {
	/**
	 * 节点标识
	 *
	 * @return
	 */
	K getNodeSign();
}
