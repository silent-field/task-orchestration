package com.slient.task.orchestration.graph.dag;

/**
 * @author gy
 * @version 1.0
 * @date 2021/2/25.
 * @description: 边信息
 */
public interface Edge<V> {
	/**
	 * 起始节点
	 *
	 * @return
	 */
	V getSourceNode();

	/**
	 * 目标节点
	 *
	 * @return
	 */
	V getTargetNode();
}
