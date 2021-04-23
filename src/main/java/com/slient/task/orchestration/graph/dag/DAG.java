package com.slient.task.orchestration.graph.dag;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.slient.task.orchestration.exception.DagRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author gy
 * @version 1.0
 * @date 2021/2/25.
 * @description: 有向无环图数据结构代码实现
 * N : 节点
 * E : 边
 */
@Slf4j
public class DAG<N extends Node, E extends Edge> {
	private final ReadWriteLock lock = new ReentrantReadWriteLock();

	/**
	 * 节点集合，每个节点包含各自的节点标识 {@linkplain Node#getNodeSign()}
	 */
	private volatile Set<N> nodes;

	/**
	 * key是dag图中一个节点，value是key的<目标节点-出边>键值对。
	 * 如节点A -> 节点B，边是E1，那么有Entry<A,Entry<B,E1>
	 */
	private volatile Map<N, Map<N, E>> edgesMap;

	/**
	 * key是dag图中一个节点，value是key的<源节点-入边>键值对。
	 * 如节点A -> 节点B，边是E1，那么有Entry<B,Entry<A,E1>
	 */
	private volatile Map<N, Map<N, E>> reverseEdgesMap;

	public DAG() {
		nodes = new HashSet<>();
		edgesMap = new HashMap<>();
		reverseEdgesMap = new HashMap<>();
	}

	/**
	 * 添加一个DAG节点
	 *
	 * @param node
	 */
	public void addNode(N node) {
		lock.writeLock().lock();

		try {
			nodes.add(node);
		} finally {
			lock.writeLock().unlock();
		}
	}

	/**
	 * 添加边
	 *
	 * @param fromNode 边的源节点
	 * @param toNode   边的目标节点
	 * @return
	 */
	public AddEdgeResult addEdge(N fromNode, N toNode) {
		return addEdge(fromNode, toNode, false);
	}

	/**
	 * 添加边
	 *
	 * @param fromNode   边的源节点
	 * @param toNode     边的目标节点
	 * @param useNewNode 是否允许使用新节点(入参传入的节点)
	 * @return
	 */
	public AddEdgeResult addEdge(N fromNode, N toNode, boolean useNewNode) {
		return addEdge(fromNode, toNode, null, useNewNode);
	}

	/**
	 * 添加边
	 *
	 * @param fromNode   边的源节点
	 * @param toNode     边的目标节点
	 * @param edge       边信息
	 * @param useNewNode 是否允许使用新节点(入参传入的节点)
	 * @return
	 */
	public AddEdgeResult addEdge(N fromNode, N toNode, E edge, boolean useNewNode) {
		lock.writeLock().lock();

		try {
			if (!isLegalEdgeVertices(fromNode, toNode, useNewNode)) {
				log.error("edge({} -> {})顶点不合法！", fromNode, toNode);
				return AddEdgeResult.FAIL;
			}

			// 判断添加边后是否存在环，如果存在则不允许添加
			if (!isLegalAddEdge(fromNode, toNode)) {
				log.error("添加的edge({} -> {})会导致形成环！", fromNode, toNode);
				return AddEdgeResult.FAIL;
			}


			addNodeIfAbsent(fromNode);
			addNodeIfAbsent(toNode);

			addEdge(fromNode, toNode, edge, edgesMap);
			addEdge(toNode, fromNode, edge, reverseEdgesMap);

			return AddEdgeResult.SUCCESS;
		} catch (Exception e) {
			return AddEdgeResult.EXCEPTION;
		} finally {
			lock.writeLock().unlock();
		}
	}

	/**
	 * 是否包含指定节点
	 *
	 * @param node
	 * @return
	 */
	public boolean containsNode(N node) {
		if (null == node) {
			log.warn("containsNode方法传入的node为空");
			return false;
		}

		lock.readLock().lock();

		try {
			return nodes.contains(node);
		} finally {
			lock.readLock().unlock();
		}
	}

	/**
	 * 是否存在以fromNode为源节点，toNode为目标节点的边
	 *
	 * @param fromNode 源节点
	 * @param toNode   目标节点
	 * @return
	 */
	public boolean containsEdge(N fromNode, N toNode) {
		lock.readLock().lock();
		try {
			if (!nodes.contains(fromNode) || !nodes.contains(toNode)) {
				return false;
			}

			if (MapUtils.isEmpty(edgesMap) || !edgesMap.containsKey(fromNode)) {
				return false;
			}

			Map<N, E> endEdges = edgesMap.get(fromNode);
			if (null == endEdges) {
				return false;
			}

			return endEdges.containsKey(toNode);
		} finally {
			lock.readLock().unlock();
		}
	}

	/**
	 * @return DAG图中节点总数
	 */
	public int getNodesCount() {
		lock.readLock().lock();

		try {
			return nodes.size();
		} finally {
			lock.readLock().unlock();
		}
	}

	/**
	 * @return DAG图中边总数
	 */
	public int getEdgesCount() {
		lock.readLock().lock();
		try {
			int count = 0;

			for (Map<N, E> value : edgesMap.values()) {
				count += value.size();
			}

			return count;
		} finally {
			lock.readLock().unlock();
		}
	}

	/**
	 * 获取起始节点集合，也就是入度为0的节点集合
	 *
	 * @return
	 */
	public Collection<N> getBeginNodes() {
		lock.readLock().lock();

		try {
			// 得到不存在入边的节点集合，reverseEdgesMap的keySet是nodes的子集
			return Sets.difference(nodes, reverseEdgesMap.keySet()).immutableCopy();
		} finally {
			lock.readLock().unlock();
		}
	}

	/**
	 * 获取结束节点集合，也就是出度为0的节点集合
	 *
	 * @return
	 */
	public Collection<N> getEndNodes() {
		lock.readLock().lock();

		try {
			// 得到不存在出边的节点集合，edgesMap的keySet是nodes的子集
			return Sets.difference(nodes, edgesMap.keySet()).immutableCopy();
		} finally {
			lock.readLock().unlock();
		}
	}

	/**
	 * 获取指定节点的上游节点集合
	 *
	 * @param node
	 * @return
	 */
	public Set<N> getUpstreamNodes(N node) {
		lock.readLock().lock();

		try {
			return getNeighborNodes(node, reverseEdgesMap);
		} finally {
			lock.readLock().unlock();
		}
	}

	/**
	 * 获取指定节点的下游节点集合
	 *
	 * @param node
	 * @return
	 */
	public Set<N> getDownstreamNodes(N node) {
		lock.readLock().lock();

		try {
			return getNeighborNodes(node, edgesMap);
		} finally {
			lock.readLock().unlock();
		}
	}

	/**
	 * 获取入度
	 *
	 * @param node
	 * @return
	 */
	public int getInDegree(N node) {
		lock.readLock().lock();

		try {
			return getUpstreamNodes(node).size();
		} finally {
			lock.readLock().unlock();
		}
	}

	/**
	 * 判断图中是否存在环
	 *
	 * @return true代表有环，false代表没有环
	 */
	public boolean hasCycle() {
		lock.readLock().lock();
		try {
			return !topologicalSortReal().getKey();
		} finally {
			lock.readLock().unlock();
		}
	}

	/**
	 * 得到DAG图的拓扑排序
	 *
	 * @return
	 * @throws Exception
	 */
	public List<N> topologicalSort() {
		lock.readLock().lock();

		try {
			Pair<Boolean, List<N>> entry = topologicalSortReal();

			if (Boolean.TRUE.equals(entry.getKey())) {
				return entry.getValue();
			}

			throw new DagRuntimeException("当前图存在环! ");
		} finally {
			lock.readLock().unlock();
		}
	}

	/**
	 * 如果节点不存在，则添加
	 *
	 * @param node
	 */
	private void addNodeIfAbsent(N node) {
		if (!containsNode(node)) {
			addNode(node);
		}
	}

	/**
	 * 添加边
	 *
	 * @param fromNode 源节点
	 * @param toNode   目标节点
	 * @param edge     边
	 * @param edges    边Map，可能是出边Map或者是入边Map
	 */
	private void addEdge(N fromNode, N toNode, E edge, Map<N, Map<N, E>> edges) {
		edges.putIfAbsent(fromNode, new HashMap<>());
		Map<N, E> toNodeEdges = edges.get(fromNode);
		toNodeEdges.put(toNode, edge);
	}

	/**
	 * 判断当添加一条边时，两个顶点是否合法。
	 * 1. 两个顶点不能相同
	 * 2. 两个顶点必须存在
	 *
	 * @param fromNode   边的源节点
	 * @param toNode     边的目标节点
	 * @param useNewNode 是否允许使用新节点(入参传入的节点)
	 * @return
	 */
	private boolean isLegalEdgeVertices(N fromNode, N toNode, boolean useNewNode) {
		if (null == fromNode || null == toNode) {
			log.error("edge fromNode({}) toNode({})不能为null", fromNode, toNode);
			return false;
		}

		if (fromNode.equals(toNode)) {
			log.error("edge fromNode({})不可以等于toNode({})", fromNode, toNode);
			return false;
		}

		// 如果不允许使用入参节点作为新节点，那么fromNode，toNode必须都存在
		if (!useNewNode && (!containsNode(fromNode) || !containsNode(toNode))) {
			log.error("edge fromNode({})或者toNode({})不是当前DAG图中的节点", fromNode, toNode);
			return false;
		}

		return true;
	}

	/**
	 * 判断当添加一条边(fromNode -> toNode)后，是否还满足无环约束，出现环则是非法的边
	 *
	 * @param fromNode 源节点
	 * @param toNode   目标节点
	 * @return
	 */
	private boolean isLegalAddEdge(N fromNode, N toNode) {
		// 检查DAG是不是存在环
		int verticesCount = getNodesCount();

		Queue<N> queue = new LinkedList<>();
		queue.add(toNode);

		// 从toNode出发BFS遍历下游节点，如果存在下游节点是fromNode，说明存在环
		while (!queue.isEmpty() && (--verticesCount > 0)) {
			N key = queue.poll();

			for (N downstreamNode : getDownstreamNodes(key)) {
				if (downstreamNode.equals(fromNode)) {
					return false;
				}

				queue.add(downstreamNode);
			}
		}

		return true;
	}

	/**
	 * 获取指定节点对应相邻节点(上游或下游)集合
	 *
	 * @param node
	 * @param edges
	 * @return
	 */
	private Set<N> getNeighborNodes(N node, final Map<N, Map<N, E>> edges) {
		final Map<N, E> neighborEdges = edges.get(node);

		if (neighborEdges == null) {
			return Collections.emptySet();
		}

		return neighborEdges.keySet();
	}

	/**
	 * 确定是否存在环，如果不存在环可以得到DAG拓扑排序结果
	 * <p>
	 * DAG图(无环)才有拓扑排序，否则变成循环引用
	 * <p>
	 * 使用广度优先访问：
	 * 1、遍历图中所有顶点，把入度为0的顶点加入队列（由于取顶点顺序不一样，不同代码实现可能会出现拓扑排序结果不一样）
	 * 2、轮询队列中的顶点，更新其邻接点(下游节点)的入度值（减1），如果邻接点入度减1等于0，则将该邻接点加入队列
	 * 3、重复第2步，直到队列为空
	 * 4、如果不能遍历所有节点，意味着当前这个图不满足DAG(无环)的约束，也就不存在拓扑排序。
	 * <p>
	 * key为true表示无环，false表示存在环。value是拓扑排序结果
	 *
	 * @return
	 */
	private Pair<Boolean, List<N>> topologicalSortReal() {
		// 入度为0的节点集合
		Queue<N> zeroInDegreeNodeQueue = new LinkedList<>();

		// 拓扑排序结果
		List<N> topologicalSortResult = new ArrayList<>();

		// 入度不为0的分组集合，key是节点，value是入度值
		Map<N, Integer> notZeroInDegreeNodeMap = new HashMap<>();

		// 对节点按入度是否为0进行分组
		for (N node : nodes) {
			int inDegree = getInDegree(node);

			if (inDegree == 0) {
				zeroInDegreeNodeQueue.add(node);
				// 入度为0的节点是第一层(Root)节点
				topologicalSortResult.add(node);
			} else {
				notZeroInDegreeNodeMap.put(node, inDegree);
			}
		}

		// 如果不存在入度为0的节点，意味着存在环
		if (zeroInDegreeNodeQueue.isEmpty()) {
			return ImmutablePair.of(false, topologicalSortResult);
		}

		// 使用拓扑算法删除入度为0的节点及其关联边
		while (!zeroInDegreeNodeQueue.isEmpty()) {
			N currentNode = zeroInDegreeNodeQueue.poll();
			// 获取对应邻接点(下游节点)集合
			Set<N> downstreamNodes = getDownstreamNodes(currentNode);

			for (N downstreamNode : downstreamNodes) {
				// 入度值
				Integer degree = notZeroInDegreeNodeMap.get(downstreamNode);

				if (--degree == 0) {
					// 如果删除边后，邻接点入度为0
					// 添加到拓扑排序结果，添加到入度为0的分组(zeroInDegreeNodeQueue)，从入度不为0的分组(notZeroInDegreeNodeMap)中移除
					topologicalSortResult.add(downstreamNode);
					zeroInDegreeNodeQueue.add(downstreamNode);
					notZeroInDegreeNodeMap.remove(downstreamNode);
				} else {
					// 如果删除边后，入度不为0，说明还存在上游节点，更新入度值
					notZeroInDegreeNodeMap.put(downstreamNode, degree);
				}
			}
		}

		// 如果经过上面循环步骤后，最终结果不存在入度大于0的节点，那么说明没有环
		return ImmutablePair.of(MapUtils.isEmpty(notZeroInDegreeNodeMap), topologicalSortResult);
	}

	/**
	 * DAG整体快照
	 *
	 * @return
	 */
	public Triple<Set<N>, Map<N, Map<N, E>>, Map<N, Map<N, E>>> snapshot() {
		lock.readLock().lock();

		try {
			return ImmutableTriple.of(ImmutableSet.copyOf(nodes), ImmutableMap.copyOf(edgesMap), ImmutableMap.copyOf(reverseEdgesMap));
		} finally {
			lock.readLock().unlock();
		}
	}

	/**
	 * DAG 节点快照
	 *
	 * @return
	 */
	public Set<N> nodeSnapshot() {
		lock.readLock().lock();

		try {
			return ImmutableSet.copyOf(nodes);
		} finally {
			lock.readLock().unlock();
		}
	}

	public List<N> getAllDownstreamNodes(N startNode) {
		List<N> all = new ArrayList<>();

		findDownstreamNodes(Lists.newArrayList(startNode), all);

		return all;
	}

	private void findDownstreamNodes(List<N> levelNodes, List<N> result) {
		List<N> nextLevelNodes = new ArrayList<>();

		for (N levelNode : levelNodes) {
			Set<N> oneNextLevelNodes = getDownstreamNodes(levelNode);
			if (CollectionUtils.isNotEmpty(oneNextLevelNodes)) {
				nextLevelNodes.addAll(oneNextLevelNodes);

				for (N oneNextLevelNode : oneNextLevelNodes) {
					if (result.contains(oneNextLevelNode)) {
						result.remove(oneNextLevelNode);
					}
					result.add(oneNextLevelNode);
				}
			}
		}

		if (!nextLevelNodes.isEmpty()) {
			findDownstreamNodes(nextLevelNodes, result);
		}
	}
}
