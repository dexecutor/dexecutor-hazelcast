package com.github.dexecutor.hazelcast;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

import com.github.dexecutor.core.DexecutorState;
import com.github.dexecutor.core.Phase;
import com.github.dexecutor.core.graph.Dag;
import com.github.dexecutor.core.graph.DefaultDag;
import com.github.dexecutor.core.graph.Node;
import com.github.dexecutor.core.graph.Traversar;
import com.github.dexecutor.core.graph.TraversarAction;
import com.github.dexecutor.core.graph.Validator;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IMap;

public class HazelcastDexecutorState<T extends Comparable<T>, R> implements DexecutorState<T, R> {

	private final String CACHE_ID_PHASE;
	private final String CACHE_ID_GRAPH;
	private final String CACHE_ID_NODES_COUNT;
	private final String CACHE_ID_PROCESSED_NODES;
	private final String CACHE_ID_DISCONDINUED_NODES;
	private final String CACHE_ID_ERRORED_NODES;

	private IMap<String, Object> distributedCache;
	private IAtomicLong nodesCount;
	private Collection<Node<T, R>> processedNodes;
	private Collection<Node<T, R>> discontinuedNodes;
	private Collection<T> erroredNodes;

	public HazelcastDexecutorState(final String cacheName, final HazelcastInstance hazelcastInstance) {
		CACHE_ID_PHASE = cacheName + "-phase";
		CACHE_ID_GRAPH = cacheName + "-graph";
		CACHE_ID_NODES_COUNT = cacheName + "-nodes-count";
		CACHE_ID_PROCESSED_NODES = cacheName + "-processed-nodes";
		CACHE_ID_DISCONDINUED_NODES = cacheName + "-discontinued-nodes";
		CACHE_ID_ERRORED_NODES = cacheName + "-errored-nodes";

		this.distributedCache = hazelcastInstance.getMap(cacheName);

		this.distributedCache.put(CACHE_ID_PHASE, Phase.BUILDING);
		this.distributedCache.put(CACHE_ID_GRAPH, new DefaultDag<T, R>());

		this.nodesCount = hazelcastInstance.getAtomicLong(CACHE_ID_NODES_COUNT);
		this.processedNodes = hazelcastInstance.getList(CACHE_ID_PROCESSED_NODES);
		this.discontinuedNodes = hazelcastInstance.getList(CACHE_ID_DISCONDINUED_NODES);
		this.erroredNodes = hazelcastInstance.getList(CACHE_ID_ERRORED_NODES);
	}

	@Override
	public void addIndependent(T nodeValue) {
		@SuppressWarnings("unchecked")
		Dag<T, R> graph = (Dag<T, R>) this.distributedCache.get(CACHE_ID_GRAPH);
		graph.addIndependent(nodeValue);
		this.distributedCache.put(CACHE_ID_GRAPH, graph);		
	}

	@Override
	public void addDependency(T evalFirstValue, T evalAfterValue) {
		@SuppressWarnings("unchecked")
		Dag<T, R> graph = (Dag<T, R>) this.distributedCache.get(CACHE_ID_GRAPH);
		graph.addDependency(evalFirstValue, evalAfterValue);
		this.distributedCache.put(CACHE_ID_GRAPH, graph);
	}

	@Override
	public void addAsDependentOnAllLeafNodes(T nodeValue) {
		@SuppressWarnings("unchecked")
		Dag<T, R> graph = (Dag<T, R>) this.distributedCache.get(CACHE_ID_GRAPH);
		graph.addAsDependentOnAllLeafNodes(nodeValue);
		this.distributedCache.put(CACHE_ID_GRAPH, graph);		
	}

	@Override
	public void addAsDependencyToAllInitialNodes(T nodeValue) {
		@SuppressWarnings("unchecked")
		Dag<T, R> graph = (Dag<T, R>) this.distributedCache.get(CACHE_ID_GRAPH);
		graph.addAsDependencyToAllInitialNodes(nodeValue);
		this.distributedCache.put(CACHE_ID_GRAPH, graph);		
	}

	@Override
	public Set<Node<T, R>> getInitialNodes() {
		@SuppressWarnings("unchecked")
		Dag<T, R> graph = (Dag<T, R>) this.distributedCache.get(CACHE_ID_GRAPH);
		return graph.getInitialNodes();
	}

	@Override
	public Set<Node<T, R>> getNonProcessedRootNodes() {
		@SuppressWarnings("unchecked")
		Dag<T, R> graph = (Dag<T, R>) this.distributedCache.get(CACHE_ID_GRAPH);
		return graph.getNonProcessedRootNodes();
	}

	@Override
	public Node<T, R> getGraphNode(T id) {
		@SuppressWarnings("unchecked")
		Dag<T, R> graph = (Dag<T, R>) this.distributedCache.get(CACHE_ID_GRAPH);
		return graph.get(id);
	}

	@Override
	public int graphSize() {
		@SuppressWarnings("unchecked")
		Dag<T, R> graph = (Dag<T, R>) this.distributedCache.get(CACHE_ID_GRAPH);
		return graph.size();
	}

	@Override
	public void validate(Validator<T, R> validator) {
		@SuppressWarnings("unchecked")
		Dag<T, R> graph = (Dag<T, R>) this.distributedCache.get(CACHE_ID_GRAPH);
		validator.validate(graph);		
	}

	@Override
	public void setCurrentPhase(Phase currentPhase) {
		this.distributedCache.set(CACHE_ID_PHASE, currentPhase);		
	}

	@Override
	public Phase getCurrentPhase() {
		return (Phase) this.distributedCache.get(CACHE_ID_PHASE);
	}

	@Override
	public int getUnProcessedNodesCount() {
		return (int) this.nodesCount.get();
	}

	@Override
	public void incrementUnProcessedNodesCount() {
		this.nodesCount.incrementAndGet();		
	}

	@Override
	public void decrementUnProcessedNodesCount() {
		this.nodesCount.decrementAndGet();		
	}

	@Override
	public boolean shouldProcess(Node<T, R> node) {
		return !isAlreadyProcessed(node) && allIncomingNodesProcessed(node);
	}
	
	private boolean isAlreadyProcessed(final Node<T, R> node) {
		return this.processedNodes.contains(node);
	}

	private boolean allIncomingNodesProcessed(final Node<T, R> node) {
		if (node.getInComingNodes().isEmpty() || areAlreadyProcessed(node.getInComingNodes())) {
			return true;
		}
		return false;
	}
	
	private boolean areAlreadyProcessed(final Set<Node<T, R>> nodes) {
        return this.processedNodes.containsAll(nodes);
    }

	@Override
	public void markProcessingDone(Node<T, R> node) {
		this.processedNodes.add(node);		
	}

	@Override
	public Collection<Node<T, R>> getProcessedNodes() {
		return new ArrayList<>(this.processedNodes);
	}

	@Override
	public boolean isDiscontinuedNodesNotEmpty() {
		return !this.discontinuedNodes.isEmpty();
	}

	@Override
	public Collection<Node<T, R>> getDiscontinuedNodes() {
		return new ArrayList<Node<T, R>>(this.discontinuedNodes);
	}

	@Override
	public void markDiscontinuedNodesProcessed() {
		this.discontinuedNodes.clear();		
	}

	@Override
	public void processAfterNoError(Collection<Node<T, R>> nodes) {
		this.discontinuedNodes.addAll(nodes);		
	}

	@Override
	public void print(Traversar<T, R> traversar, TraversarAction<T, R> action) {
		Dag<T, R> graph = (Dag<T, R>) this.distributedCache.get(CACHE_ID_GRAPH);
		traversar.traverse(graph, action);		
	}

	@Override
	public void addErrored(T id) {
		this.erroredNodes.add(id);		
	}

	@Override
	public void removeErrored(T id) {
		this.erroredNodes.remove(id);		
	}

	@Override
	public int erroredCount() {
		return this.erroredNodes.size();
	}

	@Override
	public void forcedStop() {
		// TODO Auto-generated method stub		
	}
}
