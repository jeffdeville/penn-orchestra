/*
 * Copyright (C) 2010 Trustees of the University of Pennsylvania
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS of ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.upenn.cis.orchestra.util;

import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Queue;
import java.util.LinkedList;


/**
 * An adjacency-list based representation of a graph
 * 
 * @author netaylor
 *
 * @param <T> The vertex type. It must support hashing and equality operations.
 */
public class Graph<T> {
	private Set<T> vertices = new HashSet<T>();

	// Mapping from node to its predecessors
	private Map<T,Set<T>> pred = new HashMap<T,Set<T>>();
	// Mapping from node to its successors
	private Map<T,Set<T>> succ = new HashMap<T,Set<T>>();
	
	public void addEdge(T predNode, T succNode) {
		vertices.add(predNode);
		vertices.add(succNode);
		
		Set<T> predSet = pred.get(succNode);
		if (predSet == null) {
			predSet = new HashSet<T>();
			pred.put(succNode, predSet);
		}

		Set<T> succSet = succ.get(predNode);
		if (succSet == null) {
			succSet = new HashSet<T>();
			succ.put(predNode, succSet);
		}
		
		predSet.add(predNode);
		succSet.add(succNode);
	}
	
	public void addPredecessors(T succNode, Set<T> predNodes) {
		for (T predNode : predNodes) {
			addEdge(predNode, succNode);
		}
	}
	
	public static class NotDAG extends Exception {
		private static final long serialVersionUID = 1L;
	}
	
	/**
	 * Performs a topological sort on the vertices of the graph
	 * 
	 * @return	The vertices in topological order
	 * @throws NotDAG
	 * 			If the graph is not a DAG (i.e it has a cycle)
	 * 			and therefore cannot have a topological ordering
	 */
	public List<T> topologicalSort() throws NotDAG {
		List<T> sorted = new ArrayList<T>(vertices.size());
		
		// List of nodes that have already been output
		Set<T> done = new HashSet<T>(vertices.size());
		// Set of nodes that have had a predecessor output
		// and therefore may be outputable themselves
		Queue<T> needToCheck = new LinkedList<T>();
		
		// Start with nodes with in degree zero
		for (T v : vertices) {
			Set<T> predSet = pred.get(v);
			if (predSet == null) {
				done.add(v);
				sorted.add(v);
				Set<T> succSet = succ.get(v);
				if (succSet != null) {
					needToCheck.addAll(succSet);
				}
			}
		}
		
		DEQUEUE: while (! needToCheck.isEmpty()) {
			T v = needToCheck.remove();
			if (done.contains(v)) {
				// Node may end up in needToCheck
				// many times
				continue;
			}
			Set<T> predSet = pred.get(v);
			for (T predV : predSet) {
				if (! done.contains(predV)) {
					continue DEQUEUE;
				}
			}
			
			done.add(v);
			sorted.add(v);
			Set<T> succSet = succ.get(v);
			if (succSet != null) {
				needToCheck.addAll(succSet);
			}			
		}
		
		if (done.size() != vertices.size()) {
			throw new NotDAG();
		}
		
		return sorted;
	}
}
