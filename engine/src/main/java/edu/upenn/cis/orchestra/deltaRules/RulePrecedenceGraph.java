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
package edu.upenn.cis.orchestra.deltaRules;


import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import edu.upenn.cis.orchestra.deltaRules.exceptions.RulesCycleException;
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datamodel.AbstractRelation;


/**
 * This class is used to sort rules using a topological sort. 
 *
 * @author Olivier Biton
 *
 */
public class RulePrecedenceGraph {

	// Input list of rules
	private List<Rule> _rules;
	// Map each relation to its index in the graph matrix
	private Map<AbstractRelation, Integer> _ruleIndexes;
	// Number of relations in the graph.
	private int _nbRelations=0;
	
	// Graph stored as a boolean matrix.
	private boolean[][] _graph;
	
	// Nodes flags used during depth first search 
	protected enum Flag 
				{
					Unseen,
					Current,
					Done
				};
				
	// Flag of node in the depth first search
	private Flag[] _nodeFlag;
	
	// This array is used to store at what time a node has been seen in the depth first algorithm 
	// the "time" for a node is the "time" at which the recursion was ended, not started (a root time
	// is higher than its subnodes time)
	// Will be used to sort the nodes
	private int[] _nodeLastSeenTime;
	private int _currTime;
	
	
	/**
	 * Initialize the graph from a set of rules. 
	 * The internal representation of dependencies is initialized from these rules... 
	 * 
	 * @param rules Rules to use for the init
	 */
	private RulePrecedenceGraph (List<Rule> rules)
	{
		_rules = rules;
		_ruleIndexes = new HashMap<AbstractRelation, Integer> ();
		
		// Init the indexes map 
		for (Rule r : _rules)
		{
			addToIndex(r.getHead());
			for (Atom a : r.getBody())
				addToIndex(a);
		}
		
		// Init the graph matrix
		_graph = new boolean[_nbRelations][_nbRelations]; // default value: false
		for (Rule r : _rules)
		{
			int u = _ruleIndexes.get(r.getHead().getRelation());
			for (Atom a : r.getBody())
			{
				int v = _ruleIndexes.get(a.getRelation());
				_graph[u][v] = true;
			}
		}
	}
	
	/**
	 * If the relation used in the atom a does not have an index in the graph yet, add it 
	 * to the indexes map
	 * @param a Atom for which relation index must be checked/created if necessary
	 */
	private void addToIndex (Atom a)
	{
		if (!_ruleIndexes.containsKey(a.getRelation()))
			_ruleIndexes.put(a.getRelation(), Integer.valueOf(_nbRelations++));
	}
	
	
	/**
	 * Run a depth first search in the graph to detect cycles.
	 * @return True if no cycle detected, false if there is at least one cycle
	 */
	protected boolean dfs ()
	{
		boolean res = true;
		
		// Init all nodes flags as being unseen
		_nodeFlag = new Flag[_nbRelations];
		for (int i = 0 ; i < _nbRelations ; i++)
			_nodeFlag[i] = Flag.Unseen;
		
		// init the the "last time seen" array
		_nodeLastSeenTime = new int[_nbRelations];
		_currTime = 0;
		
		// For each node, if it's unseen from previous recursions, run the depth first search
		for (int u = 0 ; u < _nbRelations && res ; u++)
			if (_nodeFlag[u] == Flag.Unseen)
				res = dfs_visit(u);
		
		return res;
	}
	
	/**
	 * Recursive method used for the depth first search for a given node in the graph
	 * @param u Index of the node to visit
	 */
	private boolean dfs_visit (int u) 
	{
		boolean res = true;
		
		// This node is being seen in the depth search
		_nodeFlag[u] = Flag.Current;
		
		// For each edge from this node in the graph...
		for (int v = 0 ; v < _nbRelations && res ; v++)
			if (_graph[u][v])
			{
				// If the node has not been seen yet, continue the depth first search
				if (_nodeFlag[v]==Flag.Unseen)
					res = dfs_visit(v);
				// if it's currently being processed, as we are in depth first this is a cycle (back edge)!
				else
					res = (_nodeFlag[v]==Flag.Done);
			}
		
		// All sub-nodes have now been seen, node can be marked Done
		_nodeFlag[u] = Flag.Done;
		// Set the "last seen" time for the node
		_nodeLastSeenTime[u] = _currTime++;
		// No cycle detected here
		return res;
	}
	
	/**
	 * Run a topological sort of the graph defined by this list of rules
	 * If a cycle is detected in this graph, an exception will be raised!
	 * @param rules Rules to sort
	 * @throws RulesCycleException If a cycle is detected in the rules 
	 */
	public static void topologicalSort (List<Rule> rules)
				throws RulesCycleException
	{
		RulePrecedenceGraph graph = new RulePrecedenceGraph (rules);
		if (!graph.dfs())
			throw new RulesCycleException ();
		Collections.sort(rules, graph.new RuleComparator());
	}
	
	/**
	 * Comparator used to sort the rules.
	 * Rules are sorted according to the "last seen time" as specified in the 
	 * depth first search (<code>dfs</code> method)
	 * @author Olivier Biton
	 * @see RulePrecedenceGraph#dfs()
	 */
	private class RuleComparator implements Comparator<Rule>
	{
		
		public int compare(Rule r1, Rule r2) {
			int i1 = _ruleIndexes.get(r1.getHead().getRelation());
			int i2 = _ruleIndexes.get(r1.getHead().getRelation());
			return _nodeLastSeenTime[i1] - _nodeLastSeenTime[i2];
		}
	}
	
	
}
