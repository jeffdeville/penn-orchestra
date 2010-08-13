package edu.upenn.cis.orchestra.proql;

import java.util.HashSet;
import java.util.Set;

public class MatchPatterns {

	/**
	 * Standard matching function:  match patterns against the root
	 * 
	 * @param g
	 * @param p
	 * @return
	 */
	public static Set<SchemaSubgraph> getSubgraphs(SchemaGraph g, Pattern p) {
		Set<SchemaSubgraph> ret = new HashSet<SchemaSubgraph>();
		PatternStep ps = p.getPatternStart();
		Set<TupleMatch> rootMatches = ps.getTargetSet();
		
		Set<TupleNode> roots = new HashSet<TupleNode>();
		for (TupleMatch tm : rootMatches) {
			String rn = tm.getName(); 
			
			roots.addAll(getRootNodes(g, rn));
		}
		
		for (TupleNode root : roots) {
			SchemaSubgraph gr = getSubgraph(g, root, p);
			if (!gr.getAllDerivations().isEmpty())
				ret.add(gr);
		}
		return ret;
	}

	/**
	 * Finds and returns the set of all tuple nodes that qualify as "roots".
	 * 
	 * @param g
	 * @param relName
	 * @return
	 */
	public static Set<TupleNode> getRootNodes(SchemaGraph g, String relName) {
		Set<TupleNode> ret = new HashSet<TupleNode>();
		for (TupleNode t: g.getAllTuples()) {
			if (relName.isEmpty() || t.matches(relName)) {
				ret.add(t);
			}
		}
		return ret;
	}
	
	/**
	 * Rooted match function: match pattern from the root tuple node
	 * 
	 * @param g
	 * @param root
	 * @param p
	 * @return
	 */
	public static SchemaSubgraph getSubgraph(SchemaGraph g, TupleNode root, Pattern p) {
		PatternStep ps = p.getPatternStart();
		
		SchemaSubgraph sg = new SchemaSubgraph(root, g);
		Set<TupleNode> visited = new HashSet<TupleNode>();
	
		getSubgraph(g, root, ps, sg, visited);
		
		return sg;
	}

	
	static boolean originalGetSubgraph(SchemaGraph g, TupleNode root, PatternStep p, SchemaSubgraph sg,
			Set<TupleNode> visited) {
		
		if (visited.contains(root))
			return false;
		
		Set<TupleNode> next = new HashSet<TupleNode>();
		boolean ret = false;
		for (DerivationNode d: g.getTargetDerivations(root)) {
			PatternStep.MATCH_RESULT result = p.match(d, next);
		
			if (result == PatternStep.MATCH_RESULT.MATCH_ADVANCE) {
				if (p.isEndOfPattern()) {
					ret = true;
					sg.addDerivationNode(d);
					continue;
				} else {
					// Recursively try to match the next pattern step -- saving the
					// fact that we are considering this derivation node
					boolean addThis = false;//true;
					Set<TupleNode> v = new HashSet<TupleNode>();
					v.addAll(visited);
					v.add(root);
					for (TupleNode tn : next) {
						if (getSubgraph(g, tn, p.getNextPatternStep(), sg, v))
							addThis = true;//&= false;
					}
					if (addThis)
						sg.addDerivationNode(d);
					ret = ret | addThis;
					continue;
				}
			} else if (result == PatternStep.MATCH_RESULT.MATCH_NOADVANCE) {
				Set<TupleNode> v = new HashSet<TupleNode>();
				v.addAll(visited);
				v.add(root);
				boolean addThis = false;//true;
				if (!p.isEndOfPattern()) {
					// Recursively try to match the next pattern step -- saving the
					// fact that we are considering this derivation node
					for (TupleNode tn : next) {
						if (getSubgraph(g, tn, p.getNextPatternStep(), sg, v))
							addThis = true;//&= false;
					}
				}
				
				// Also recursively try to match the next node on the *current*
				// step -- this is a nondeterministic choice so we try both branches

				for (TupleNode tn : next) {
					if (getSubgraph(g, tn, p, sg, v))
						addThis = true;//&= false;
				}
				if (addThis || p.isEndOfPattern()) {
					sg.addDerivationNode(d);
					addThis = true;
				}

				ret = ret | addThis;
				continue;
			} else if (result == PatternStep.MATCH_RESULT.NOMATCH_ADVANCE) {
				continue;
			} else if (result == PatternStep.MATCH_RESULT.NOMATCH_NOADVANCE) {
				// Try to match the next node on the *current*
				// step
				Set<TupleNode> v = new HashSet<TupleNode>();
				v.addAll(visited);
				v.add(root);
				visited.add(root);
				boolean addThis = false;//true;
				for (TupleNode tn : next) {
					if (getSubgraph(g, tn, p, sg, v))
						addThis = true;//&= false;
				}
				if (addThis)
					sg.addDerivationNode(d);
				ret = ret | addThis;
				continue;
			}
		}
		return ret;
	}

	static boolean getSubgraph(SchemaGraph g, TupleNode root, PatternStep p, SchemaSubgraph sg,
			Set<TupleNode> visited) {
		
		if (visited.contains(root))
			return false;
		
		Set<TupleNode> next = new HashSet<TupleNode>();
		boolean ret = false;
		for (DerivationNode d: g.getTargetDerivations(root)) {
			PatternStep.MATCH_RESULT result = p.match(d, next);
		
			if (result == PatternStep.MATCH_RESULT.MATCH_ADVANCE) {
				if (p.isEndOfPattern()) {
					ret = true;
					sg.addDerivationNode(d);
					continue;
				} else {
					// Recursively try to match the next pattern step -- saving the
					// fact that we are considering this derivation node
					boolean addThis = false;//true;
//					Set<TupleNode> v = new HashSet<TupleNode>();
//					v.addAll(visited);
//					v.add(root);
//					Greg: Replaced above with following
					visited.add(root);
					for (TupleNode tn : next) {
//						if (getSubgraph(g, tn, p.getNextPatternStep(), sg, v))
//						Greg: Replaced above with following
						if (getSubgraph(g, tn, p.getNextPatternStep(), sg, visited))
							addThis = true;//&= false;
					}
					if (addThis)
						sg.addDerivationNode(d);
					ret = ret | addThis;
					continue;
				}
			} else if (result == PatternStep.MATCH_RESULT.MATCH_NOADVANCE) {
//				Set<TupleNode> v = new HashSet<TupleNode>();
//				v.addAll(visited);
//				v.add(root);
//				Greg: Replaced above with following
				visited.add(root);
				boolean addThis = false;//true;
				if (!p.isEndOfPattern()) {
					// Recursively try to match the next pattern step -- saving the
					// fact that we are considering this derivation node
					for (TupleNode tn : next) {
//						if (getSubgraph(g, tn, p.getNextPatternStep(), sg, v))
//						Greg: Replaced above with following
						if (getSubgraph(g, tn, p.getNextPatternStep(), sg, visited))
							addThis = true;//&= false;
					}
				}
				
				// Also recursively try to match the next node on the *current*
				// step -- this is a nondeterministic choice so we try both branches

				for (TupleNode tn : next) {
//					if (getSubgraph(g, tn, p, sg, v))
//					Greg: Replaced above with following
					if (getSubgraph(g, tn, p, sg, visited))
						addThis = true;//&= false;
				}
				if (addThis || p.isEndOfPattern()) {
					sg.addDerivationNode(d);
					addThis = true;
				}

				ret = ret | addThis;
				continue;
			} else if (result == PatternStep.MATCH_RESULT.NOMATCH_ADVANCE) {
				continue;
			} else if (result == PatternStep.MATCH_RESULT.NOMATCH_NOADVANCE) {
				// Try to match the next node on the *current*
				// step
//				Set<TupleNode> v = new HashSet<TupleNode>();
//				v.addAll(visited);
//				v.add(root);
//				Greg: Replaced above with following
				visited.add(root);
				boolean addThis = false;//true;
				for (TupleNode tn : next) {
//					if (getSubgraph(g, tn, p, sg, v))
//					Greg: Replaced above with following
					if (getSubgraph(g, tn, p, sg, visited))
						addThis = true;//&= false;
				}
				if (addThis)
					sg.addDerivationNode(d);
				ret = ret | addThis;
				continue;
			}
		}
//		visited.add(root);
		return ret;
	}

	
	/*
	public static void getSubgraph(SchemaGraph g, DerivationNode root, PatternStep p,
			Subgraph match) {
		
	}*/
}
