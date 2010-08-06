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
package edu.upenn.cis.orchestra.datalog;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import edu.upenn.cis.orchestra.Debug;
import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datalog.atom.AtomArgument;
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.mappings.exceptions.RecursionException;
import edu.upenn.cis.orchestra.provenance.ProvEdbNode;
import edu.upenn.cis.orchestra.provenance.ProvFakeNode;
import edu.upenn.cis.orchestra.provenance.ProvIdbNode;
import edu.upenn.cis.orchestra.provenance.ProvenanceNode;

/**
 * View unfolding class for nonrecursive datalog rules.  Maintains a provenance tree
 * for each of these rules, which then gets converted into the appropriate datalog
 * interpreted predicates.
 * 
 * @author zives
 *
 */
public class DatalogViewUnfolder {
	/**
	 * Given a set of rules, make the named atom the distinguished atom
	 * for a query result.  Unfold the rules and create a single expression.
	 * 
	 * @param rules
	 * @param queryAtom
	 */
	public static List<Rule> unfoldQuery(Collection<Rule> rules, String queryAtom,
			Map<Atom,ProvenanceNode> prov, Set<String> provenanceRelations,
			String semiringName, String assgnExpr, boolean BFS) throws RecursionException {
		HashMap<String,List<Rule>> ruleMap = new HashMap<String,List<Rule>>();

		// Index each rule or set of rules by its atom name
		for (Rule r: rules) {
			List<Rule> rs = ruleMap.get(r.getHead().getRelationContext().toString());
			if (rs == null) {
				rs = new ArrayList<Rule>();
				ruleMap.put(r.getHead().getRelationContext().toString(), rs);				
			}
			if (!rs.contains(r))
				rs.add(r);
		}

		// The set of atoms we've already expanded -- we don't want to
		// try to expand a recursive / cyclic query into a single SQL
		// statement, and instead require the use of runMaterializedQuery.
		Set<String> visited = new HashSet<String>();

		List<Rule> currentQuery = new ArrayList<Rule>();
		currentQuery.addAll(ruleMap.get(queryAtom));

		visited.add(queryAtom);
		// See if unfolding this rule gives us a recursive query
		Map<Rule,Boolean> cache = new HashMap<Rule,Boolean>();
		for (Rule r: ruleMap.get(queryAtom))
			if (isRecursive(r, visited, ruleMap, cache))
				throw new RecursionException("Recursive rule " + r.toString());


		// Create the initial provenance node
		for (Rule r: currentQuery) {
			List<ProvenanceNode> children = new ArrayList<ProvenanceNode>();
			prov.put(r.getHead(), new ProvIdbNode(r.getHead().getRelationContext().toString(), children, semiringName, r.getHead().getRelationContext().isMapping()));
		}

		Map<Rule,List<Atom>> extraAtoms = new HashMap<Rule,List<Atom>>();
		Calendar before = Calendar.getInstance();
		if(BFS)
			expandQueryBFS(currentQuery, ruleMap, prov, provenanceRelations, extraAtoms, semiringName, assgnExpr);
		else
			expandQueryDFS(currentQuery, ruleMap, prov, provenanceRelations, extraAtoms, semiringName, assgnExpr);
		
		Calendar after = Calendar.getInstance();
		long time = after.getTimeInMillis() - before.getTimeInMillis();
		System.out.println("EXP: EXPANSION TIME: " + time + " msec");

		for (Rule r: currentQuery)
			if (extraAtoms.containsKey(r))
				r.getBody().addAll(extraAtoms.get(r));

		//		Get rid of redundant identical/complementary atoms, introduced by the datalog unfolding when applied on mappings
		//		Do this at the end (i.e., here) so that it doesn't interfere with provenance expression construction
		for (Rule r : currentQuery)
			r.minimize();

		List<Rule> uniqueRules = new ArrayList<Rule>();
		for(Rule r : currentQuery){
			if(!uniqueRules.contains(r)){
				uniqueRules.add(r);
			}
		}
		return uniqueRules;
	}

	/**
	 * Returns true if the current rule, when unfolded one step, contains recursion
	 * 
	 * @param r
	 * @param visited
	 * @param ruleMap
	 * @return
	 */
	private static boolean isRecursive(Rule r, Set<String> visited, Map<String,List<Rule>> ruleMap, Map<Rule,Boolean> cache) {
		if (cache.containsKey(r))
			return cache.get(r);
		for (Atom a : r.getBody()) {
			List<Rule> refs = ruleMap.get(a.getRelationContext().toString());

			if (refs != null && refs.size() > 0) {
				for (Rule rul : refs)
					// Self-recursive rule
					for (Atom b : r.getBody()) {
						if (rul.getHead().getRelationContext().equals(b.getRelationContext().toString())) {
							cache.put(r, true);
							return true;
						}

						if (visited.contains(b.getRelationContext().toString())) {
							cache.put(r, true);
							return true;
						}

						Set<String> visited2 = new HashSet<String>();
						visited2.addAll(visited);
						if (ruleMap.get(b.getRelationContext().toString()) != null) {
							for (Rule r2: ruleMap.get(b.getRelationContext().toString()))
								if (isRecursive(r2, visited2, ruleMap, cache)) {
									cache.put(r, true);
									return true;
								}
						} //else
						//							System.err.println("WARNING: DON'T KNOW ABOUT " + b.getRelationContext().toString());
					}
			}
		}
		cache.put(r, false);
		return false;
	}

	/**
	 * Simple test to determine that a rule's atoms all have provenance nodes
	 * 
	 * @param r
	 * @param prov
	 */
	public static void testProvenance(Rule r, Map<Atom,ProvenanceNode> prov) {
		if (prov.get(r.getHead()) == null)
			System.err.println("Head not bound to provenance: " + r.toString());

		Set<Atom> existingAtoms = new HashSet<Atom>();
		for (Atom a : r.getBody()) {

			if (prov.get(a) == null)
				System.err.println("Body atom " + a.toString() + " not bound to provenance: " + r.toString());
		}
	}

	/**
	 * Copy the provenance from Rule 0 to all of the other rules.  ASSUMES THE PREVIOUS VERSION
	 * OF THE RULE ONLY DIFFERS AT THE IDB.
	 * 
	 * @param ruleSet
	 * @param atomPos
	 * @param numAtoms
	 * @param prov
	 */
	public static void copyProvenanceToMoreRules(List<Rule> ruleSet, int atomPos, int numAtoms,
			Map<String,List<Rule>> ruleMap,
			Map<Atom,ProvenanceNode> prov, ProvIdbNode baseNode, Rule base,
			String semiringName, String assgnExpr, boolean minimize) {
		int offset = numAtoms - atomPos;

		// For the item at the earliest position, we need to do something else:
		// that IDB got mapped into something different for each rule
		Atom atomOfInterest = base.getBody().get(atomPos);

		testProvenance(base, prov);
		for (int i = 0; i < ruleSet.size(); i++) {
			Rule r = ruleSet.get(i);
			int before = r.getBody().size();
			boolean shorter = false;
			List<Atom> discarded = new ArrayList<Atom>();
			List<Atom> newAtoms = new ArrayList<Atom>();
			if(minimize)
				newAtoms = r.mergeComplementaryProvRelAtoms(discarded);
			int after = r.getBody().size();
			int decrease = before - after;
			int newOffset = offset - decrease;

			// Copy the first rule's provenance tree
			ProvenanceNode n = baseNode.deepCopy();

			// Now bind each atom "after" our IDB to the appropriate position
			for (int a = after - newOffset + 1; a < after; a++) {
				ProvenanceNode correspondingNode = baseNode.getCorresponding(prov.get(base.getBody().get(a + 
						base.getBody().size() - after)), n);

				if (prov.get(r.getBody().get(a)) != null){
					System.err.println("WARNING: " + r.getBody().get(a) + " already has provenance to be replaced!");
					System.err.println("OLD provenance: " + prov.get(r.getBody().get(a)));
					System.err.println("NEW provenance: " + correspondingNode);
				}
				prov.put(r.getBody().get(a), correspondingNode);
			}

			// Expand the current IDB according to the rule
			ProvenanceNode recentlyExpandedIdb = 
				baseNode.getCorresponding(prov.get(atomOfInterest), n);

			expandProvenanceForIdb(r, atomPos, before - offset, ruleMap, prov, (
					ProvIdbNode)recentlyExpandedIdb, semiringName, assgnExpr,
					newAtoms, discarded);

			// Copy the bindings for the other (unexpanded) IDBs
			for (int a = 0; a < atomPos; a++) {
				// For the item at the earliest position, we need to do something else:
				// that IDB got mapped into something different for each rule
				ProvenanceNode correspondingNode = 
					baseNode.getCorresponding(prov.get(base.getBody().get(a)), n);

				prov.put(r.getBody().get(a), correspondingNode);
			}

//			Add an entry for the first atom, pointing to the merged one
//			No need to do so for the second one - it is there already
			
			if(newAtoms.size() > 0)
				prov.put(discarded.get(0), prov.get(newAtoms.get(0)));

//			for(Atom a : discarded){
//				prov.remove(a);
//			}
			
//			for(Atom b : newAtoms){
//				for(Atom a : discarded){
//					if(a.getRelationContext().equals(b.getRelationContext())){
//						
//						ProvenanceNode old = n.getCorresponding(prov.get(a), n);
//						old = prov.get(b);
////						ProvenanceNode p = prov.get(b);
////						prov.put(a, p);
//					}
//				}
//			}
			prov.put(r.getHead(), n);
			testProvenance(r, prov);
		}
	}

	public static void expandProvenanceForIdb(Rule r, int atomPos, int endPos,
			Map<String,List<Rule>> ruleMap,
			Map<Atom,ProvenanceNode> prov, ProvIdbNode oldIdbProv,
			String semiringName, String assgnExpr) {
		List<Atom> newAtoms = new ArrayList<Atom>();
		List<Atom> discarded = new ArrayList<Atom>();
		
		expandProvenanceForIdb(r, atomPos, endPos, ruleMap, prov, oldIdbProv, semiringName, assgnExpr, newAtoms, discarded);
	}
			
	
	/**
	 * When an IDB is being expanded, we want to add provenance nodes for
	 * each of its subnodes into the provenance tree corresponding to the IDB.
	 * 
	 * @param r
	 * @param atomPos
	 * @param endPos
	 * @param ruleMap
	 * @param prov
	 * @param oldIdbProv
	 */
	public static void expandProvenanceForIdb(Rule r, int atomPos, int endPos,
			Map<String,List<Rule>> ruleMap,
			Map<Atom,ProvenanceNode> prov, ProvIdbNode oldIdbProv,
			String semiringName, String assgnExpr, List<Atom> newAtoms, List<Atom> discarded) {

		// We need to add children the IdbNode for oldIdbProv for all of the
		// expanded atoms
		ProvenanceNode p2;
		Atom at;
		for (int a = endPos; a >= atomPos; a--) {
			at = r.getBody().get(a);

			if (ruleMap.containsKey(at.getRelationContext().toString())) {
				if(newAtoms.size() > 0 && oldIdbProv != null){
					List<ProvenanceNode> siblings = oldIdbProv.getParent().getChildren();
					int i = 0;
					for(; i < siblings.size() && siblings.get(i) != oldIdbProv; i++);
					if(i < siblings.size() - 1){
						ProvenanceNode nextSibling = oldIdbProv.getParent().getChildAt(i + 1);
						p2 = nextSibling.replaceChild(new ProvFakeNode(semiringName), 0);
					}else{
						p2 = new ProvIdbNode(at.getRelationContext().toString(), semiringName, at.getRelationContext().isMapping());
					}
				}else{
					p2 = new ProvIdbNode(at.getRelationContext().toString(), semiringName, at.getRelationContext().isMapping());
				}
			} else {
				SortedSet<Integer> keys = at.getRelation().getKeyCols();
				List<AtomArgument> args = new ArrayList<AtomArgument>();
				//				if(keys!=null){
				//					for (Integer inx : keys)
				//						args.add(at.getValues().get(inx.intValue()));
				for(int j = 0; j < at.getValues().size(); j++){
					if(keys!=null && keys.contains(j)){
						args.add(at.getValues().get(j));
					}else{
						args.add(at.getValues().get(j));
						//							args.add(new AtomVariable(Mapping.getFreshAutogenVariableName()));
					}

					//					}
				}
				p2 = new ProvEdbNode(at.getRelationContext().toString(), args, semiringName, assgnExpr);
			}
			prov.put(at, p2);
			if (oldIdbProv != null)
				oldIdbProv.prependChild(p2);
		}
	}

	/**
	 * Takes a rule and unfolds each atom at most once
	 * 
	 * @param query
	 * @param atomName
	 * @param ruleMap
	 * @return
	 */
	private static List<Rule> expandRuleOneLevelDFS(Rule query, String atomName, 
			Map<String,List<Rule>> ruleMap,
			Map<Atom,ProvenanceNode> prov, Set<String> provenanceRelations, 
			Map<Rule,List<Atom>> extraAtoms,
			String semiringName, String assgnExpr) {
		List<Rule> results = new ArrayList<Rule>();

		results.add(query);

		boolean didSomething = false;

		// Go through the atoms in the original rule and expand one atom
		// at a time, from right to left
		int maxI = query.getBody().size();
		Set<Rule> alreadyExpanded = new HashSet<Rule>();
		for (int i = maxI - 1; i >= 0; i--) {
			for (int j = 0; j < results.size(); ) {
				Rule r = results.get(j);
				ProvenanceNode p2 = prov.get(r.getHead());
				Atom a = r.getBody().get(i);

				// Do a substitution -- from this, we get a rule SET
				if (!alreadyExpanded.contains(r) &&
						a.getRelationContext().toString().equals(atomName) && ruleMap.get(atomName) != null) {

					List<Rule> newRules;
					if (ruleMap.get(atomName) != null){
						newRules = r.substituteAtom(i, ruleMap.get(atomName), false);
					}else
						newRules = new ArrayList<Rule>();

					for (Rule r2: newRules)
						alreadyExpanded.add(r2);

					// Copy extra atoms associated with original rule to the new rules
					if (extraAtoms.containsKey(r))
						for (Rule r2: newRules) {
							extraAtoms.put(r2, new ArrayList<Atom>());
							extraAtoms.get(r2).addAll(extraAtoms.get(r));
						}

					// If it's a provenance relation, we add it back to the end
					// and save the fact that it's been used by these rules
					if (provenanceRelations.contains(atomName)) {
						for (Rule rul : newRules) {
							if (!extraAtoms.containsKey(rul)) {
								extraAtoms.put(rul, new ArrayList<Atom>());
							}
							extraAtoms.get(rul).add(a);
						}
					}

					if (newRules.size() > 0) {
						didSomething = true;
						// Replace the current rule
						copyProvenanceToMoreRules(newRules, i, maxI, ruleMap, prov, (ProvIdbNode)p2, r, semiringName, assgnExpr, false);
						results.set(j, newRules.get(0));
						r = results.get(j);

						// Insert in any additional rules into our working set
						if (newRules.size() > 1) {
							for (int k = 1; k < newRules.size(); k++){
								//								results.add(j + k - 1, newRules.get(k));
								//								results.add(newRules.get(k));
								results.add(j + k, newRules.get(k));
							}

						}
						j = j + newRules.size();
					}else{
						j++;
					}
				}else{
					j++;
				}
				//				System.out.println("> " + r.toString() + "| " + prov.get(r.getHead()));
			}
		}
		if (didSomething)
			return results;
		else
			return null;
	}

	/**
	 * Takes a union of conjunctive expressions representing a query, plus
	 * a named map of edbs/idb definitions, and expands it recursively
	 * while adding provenance annotations
	 * 
	 * Uses a DFS traversal
	 * 
	 * @param query
	 * @param ruleInx -- rule to expand from (we've expanded all previous)
	 * @param atomInx -- atom to expand from in this rule (we've expanded all prev)
	 * @param ruleMap
	 * @param provMap
	 * @param visited
	 * @throws RecursionException
	 */
	private static String expandQueryDFS(List<Rule> query, Map<String,List<Rule>> ruleMap,
			Map<Atom,ProvenanceNode> prov, Set<String> provenanceRelations, Map<Rule,List<Atom>> used,
			String semiringName, String assgnExpr) 
	throws RecursionException
	{
		// Build the initial provenance mapping for the rule: IDB with a bunch of EDB/IDB children
		for (int j = 0; j < query.size(); j++) {
			Rule r = query.get(j);

			ProvenanceNode rootIdb = prov.get(r.getHead());

			expandProvenanceForIdb(r, 0, r.getBody().size() - 1, ruleMap, prov, (ProvIdbNode)rootIdb, semiringName, assgnExpr);
		}

		//		Config.setDebug(true);
		// Take the rule and expand each atom
		List<Boolean> done = new ArrayList<Boolean>();
		for (int j = 0; j < query.size(); j++) {
			done.add(new Boolean(false));
		}
		boolean changed;
		do {
			changed = false;
			for (int j = 0; j < query.size(); j++) {
				if(!done.get(j)){
					Rule r = query.get(j);

					Debug.println("Before expansion of Rule " + j + ": ");
					if (prov.get(r.getHead()) != null)
						Debug.println(r.toString() + " | " + prov.get(r.getHead()).toString());
					else
						Debug.println(r.toString() + " | ? ");

					boolean didSomething = false;
					for (int i = r.getBody().size() - 1; i >= 0; i--) {
						Atom a = r.getBody().get(i);
						String nam = a.getRelationContext().toString();

						// greg: only expands atoms of a particular relation (nam)
						List<Rule> res = expandRuleOneLevelDFS(r, nam, ruleMap, prov, provenanceRelations, used, semiringName, assgnExpr);
						//while 
						if (res != null) {
							if (!didSomething)
								Debug.println("After expansion: ");
							for (Rule r2 : res) {
								if (prov.get(r2.getHead()) != null)
									Debug.println(r2.toString() + " | " + prov.get(r2.getHead()).toString());
								else
									Debug.println(r2.toString() + " | ? ");
							}

							query.set(j, res.get(0));
							r = query.get(j);

							// Insert in any additional rules into our working set
							if (res.size() > 1) {
								for (int k = 1; k < res.size(); k++){
									if(!query.contains(res.get(k))){
										//								query.add(j + k - 1, res.get(k));
										query.add(j + k, res.get(k));
										//										query.add(res.get(k));
										done.add(j + k, false);
									}else{
										Debug.println("Unfolding produced a rule that is already there!");
									}
								}

							}
							didSomething = true;

							//						nam = r.getBody().get(i).getRelationContext().toString();
							//						res = expandRuleOneLevel(r, nam, ruleMap, prov);
							if (i > 0)
								Debug.println("...");
						}
					}
					if (didSomething)
						changed = true;
					else{
						done.set(j, new Boolean(true));
					}
				}
			}
		} while (changed);

		/* Print the rules and their provenance annotations */
		//		Debug.println(prov.toString());
		//		StringType sType = new StringType(true,true,255);
		for (Rule r: query) {
			ProvenanceNode ruleProv = prov.get(r.getHead());
			r.setProvenance(ruleProv);

			/*
			ScMappingAtomVariable pType = new ScMappingAtomVariable("__PROV");
			pType.setType(sType);

			r.getHead().getValues().add(pType);
			try {
				r.getHead().getRelation().addColVirtual("__PROV", "column", sType);
			} catch (BadColumnName b) {
				b.printStackTrace();
			}*/

			Debug.println(r.toString());
			/*
			if (prov.get(r.getHead()) != null)
				System.out.println(r.toString() + " | " + prov.get(r.getHead()).toString());
			else
				System.out.println(r.toString() + " | ? ");
			 */
		}

		return null;
	}

	/**
	 * Takes a union of conjunctive expressions representing a query, plus
	 * a named map of edbs/idb definitions, and expands it recursively
	 * while adding provenance annotations
	 * 
	 * Uses a DFS traversal
	 * 
	 * @param query
	 * @param ruleInx -- rule to expand from (we've expanded all previous)
	 * @param atomInx -- atom to expand from in this rule (we've expanded all prev)
	 * @param ruleMap
	 * @param provMap
	 * @param visited
	 * @throws RecursionException
	 */
	private static String expandQueryBFS(List<Rule> query, Map<String,List<Rule>> ruleMap,
			Map<Atom,ProvenanceNode> prov, Set<String> provenanceRelations, Map<Rule,List<Atom>> used,
			String semiringName, String assgnExpr) 
	throws RecursionException
	{
		// Build the initial provenance mapping for the rule: IDB with a bunch of EDB/IDB children
		for (int j = 0; j < query.size(); j++) {
			Rule r = query.get(j);

			ProvenanceNode rootIdb = prov.get(r.getHead());

			expandProvenanceForIdb(r, 0, r.getBody().size() - 1, ruleMap, prov, (ProvIdbNode)rootIdb, semiringName, assgnExpr);
		}

		//		Config.setDebug(true);
		// Take the rule and expand each atom
		List<Boolean> done = new ArrayList<Boolean>();
		for (int j = 0; j < query.size(); j++) {
			done.add(new Boolean(false));
		}
		boolean changed;
		do {
			changed = false;
			for (int j = 0; j < query.size(); j++) {
				if(!done.get(j)){
					Rule r = query.get(j);

					Debug.println("Before expansion of Rule " + j + ": ");
					if (prov.get(r.getHead()) != null)
						Debug.println(r.toString() + " | " + prov.get(r.getHead()).toString());
					else
						Debug.println(r.toString() + " | ? ");

					boolean didSomething = false;

					// greg: only expands atoms of a particular relation (nam)
					List<Rule> res = expandRuleOneLevelBFS(r, ruleMap, prov, provenanceRelations, used, semiringName, assgnExpr);
					//while 
					if (res != null) {
						if (!didSomething)
							Debug.println("After expansion: ");
						for (Rule r2 : res) {
							if (prov.get(r2.getHead()) != null)
								Debug.println(r2.toString() + " | " + prov.get(r2.getHead()).toString());
							else
								Debug.println(r2.toString() + " | ? ");
						}

						query.set(j, res.get(0));
						r = query.get(j);

						// Insert in any additional rules into our working set
						if (res.size() > 1) {
							for (int k = 1; k < res.size(); k++){
								if(!query.contains(res.get(k))){
									//								query.add(j + k - 1, res.get(k));
									query.add(j + k, res.get(k));
									//										query.add(res.get(k));
									done.add(j + k, false);
								}else{
									Debug.println("Unfolding produced a rule that is already there!");
								}
							}

						}
						didSomething = true;

						//						nam = r.getBody().get(i).getRelationContext().toString();
						//						res = expandRuleOneLevel(r, nam, ruleMap, prov);
					}
					if (didSomething)
						changed = true;
					else{
						done.set(j, new Boolean(true));
					}
				}
			}
		} while (changed);

		/* Print the rules and their provenance annotations */
		//		Debug.println(prov.toString());
		//		StringType sType = new StringType(true,true,255);
		for (Rule r: query) {
			ProvenanceNode ruleProv = prov.get(r.getHead());
			r.setProvenance(ruleProv);

			/*
			ScMappingAtomVariable pType = new ScMappingAtomVariable("__PROV");
			pType.setType(sType);

			r.getHead().getValues().add(pType);
			try {
				r.getHead().getRelation().addColVirtual("__PROV", "column", sType);
			} catch (BadColumnName b) {
				b.printStackTrace();
			}*/

			Debug.println(r.toString());
			/*
			if (prov.get(r.getHead()) != null)
				System.out.println(r.toString() + " | " + prov.get(r.getHead()).toString());
			else
				System.out.println(r.toString() + " | ? ");
			 */
		}

		return null;
	}

	/**
	 * Takes a rule and unfolds each atom at most once
	 * 
	 * @param query
	 * @param atomName
	 * @param ruleMap
	 * @return
	 */
	private static List<Rule> expandRuleOneLevelBFS(Rule query, 
			Map<String,List<Rule>> ruleMap,
			Map<Atom,ProvenanceNode> prov, Set<String> provenanceRelations, 
			Map<Rule,List<Atom>> extraAtoms,
			String semiringName, String assgnExpr) {
		List<Rule> results = new ArrayList<Rule>();

		results.add(query);

		boolean didSomething = false;

		// Go through the atoms in the original rule and expand one atom
		// at a time, from right to left
		int maxI = query.getBody().size();
		//		Set<Rule> alreadyExpanded = new HashSet<Rule>();
		for (int i = maxI - 1; i >= 0; i--) {
			for (int j = 0; j < results.size(); ) {
				Rule r = results.get(j);
				int sizeBeforeUnfolding = r.getBody().size();
				ProvenanceNode p2 = prov.get(r.getHead());
				Atom a = r.getBody().get(i);
				String atomName = a.getRelationContext().toString();

				// Do a substitution -- from this, we get a rule SET

				List<Rule> newRules;
				if (ruleMap.get(atomName) != null){
					newRules = r.substituteAtom(i, ruleMap.get(atomName), false);
				}else{
					newRules = new ArrayList<Rule>();
				}


				// Copy extra atoms associated with original rule to the new rules
				if (extraAtoms.containsKey(r))
					for (Rule r2: newRules) {
						extraAtoms.put(r2, new ArrayList<Atom>());
						extraAtoms.get(r2).addAll(extraAtoms.get(r));
					}

				// If it's a provenance relation, we add it back to the end
				// and save the fact that it's been used by these rules
				if (provenanceRelations.contains(atomName)) {
					for (Rule rul : newRules) {
						if (!extraAtoms.containsKey(rul)) {
							extraAtoms.put(rul, new ArrayList<Atom>());
						}
						extraAtoms.get(rul).add(a);
					}
				}

				if (newRules.size() > 0) {
					didSomething = true;

					// Replace the current rule
//					copyProvenanceToMoreRules(newRules, i, maxI, ruleMap, prov, (ProvIdbNode)p2, r, semiringName, assgnExpr, true);
					copyProvenanceToMoreRules(newRules, i, sizeBeforeUnfolding, ruleMap, prov, (ProvIdbNode)p2, r, semiringName, assgnExpr, true);

					results.set(j, newRules.get(0));
					r = results.get(j);

					// Insert in any additional rules into our working set
					if (newRules.size() > 1) {
						for (int k = 1; k < newRules.size(); k++){
							//								results.add(j + k - 1, newRules.get(k));
							//								results.add(newRules.get(k));
							results.add(j + k, newRules.get(k));
						}

					}
					j = j + newRules.size();
				}else{
					j++;
				}
				//				System.out.println("> " + r.toString() + "| " + prov.get(r.getHead()));
			}
		}
		if (didSomething)
			return results;
		else
			return null;
	}

}
