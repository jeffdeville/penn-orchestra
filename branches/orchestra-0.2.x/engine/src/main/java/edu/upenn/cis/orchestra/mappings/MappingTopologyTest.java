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
package edu.upenn.cis.orchestra.mappings;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import edu.upenn.cis.orchestra.Debug;
import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datalog.atom.AtomArgument;
import edu.upenn.cis.orchestra.datalog.atom.AtomVariable;
import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.RelationContext;

/**
 * @author zives
 *
 */
public class MappingTopologyTest {
	
	public static boolean isWeaklyAcyclic(Collection<Mapping> mappings) {
		return isWeaklyAcyclic(mappings, false);
	}

	/**
	 * Test the set of mappings for weak acyclicity
	 * 
	 * @param mappings
	 * @param nullsMarked Flag indicating that we have already marked the labeled nulls
	 * @return
	 */
	public static boolean isWeaklyAcyclic(Collection<Mapping> mappings, boolean nullsMarked) {
		if (!nullsMarked)
			markLabeledNulls(mappings);
		
		for (Mapping m : mappings) {
			// Determine, for the mapping, which arguments are labeled nulls (dependent on
			// a Skolem somewhere)
			Set<AtomArgument> skolemArgs = new HashSet<AtomArgument>();
			for (Atom a: m.getBodyWithoutSkolems()) {
				for (int inx = 0; inx < a.getValues().size(); inx++) {
					if (a.isNullable(inx))
						skolemArgs.add(a.getValues().get(inx));
				}
			}

			// If we have a Skolem over a Skolem, then we have a non-weakly acyclic case 
			for (Atom a: m.getSkolemAtoms()) {
				for (AtomArgument arg: a.getValues())
					if (skolemArgs.contains(arg))
						return false;
			}
		}
		return true;
	}
	
	/**
	 * TODO: Compute the set of atoms that are referenced recursively
	 * 
	 * @param mappings
	 * @return
	 */
	public static Set<Atom> getRecursivelyUsedAtoms(Collection<Mapping> mappings) {
		Set<Atom> ret = new HashSet<Atom>();
		
		return ret;
	}
	
	/**
	 * Annotate the atoms in the mappings with information about whether they
	 * are labeled-nullable
	 * 
	 * @param mappings
	 */
	public static void markLabeledNulls(Collection<Mapping> mappings) {
		boolean changed = false;
		
		Debug.println("Marking labeled nulls...");
		
		HashMap<RelationContext, Set<Atom>> headAtoms = new HashMap<RelationContext, Set<Atom>>();
		HashMap<RelationContext, Set<Atom>> bodyAtoms = new HashMap<RelationContext, Set<Atom>>();

		// Iterate over each mapping.  For each existential in the head,
		// set the variable to be labeled-nullable
		for (Mapping m : mappings) {
			
			// First create some auxiliary info so we know which mappings
			// refer to whom
			for (Atom a : m.getBody()) {
				RelationContext cx = a.getRelationContext();
				if (!bodyAtoms.containsKey(a)) 
					bodyAtoms.put(cx, new HashSet<Atom>());
				bodyAtoms.get(cx).add(a);
			}
			for (Atom a : m.getMappingHead()) {
				RelationContext cx = a.getRelationContext();
				if (!headAtoms.containsKey(a)) 
					headAtoms.put(cx, new HashSet<Atom>());
				headAtoms.get(cx).add(a);
			}
			
			for (Atom a: m.getMappingHead()) {
				int inx = 0;
				for (AtomArgument arg : a.getValues()) {
					if ((arg instanceof AtomVariable) && !m.getAllBodyVariables().contains(arg)) {
						a.setIsNullable(inx);
						changed = true;
					}
					inx++;
				}
			}
		}
		while (changed) {
			changed = false;

			// Propagate the labeled-nullable-ness from mapping head to mapping head
			// Propagate the labeled-nullable-ness from mapping head to other mapping body
			for (Mapping m : mappings) {
				for (Atom a: m.getMappingHead()) {
					int inx = 0;
					
					for (AtomArgument arg : a.getValues()) {
						if (a.isNullable(inx)) {
							// Look up other mappings with same
							// atom, set the same position to nullable
							for (Atom a2: headAtoms.get(a.getRelationContext()))
								if (a2 != null && !a2.isNullable(inx)) {
									a2.setIsNullable(inx);
									changed = true;
								}
							
							if (bodyAtoms.get(a.getRelationContext()) != null)
								for (Atom a2: bodyAtoms.get(a.getRelationContext()))
									if (a2 != null && !a2.isNullable(inx)) {
										a2.setIsNullable(inx);
										changed = true;
									}
						}
						inx++;
					}
				}
			}
					
			// Propagate the labeled-nullable-ness from body to head
			for (Mapping m : mappings) {
				for (Atom a: m.getBody()) {
					int inx = 0;
					
					for (AtomArgument arg : a.getValues()) {
						if (a.isNullable(inx)) {
							
							// Find any uses of this variable in the head
							AtomArgument arg2 = a.getVariables().get(inx);
							for (Atom headAtom : m.getMappingHead()) {
								int inx2 = headAtom.getVariables().indexOf(arg2);
								
								if (inx2 != -1 && !headAtom.isNullable(inx2)) {
									headAtom.setIsNullable(inx2);
									changed = true;
								}
									
							}
						}
						inx++;
					}
				}
			}
		}

		// Debug output
		for (Mapping m : mappings) {
			Debug.println(m.toString());
		}
	}
}
