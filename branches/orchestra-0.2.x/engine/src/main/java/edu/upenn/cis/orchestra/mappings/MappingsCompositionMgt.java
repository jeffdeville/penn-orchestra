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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import edu.upenn.cis.orchestra.mappings.exceptions.CompositionException;
import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datamodel.AbstractRelation;
import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.Schema;

public class MappingsCompositionMgt 
{
	/**
	 * Compose the rules to remove all the virtual relations (not 
	 * materialized relations).
	 * Based on the technique of basic composition [Nash et al, PODS05] 
	 * @param rules Rules to be composed
	 */
	public static void composeMappings (List<Mapping> mappings, Map<String, Schema> builtInSchemas)
	{
		// Get the list of virtual relations (not materialized, thus they can be removed)
		List<Rule> rules = MappingsInversionMgt.splitMappingsHeads(mappings, builtInSchemas);
		List<Relation> virtRels = getAllVirtualRelations(rules);
		
		// For each of these virtual relations: compose the rules to make it disappear
		for (Relation rel : virtRels)
			eliminateVirtualRel(rel, mappings, rules);
		
		// Remove the equalities introduced by the composition
		eliminateEqualities(mappings);
	}
	
	public static void composeRules (List<Rule> rules)
	{
		List<Mapping> mappings = new ArrayList<Mapping>();
		mappings.addAll(rules);
		// Get the list of virtual relations (not materialized, thus they can be removed)
		List<Relation> virtRels = getAllVirtualRelations(rules);
		
		// For each of these virtual relations: compose the rules to make it disappear
		for (Relation rel : virtRels)
			eliminateVirtualRel(rel, mappings, rules);
		
		// Remove the equalities introduced by the composition
		eliminateEqualities(mappings);
		rules.clear();
		for(Mapping m : mappings)
			rules.add((Rule)m);
	}

	
	/**
	 * After the composition: remove the equalities by substituting variables in the 
	 * head and body of each rule.
	 * Compatibility of equalities parameters have already been checked when creating the 
	 * equalities atoms.
	 * @param rules Rules for which to remove equalities atoms.
	 */
	private static void eliminateEqualities (List<Mapping> rules)
	{
		for (Mapping r : rules)
			r.eliminateEqualities();
	}
	
	// TODO if cannot be composed : exception!??! => Would need to track provenance or compositions to make sure that each rule was rewritten!
	/**
	 * Remove all occurences of the virtual (not materialized) relation <code>rel</code>
	 * in the rules by composing with other rules
	 * @param rel Virtual relation to remove by composing
	 * @param rules 
	 */
	private static void eliminateVirtualRel (Relation rel, List<Mapping> mappings, List<Rule> rules)
	{
		List<Rule> defs = new ArrayList<Rule> ();
		List<Rule> tgds = new ArrayList<Rule> ();
		getDefsAndTgds(rel, rules, defs, tgds);
		
//		rules.clear();
		mappings.clear();
		for (Rule rule : tgds)
			eliminateVirtualRel(rule.getParentMapping(), 0, rel, defs, mappings);
//			eliminateVirtualRel(rule, 0, rel, defs, rules);
	}
	
	/**
	 * For the atom of indice indAtom of rule r, replace the virtual relation 
	 * vRel (if it is used in this atom) by composing with the definition rules.
	 * All possible compositions are tried further (with the next atoms) and added 
	 * to res if it succeeded until the next atom  
	 * @param r Rule to compose with defs to remove occurences of vRel
	 * @param indAtom Current indice of the atom in recursion
	 * @param vRel Virtual relation to remove with composition
	 * @param defs Definition rules for the relation vRel
	 * @param res Results list to which the composition must be added 
	 */
	private static void eliminateVirtualRel (Mapping m, 
													int indAtom, 
													AbstractRelation vRel, 
													List<Rule> defs, 
													List<Mapping> res)
	{		
		// If we've composed all the possible atoms, r is fine and can be sent as a result
		if (indAtom == m.getBody().size())
			res.add (m);
		else
		{
			// If the current atom is related to the virtual relation to remove by composition
			// then try to compose with all existing definitions
			if (m.getBody().get(indAtom).getRelation()==vRel)
			{
				// Try to compose with all definitions
				// Note: If there is no definition, this rule will just be dropped
				for (Rule def : defs)
				{
					try
					{
						Mapping rComp = m.composeWith(indAtom, def);
						// Compose the remaining atoms if necessary...
						eliminateVirtualRel(rComp, indAtom+1, vRel, defs, res);
					} catch (CompositionException ex)
					{
						// Composition exception can occur if variables used in these atoms 
						// are not compatible...
						// We just move on (copyright TJ 2006)!
					}
				}
					
			}
			// Or we can just keep this atom as it is, and continue further 
			else
				eliminateVirtualRel(m, indAtom+1, vRel, defs, res);
			
		}
	}
	
	
	
	/**
	 * For a given (virtual) relation, extract the list of defs rules (rules with this relation 
	 * in the head) and tgds (rules without this relation in the head)
	 * @param rel Virtual relation for which to proceed
	 * @param rules Current set of rules (used in composition)
	 * @param defs Defs rules will be added to this list. List will be cleared first, must no be null!
	 * @param tgds Tgds rules will be added to this list. List will be cleared first, must no be null!
	 */
	private static void getDefsAndTgds (Relation rel, List<Rule> rules, List<Rule> defs, List<Rule> tgds)
	{
		defs.clear(); 
		tgds.clear();
		
		assert(!rel.isMaterialized()) : "To be removed in composition a relation must be virtual";
		
		for (Rule rule : rules)
			if (rule.getHead().getRelation()==rel)
				defs.add (rule);
			else
				tgds.add (rule);
	}
	
	/**
	 * From a given list of rules extract all the virtual relations.<BR>
	 * These relations will have to be removed during composition
	 * @param rules Rules from which to extract virtual relations
	 * @return List (actually a set) of virtual relations
	 */
	private static List<Relation> getAllVirtualRelations (List<Rule> rules)
	{
		List<Relation> res = new ArrayList<Relation> ();
		
		for (Rule rule : rules)
		{			
			if (!rule.getHead().getRelation().isMaterialized())
				if (!res.contains(rule.getHead().getRelation()))
					res.add(rule.getHead().getRelation());				
			for (Atom atom : rule.getBody())
				if (!atom.getRelation().isMaterialized())
					if (!res.contains(atom.getRelation()))
						res.add(atom.getRelation());				
		}
		return res;
	}


}
