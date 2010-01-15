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
package edu.upenn.cis.orchestra.provenance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datalog.atom.AtomArgument;
import edu.upenn.cis.orchestra.datalog.atom.AtomVariable;
import edu.upenn.cis.orchestra.datalog.atom.Atom.AtomType;
import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.RelationContext;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.TypedRelation;
import edu.upenn.cis.orchestra.deltaRules.DeltaRuleGen;
import edu.upenn.cis.orchestra.mappings.Rule;

/**
 * Unfolding class for mappings (multiple atoms in head).
 * 
 * @author gregkar
 *
 */

public class OuterJoinUnfolder {

	public static List<Rule> unfoldOuterJoins(List<Rule> inputRules, List<RelationContext> outerJoinRelations, 
			AtomType baseType, AtomType resType, Map<String, Schema> builtInSchemas){
		List<Rule> ret = new ArrayList<Rule>();

		if(outerJoinRelations.size() == 0){
			return inputRules;
		}else{ // Some ASRs exist, use them!
			//   Assume no overlapping outer joins, so it is OK to process them one at a time, in a greedy fashion
			List<Rule> allRules = new ArrayList<Rule>();
			allRules.addAll(inputRules);

			for(RelationContext relCtx : outerJoinRelations){
				ProvenanceRelation ojRel = (ProvenanceRelation)relCtx.getRelation();
				List<Mapping> invOJMappings = ojRel.getInverseOuterJoinMappings(builtInSchemas);

				for(Mapping ojDef : invOJMappings){
					for(int i = 0; i < allRules.size();){
						Rule ar = allRules.get(i);
						List<Rule> unfoldedRules = unfoldOuterJoinMapping(ar, ojDef, baseType, resType);
						if(unfoldedRules.size() > 0){
							//									someUnfolded = true;
							allRules.remove(i);
							allRules.addAll(unfoldedRules);
							//										ret.addAll(unfoldedRules);
							//										break;
						}else{
							i++;
						}
					}
				}
			}
			ret.addAll(allRules);
			return ret;
		}
		//			May also need to "mess" with types of atoms, to set _NEW and _INV appropriately,
		//			at least for the case of "one-step-at-a-time" query processing
	}

	public static void findEquivalentVarSubstitutions(List<Atom> ruleBody, List<Atom> defHead, Map<String, AtomArgument> partialVarMap, List<Map<String, AtomArgument>> completedMaps, List<List<Atom>> completedRemainingTargetAtoms){
		if(defHead.size() > 0){
			Atom defHeadAtom = defHead.get(0);
			//			Activate "matched" flag if only one homomorphism may exist, or if only first one is needed/suffices
			//			boolean matched = false;
			//			for(int j = 0; j < rule.size() && !matched; j++){
			for(int j = 0; j < ruleBody.size(); j++){	
				Atom ruleBodyAtom = ruleBody.get(j);

				if(ruleBodyAtom.getRelationContext().equals(defHeadAtom.getRelationContext())){
					boolean bpossible = true;
					Map<String, AtomArgument> tempMap = new HashMap<String, AtomArgument>();
					tempMap.putAll(partialVarMap);

					//					defHead of outer join definition only has variables
					//					no need for special cases for constants, skolems etc

					for(int i = 0; i < defHeadAtom.getVariables().size() && bpossible; i++){
						AtomVariable headVar = defHeadAtom.getVariables().get(i);
						AtomVariable bodyVar = ruleBodyAtom.getVariables().get(i);

						if(tempMap.containsKey(headVar) && !tempMap.get(headVar.getName()).equals(bodyVar)){
							bpossible = false;
						}else{
							tempMap.put(headVar.getName(), bodyVar);
						}
					}
					if(bpossible){
						//						matched = true;
						List<Atom> remainingDefHead = new ArrayList<Atom>();
						List<Atom> remainingRuleBody = new ArrayList<Atom>();

						//						First head atom has been matched
						for(int k = 1; k < defHead.size(); k++){
							remainingDefHead.add(defHead.get(k));
						}

						//						"Image" of first head atom is body atom j
						for(int k = 0; k < ruleBody.size(); k++){
							if(k != j){
								remainingRuleBody.add(ruleBody.get(k));
							}
						}
						findEquivalentVarSubstitutions(remainingRuleBody, remainingDefHead, tempMap, completedMaps, completedRemainingTargetAtoms);
					}
				}
			}
		}else{
			completedMaps.add(partialVarMap);
			completedRemainingTargetAtoms.add(ruleBody);
		}
	}

	public static List<Rule> unfoldOuterJoinMapping(Rule rule, Mapping def, AtomType baseType, AtomType resType){
		List<Rule> ret = new ArrayList<Rule>();
		List<Map<String, AtomArgument>> completedMaps = new ArrayList<Map<String,AtomArgument>>();
		Map<String, AtomArgument> partialVarMap = new HashMap<String, AtomArgument>();
		List<List<Atom>> completedRemainingTargetAtoms = new ArrayList<List<Atom>>();

		findEquivalentVarSubstitutions(rule.getBody(), def.getMappingHead(), partialVarMap, completedMaps, completedRemainingTargetAtoms);

		for(int i = 0; i < completedMaps.size(); i++){
			Map<String, AtomArgument> m = completedMaps.get(i);
			List<Atom> remainingTargetAtoms = completedRemainingTargetAtoms.get(i);

			for(Atom a : def.getBody()){
				List<AtomArgument> newArgList = new ArrayList<AtomArgument>();
				for(AtomArgument arg : a.getValues()){
					if(arg instanceof AtomVariable){
						AtomVariable v = (AtomVariable)arg;
						AtomArgument vimg = m.get(v.getName());

						if(vimg != null)
							newArgList.add(vimg);
						else
							newArgList.add(v);
					}else{
						newArgList.add(arg);
					}
				}
				Atom unfoldedAtom = new Atom(a.getRelationContext(), newArgList);
				remainingTargetAtoms.add(unfoldedAtom);
			}
			Rule unfoldedRule = new Rule(rule.getHead().deepCopy(), remainingTargetAtoms, rule.getParentMapping(), rule.getBuiltInSchemas());
			unfoldedRule.setProvenance(rule.getProvenance());
			unfoldedRule.setOnlyKeyAndNulls();
			unfoldedRule.getHead().setType(resType);
			for(Atom a : unfoldedRule.getBody())
				a.setType(baseType);
			ret.add(unfoldedRule);
		}

		return ret;
	}





	/** @deprecated */
	public static List<Rule> oldUnfoldOuterJoins(List<Rule> inputRules, List<RelationContext> outerJoinRelations, 
			AtomType baseType, AtomType resType, Map<String, Schema> builtInSchemas){

		if(outerJoinRelations.size() == 0){
			return inputRules;
		}else{ // Some ASRs exist, use them!
			List<Rule> inverseOuterJoinRules = getInverseOuterJoinRules(baseType, outerJoinRelations, builtInSchemas);
			List<Rule> unusedDefs = new ArrayList<Rule>();

			//			Replace Mapping relation atoms _NEW with Join relations where they appear
			List<Rule> allInvRules = DeltaRuleGen.unfoldProvDefs(inverseOuterJoinRules, inputRules, unusedDefs, true);

			//			Replace Mapping relation atoms _INV with Join relations where they appear
			//			The following unfolding is more efficient if done in the reverse order of mappings in ASRs 
			//			It is not necessary if the input rules have been unfolded already, but it is required for 
			//			the (one-step-at-a-time) derivability program ...
			Map<TypedRelation, List<Rule>> defsMap = Rule.list2map(allInvRules); 

			for(RelationContext rel : outerJoinRelations){
				ProvenanceRelation provRel = (ProvenanceRelation)rel.getRelation();

				for(int i = provRel.getRels().size(); i > 0; i--){
					ProvenanceRelation p = provRel.getRels().get(i-1);
					TypedRelation key = new TypedRelation(p, resType);
					List<Rule> keyDefs = defsMap.get(key);

					List<Rule> unfoldedDefs = DeltaRuleGen.unfoldProvDefs(keyDefs, allInvRules, unusedDefs, true);
					for(Rule rr : unfoldedDefs){
						rr.minimize();
					}
					defsMap = Rule.list2map(unfoldedDefs);
					defsMap.remove(key);
					allInvRules = Rule.map2list(defsMap);
				}
			}
			return allInvRules;
		}
	}

	/** @deprecated */
	protected static List<Rule> getInverseOuterJoinRules(AtomType type, List<RelationContext> outerJoinRelations, Map<String, Schema> builtInSchemas) {
		List<Rule> ret = new ArrayList<Rule>();
		for(RelationContext relCtx : outerJoinRelations){
			ProvenanceRelation provRel = (ProvenanceRelation)relCtx.getRelation();
			//			List<Rule> inv = provRel.invertOJMappings(getMappingDb());
			List<Rule> inv = provRel.getInverseOuterJoinRules(builtInSchemas);
			for(Rule r : inv){
				r.getHead().setType(type);
				for(Atom a : r.getBody()){
					a.setType(type);
				}
				ret.add(r);
			}
		}
		return ret;
	}

	/** @deprecated */
	protected static List<Rule> getInverseOuterUnionRules(AtomType type, List<RelationContext> outerUnionRelations, Map<String, Schema> builtInSchemas) {
		List<Rule> ret = new ArrayList<Rule>();
		for(RelationContext relCtx : outerUnionRelations){
			ProvenanceRelation provRel = (ProvenanceRelation)relCtx.getRelation();
			//			List<Rule> inv = provRel.invertOJMappings(getMappingDb());
			List<Rule> inv = provRel.getInverseOuterUnionRules(builtInSchemas);
			for(Rule r : inv){
				r.getHead().setType(type);
				for(Atom a : r.getBody()){
					a.setType(type);
				}
				ret.add(r);
			}
		}
		return ret;
	}


}
