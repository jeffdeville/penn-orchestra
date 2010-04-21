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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.w3c.dom.Document;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.datalog.DatalogProgram;
import edu.upenn.cis.orchestra.datalog.NonRecursiveDatalogProgram;
import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datalog.atom.AtomArgument;
import edu.upenn.cis.orchestra.datalog.atom.AtomVariable;
import edu.upenn.cis.orchestra.datalog.atom.Atom.AtomType;
import edu.upenn.cis.orchestra.datamodel.ITranslationRules;
import edu.upenn.cis.orchestra.datamodel.ITranslationState;
import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.RelationContext;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.TranslationState;
import edu.upenn.cis.orchestra.mappings.MappingsInversionMgt;
import edu.upenn.cis.orchestra.mappings.MappingsTranslationMgt;
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.provenance.ProvenanceRelation;
import edu.upenn.cis.orchestra.util.XMLParseException;

/**
 * Basic generator for delta rules, based on
 * knowledge of provenance relations.
 * 
 * @author zives, gkarvoun
 *
 */
public abstract class DeltaRuleGen implements IDeltaRuleGen {
	private final ITranslationRules _translationRules;
	private final Map<String, Schema> _builtInSchemas;
	private final OrchestraSystem _system;
	
	/** The generated rules. */
	protected IDeltaRules _deltaRules;
	
	/**
	 * Base abstract class for delta rule generation
	 * @param system 
	 * @param translationRules 
	 * @param builtInSchemas
	 * 
	 */
	public DeltaRuleGen (OrchestraSystem system, ITranslationRules translationRules, Map<String, Schema> builtInSchemas) 
	{
		_translationRules = translationRules;
		_builtInSchemas = builtInSchemas;
		//_provenancePrep = _dr.getProvenancePrepInfo();
		_system = system;
		createRules(_builtInSchemas);
	}

	/**
	 * Creates a new {@code DeltaRuleGen} from serialized rules. Used for testing.
	 * 
	 * @param translationRulesDoc
	 * @param builtInSchemasDoc
	 * @param system 
	 * @throws XMLParseException
	 */
	protected DeltaRuleGen(Document translationRulesDoc,
			Document builtInSchemasDoc, OrchestraSystem system) throws XMLParseException {
		this(system, deserializeTranslationState(translationRulesDoc, system), OrchestraSystem
				.deserializeBuiltInFunctions(builtInSchemasDoc));
	}
	
	/**
	 * Returns the {@code ITranslationState} represented by {@code
	 * translationStateDoc}.
	 * 
	 * @param translationStateDoc a {@code Document} produced by {@code
	 *            ITranslationState.serialize()}
	 * @param system 
	 * @return the {@code ITranslationState} represented by {@code
	 *         translationStateDoc}
	 * @throws XMLParseException 
	 */
	private static ITranslationState deserializeTranslationState(
			Document translationStateDoc, OrchestraSystem system) throws XMLParseException {
		return TranslationState.deserialize(translationStateDoc, system);
	}

	/**
	 * Create, cache a set of rules
	 * 
	 */
	protected abstract void createRules(Map<String, Schema> builtInSchemas);

	@Override
	public IDeltaRules getDeltaRules() {
		return _deltaRules;
	}
	

//	public List<Rule> getBaseInsRules ()
//	{
////	why not baseIns?
////	return _insRules;
//	return getSystem().getMappingEngine().getBaseInsertionRules();
//	}

	
	protected Map<String, Schema> getBuiltInSchemas() {
		return _builtInSchemas;
	}

	protected ITranslationRules getTranslationRules() {
		return _translationRules;
	}

	/**
	 * Get the EDB relations
	 * @return EDB relations
	 */
	protected List<RelationContext> getEdbs() 
	{
		return getTranslationRules().getEdbs(getBuiltInSchemas());
	}

	/**
	 * Get the rejection relations.
	 * 
	 * @return the rejection relations
	 */
	protected List<RelationContext> getRej() {
		return getTranslationRules().getRej(_system);
	}
	/**
	 * Get the IDB relations
	 * @return IDB relations
	 */
	protected List<RelationContext> getIdbs() 
	{
		return getTranslationRules().getIdbs(getBuiltInSchemas());
	}

	protected List<RelationContext> getMappingRelations() {
		return getTranslationRules().getRealMappingRelations();
	}

	
	  

	/**
	 * "ForProvQ" rules are meant to be used for inverse traversal of the provenance 
	 * graph, so existential variables there need not be skolemized (and new existentials
	 * from the body of the mapping (pre-inversion) are dealt with, since we are just 
	 * interested in tuples already in the database (or some particular tuple, in the case 
	 * of the provenance viewer) instead of introducing new ones (as in the Ins/Del case) 
	 *      
	 * @return
	 */

	public static List<Rule> getSource2ProvRulesForProvQ (ITranslationRules translationRules)
	{
		return subtractFakeRules(MappingsTranslationMgt.computeSource2ProvMappingsForProvQ(
				translationRules.getSource2ProvRules()));
	}    

	public static List<Rule> getProv2TargetRulesForProvQ (ITranslationRules translationRules, Map<String, Schema> builtInSchemas)
	{
		return subtractFakeRules(MappingsInversionMgt.splitMappingsHeads(
				MappingsTranslationMgt.computeProv2TargetMappingsForProvQ(
						translationRules.getProv2TargetMappings()), 
						builtInSchemas));
	}  

	

	 

	private static List<Rule> getSource2ProvRulesForIns (ITranslationRules translationRules)
	{
		return subtractFakeRules(MappingsTranslationMgt.computeSource2ProvMappingsForIns(
				translationRules.getSource2ProvRules()));
	}

	public static List<Rule> subtractFakeRules(List<Rule> allMappings)
	{
		List<Rule> realMappings = new ArrayList<Rule>();
		for(Rule r : allMappings){
			if(!r.isFakeMapping()){
				realMappings.add(r);
			}
		}
		return realMappings;
	}

	public static List<Mapping> subtractFakeMappings(List<Mapping> allMappings)
	{
		List<Mapping> realMappings = new ArrayList<Mapping>();
		for(Mapping r : allMappings){
			if(!r.isFakeMapping()){
				realMappings.add(r);
			}
		}
		return realMappings;
	}


	protected List<Rule> getSource2ProvRulesForIns ()
	{
		return subtractFakeRules(DeltaRuleGen.getSource2ProvRulesForIns(getTranslationRules()));
	}

	protected List<Rule> getIncrementallyMaintenableJoinRules ()
	{
		List<Rule> ret = new ArrayList<Rule>();
		for(RelationContext relCtx : getIncrementallyMaintenableJoinRelations()){
			ProvenanceRelation prel = (ProvenanceRelation)relCtx.getRelation();
			ret.addAll(prel.outerJoinMappingsForMaintenance(getBuiltInSchemas()));
		}
		return ret;
	}
	
	protected List<Rule> getRealOuterJoinRules ()
	{
		List<Rule> ret = new ArrayList<Rule>();
		for(RelationContext relCtx : getRealOuterJoinRelations()){
			ProvenanceRelation prel = (ProvenanceRelation)relCtx.getRelation();
			ret.addAll(prel.outerJoinMappingsForMaintenance(getBuiltInSchemas()));
		}
		return ret;
	}

	protected List<Rule> getOuterUnionRules ()
	{
		List<Rule> ret = new ArrayList<Rule>();
		for(RelationContext relCtx : getOuterUnionRelations()){
			ProvenanceRelation prel = (ProvenanceRelation)relCtx.getRelation();
			ret.addAll(prel.outerUnionMappings(getBuiltInSchemas()));
		}
		return ret;
	}

	protected List<RelationContext> getIncrementallyMaintenableJoinRelations ()
	{
		List<RelationContext> ret = new ArrayList<RelationContext>();
		
		ret.addAll(getTranslationRules().getInnerJoinRelations());
		ret.addAll(getTranslationRules().getSimulatedOuterJoinRelations());
		
		return ret;
	}

	protected List<RelationContext> getRealOuterJoinRelations ()
	{
		return getTranslationRules().getRealOuterJoinRelations();
	}
	
	protected List<RelationContext> getOuterUnionRelations ()
	{
		return getTranslationRules().getOuterUnionRelations();
	}

	protected List<Rule> getLocal2PeerRules ()
	{
		return getTranslationRules().getLocal2PeerRules();
	} 

	private List<Rule> idbDeltaApplicationRules(boolean ins, boolean skipold, 
			boolean includeKeysOnly){
		return deltaApplicationRules(getIdbs(), ins, skipold, includeKeysOnly, getBuiltInSchemas());
	}

	protected List<Rule> idbDeltaApplicationRules(boolean ins, boolean includeKeysOnly){
		return idbDeltaApplicationRules(ins, false, includeKeysOnly);
	}

	/**
	 * If DO_APPLY, "moves" NEW to NONE for all edbs, idbs and mapping rels
	 * else it just clears the NEW
	 * 
	 * These operations aren't counted in execution timing.
	 * 
	 * @return
	 */
	protected static DatalogProgram applyDeltasToBase(List<RelationContext> mappingRels,
			List<RelationContext> edbs, List<RelationContext> idbs, Map<String, Schema> builtInSchemas) {
		List<Rule> rules = new ArrayList<Rule>();
		DatalogProgram p;

		rules.addAll(moveRelationList(edbs/*getEdbs()*/, AtomType.NONE, AtomType.NEW, builtInSchemas));
		rules.addAll(moveRelationList(idbs/*getIdbs()*/, AtomType.NONE, AtomType.NEW, builtInSchemas));
		rules.addAll(moveRelationList(mappingRels/*getMappingRelations()*/, AtomType.NONE, AtomType.NEW, builtInSchemas));
		p = new NonRecursiveDatalogProgram(rules, true);


		p.omitFromCount();
		return p;
	}

	


	protected static DatalogProgram cleanupRelations(AtomType typ, List<RelationContext> mappingRels,
			List<RelationContext> edbs, List<RelationContext> idbs, Map<String, Schema> builtInSchemas) {
		List<Rule> ret = new ArrayList<Rule>();
		//List<DatalogProgram> ret = new ArrayList<DatalogProgram>();

		ret.addAll(clearRelationList(idbs, typ, builtInSchemas));

		// I think I shouldn't delete edb dels ...
		if (typ != AtomType.DEL && typ != AtomType.RCH)
			ret.addAll(clearRelationList(edbs, typ, builtInSchemas));

		if (typ != AtomType.RCH)
			ret.addAll(clearRelationList(mappingRels, typ, builtInSchemas));

		DatalogProgram p = new NonRecursiveDatalogProgram(ret,false);
		return p;
	}

	protected static List<Rule> clearRelationList(List<RelationContext> rels, AtomType type,
			Map<String, Schema> builtInSchemas){
		List<Rule> ret = new ArrayList<Rule>();
		//List<DatalogProgram> ret = new ArrayList<DatalogProgram>();

		for(RelationContext rel : rels){
			Rule r = relCleanup(rel, type, builtInSchemas);
			ret.add(r);//new SingleRuleDatalogProgram(r));
		}
		return ret;
	}

	protected static List<Rule> copyRelationList(List<RelationContext> rels, AtomType headType, 
			AtomType bodyType, Map<String, Schema> builtInSchemas){
		List<Rule> ret = new ArrayList<Rule>();
		//List<DatalogProgram> ret = new ArrayList<DatalogProgram>();
		for(RelationContext rel : rels){
			Rule r = relCopy(rel, headType, bodyType, builtInSchemas);
			ret.add(r);//new SingleRuleDatalogProgram(r));
		}
		return ret;
	}

	

	protected static List<Rule> deltaApplicationRule(RelationContext relation, boolean pos, AtomType type, 
			boolean skipold, boolean includeKeysOnly, Map<String, Schema> builtInSchemas) {
		return deltaApplicationRule(relation, pos, AtomType.NEW, AtomType.NONE, type, skipold, includeKeysOnly, true, builtInSchemas);
	}

	protected static List<Rule> deltaApplicationRule(RelationContext relation, boolean pos,
			AtomType resType, AtomType relType, AtomType deltaType, 
			boolean skipold, boolean includeKeysOnly, boolean deleteFromHead, Map<String, Schema> builtInSchemas) {
		List<Rule> ret = new ArrayList<Rule>();
		List<AtomArgument> vars = new ArrayList<AtomArgument>();

		for(int i = 0; i < relation.getRelation().getFields().size(); i++){
			vars.add(new AtomVariable(Mapping.getFreshAutogenVariableName()));
		}
		Atom head = new Atom(relation, vars, resType);

		//ScMappingAtom bodyatom = new ScMappingAtom(head, pos ? AtomType.INS : AtomType.DEL);
		Atom bodyatom = new Atom(head, deltaType);

		if(Config.getStratified() && deltaType == AtomType.DEL)
			//    	if(Config.DO_STRATIFIED)
			bodyatom.setAllStrata(); // STRATIFIED/INS doesn't need allStrata, just last one

		List<Atom> body1 = new ArrayList<Atom>();
		Atom bodyat1 = head.deepCopy();
		bodyat1.setType(relType);
		body1.add(bodyat1);

		Rule r;
		if(pos){
			if (skipold == false) {
				ret.add(new Rule(head, body1, null, includeKeysOnly, builtInSchemas));
			}
			List<Atom> body2 = new ArrayList<Atom>();
			body2.add(bodyatom);
			r = new Rule(head, body2, null, includeKeysOnly, builtInSchemas);
		}else{
			bodyatom.negate();
			body1.add(bodyatom);
			r = new Rule(head, body1, null, includeKeysOnly, builtInSchemas);
			if(deleteFromHead)
				r.setDeleteFromHead();
		}

		ret.add(r);

		return ret;      
	}

	


	protected static List<Rule> deltaApplicationRules(List<RelationContext> rels, boolean ins, boolean skipold, 
			boolean includeKeysOnly, Map<String, Schema> builtInSchemas){
		List<Rule> vr = new ArrayList<Rule>();

		for (RelationContext idb : rels) {
			List<Rule> v;
			if (ins) {
				// Add INS TO NEW inside fixpoint - no need to copy OLD again
				// enforced by skipold = true
				v = deltaApplicationRule(idb, ins, AtomType.INS, true,
						includeKeysOnly, builtInSchemas);

			} else {
				if (Config.getStratified()) {
					v = deltaApplicationRule(idb, ins, AtomType.DEL, skipold,
							includeKeysOnly, builtInSchemas);
				} else {
					v = deltaApplicationRule(idb, ins, AtomType.ALLDEL,
							skipold, includeKeysOnly, builtInSchemas);
				}
			}
			for (Rule r : v) {
				vr.add(r);
				// vr.add(new SingleRuleDatalogProgram(r));
			}
		}
		// NonRecursiveDatalogProgram ret = new NonRecursiveDatalogProgram(vr);
		return vr;// ret;
	}

	

	private static List<Rule> moveRelationList(List<RelationContext> rels, AtomType headType, 
			AtomType bodyType, Map<String, Schema> builtInSchemas){
		List<Rule> ret = new ArrayList<Rule>();
		//List<DatalogProgram> ret = new ArrayList<DatalogProgram>();
		for(RelationContext rel : rels){
			Rule r = relMove(rel, headType, bodyType, builtInSchemas);
			ret.add(r);//new SingleRuleDatalogProgram(r));
		}
		return ret;
	}

	protected static Rule relCleanup(RelationContext relation, AtomType type,
			Map<String, Schema> builtInSchemas) {
		List<AtomArgument> vars = new ArrayList<AtomArgument>();
		for(int i = 0; i < relation.getRelation().getFields().size(); i++){
			vars.add(new AtomVariable(Mapping.getFreshAutogenVariableName()));
		}
		Atom deleteHead = new Atom(relation, vars, type);
		deleteHead.negate();
		List<Atom> deleteBody = new ArrayList<Atom>();
		Rule del = new Rule(deleteHead, deleteBody, null, builtInSchemas);

		return del;      
	}

	private static Rule relCopy(RelationContext relation, AtomType headType, AtomType bodyType, Map<String, Schema> builtInSchemas){
		return relCopy(relation, headType, relation, bodyType, false, builtInSchemas);
	}

	protected static Rule relCopy(RelationContext relation1, AtomType headType, RelationContext relation2, AtomType bodyType, boolean delfix, Map<String, Schema> builtInSchemas) {
		List<AtomArgument> vars = new ArrayList<AtomArgument>();
		for(int i = 0; i < relation1.getRelation().getFields().size(); i++){
			vars.add(new AtomVariable(Mapping.getFreshAutogenVariableName()));
		}
		Atom head = new Atom(relation1, vars, headType);
		Atom bodyatom = new Atom(relation2, vars, bodyType);
		List<Atom> copyBody = new ArrayList<Atom>();
		copyBody.add(bodyatom);
		Rule copy = new Rule(head, copyBody, null, delfix, builtInSchemas);	

		return copy;      
	}

	

	private static Rule relMove(RelationContext relation, AtomType headType, AtomType bodyType, Map<String, Schema> builtInSchemas){
		Rule newRule = relCopy(relation, headType, relation, bodyType, false, builtInSchemas);
		newRule.setClearNcopy();
		return newRule;
	}

	

	/*
	 * 
	 */
	

	/**
	 * Go through defs and "substituteAtom" the ones whose head only appears in one rule and body 
	 * has a single atom. Those rules that are "unfolded" this way don't need to exist at all
	 * We can also unfold the M- in the inv rules later, where the M- appears positively, but I 
	 * think there it also only makes sense to unfold the same ones as here ...
	 *  
	 * @param defs
	 * @param provUpdRules
	 * @return
	 */
	public static List<Rule> unfoldProvDefs(List<Rule> defs, List<Rule> provUpdRules, List<Rule> unusedDefs,
			boolean unfoldPosMultiAtomDefs) {
		List<Rule> ret = new ArrayList<Rule>();

		if(defs == null){
			return provUpdRules;
		}else{
			unusedDefs.clear();
			unusedDefs.addAll(defs);
			for(Rule r : provUpdRules){
				ret.addAll(r.substituteSingleAtomDefs(defs, unusedDefs, unfoldPosMultiAtomDefs));
			}
		}
		return ret;
//		for(Rule def : defs)
//		unusedDefs.add(def);
//		return provUpdRules;
	}

	
}
