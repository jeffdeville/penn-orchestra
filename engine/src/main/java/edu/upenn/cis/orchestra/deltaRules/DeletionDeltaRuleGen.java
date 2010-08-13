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
import edu.upenn.cis.orchestra.Debug;
import edu.upenn.cis.orchestra.datalog.Datalog;
import edu.upenn.cis.orchestra.datalog.DatalogProgram;
import edu.upenn.cis.orchestra.datalog.DatalogSequence;
import edu.upenn.cis.orchestra.datalog.NonRecursiveDatalogProgram;
import edu.upenn.cis.orchestra.datalog.RecursiveDatalogProgram;
import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datalog.atom.AtomArgument;
import edu.upenn.cis.orchestra.datalog.atom.AtomVariable;
import edu.upenn.cis.orchestra.datalog.atom.Atom.AtomType;
import edu.upenn.cis.orchestra.datamodel.ITranslationRules;
import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.RelationContext;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.mappings.MappingsInversionMgt;
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.util.XMLParseException;

/**
 * Generator for deletion delta rules
 * 
 * @author zives, gkarvoun
 *
 */
public class DeletionDeltaRuleGen extends DeltaRuleGen {
	
	/**
	 * Indicates that there exist bidirectional mappings in the system.
	 */
	private boolean bidirectional;
	
	public DeletionDeltaRuleGen (OrchestraSystem system, ITranslationRules translationRules, Map<String, Schema> builtInSchemas, boolean containsBidirectionalMappings)
	{
		super(system, translationRules, builtInSchemas);
		bidirectional = containsBidirectionalMappings;
	}

	/**
	 * Creates a new {@code DeletionDeltaRuleGen} from serialized state. Used for testing.
	 * 
	 * @param translationStateDoc
	 * @param builtInSchemasDoc
	 * @param system 
	 * @throws XMLParseException
	 */
	DeletionDeltaRuleGen(Document translationStateDoc,
			Document builtInSchemasDoc, OrchestraSystem system) throws XMLParseException {
		super(translationStateDoc, builtInSchemasDoc, system);
		bidirectional = system.isBidirectional();
	}
	
	
	private List<DatalogSequence> updatePolicyRules() {
		DatalogSequence prep = new DatalogSequence(false, true);
		DatalogSequence policy = new DatalogSequence(false, true);
		DatalogSequence post = new DatalogSequence(false, true);
		List<DatalogSequence> ret = new ArrayList<DatalogSequence>();

		List<Rule> bidirDels = backwardDelPropagationRules();
		if(bidirDels.size() > 0){
//			Needed because GUI deletions go into _L_DEL relations
			prep.add(new NonRecursiveDatalogProgram(moveLtoP(AtomType.DEL, AtomType.DEL, getBuiltInSchemas()) ,false, "Bidir_Prep"));
			policy.add(new RecursiveDatalogProgram(bidirDels, true, "Bidir_Policy"));
//			The following should not be necessary, and will probably be wrong if we combine 
//			unidirectional and bidirectional mappings - leave for now to make sure maintenance is correct
			post.add(new NonRecursiveDatalogProgram(clearRelationList(getIdbs(), AtomType.DEL, getBuiltInSchemas()), "Bidir_Post"));
		}

		ret.add(prep);
		ret.add(policy);
		ret.add(post);
		return ret;
	}

	protected DatalogSequence sideEffectFreeUpdatePolicyRulesOneProg(Map<String, Schema> builtInSchemas) {
		DatalogSequence ret = new DatalogSequence(false, true);
		DatalogSequence upd = new DatalogSequence(false, true);
		List<Rule> bidirDels = backwardDelPropagationRules();
		if(bidirDels.size() > 0){
			//			Needed because GUI deletions go into _L_DEL relations
			upd.add(new NonRecursiveDatalogProgram(moveLtoP(AtomType.DEL, AtomType.DEL, getBuiltInSchemas()), false, "MoveLtoP"));
			//			HACKS, to avoid deleting idbs right away
			upd.add(new NonRecursiveDatalogProgram(copyRelationList(getIdbs(), AtomType.INS, AtomType.DEL, getBuiltInSchemas()), false, "CopyIDBs"));
			upd.add(new NonRecursiveDatalogProgram(applyRelonRel(getIdbs(), true, AtomType.NONE, AtomType.NONE, AtomType.INS, true, false, false, getBuiltInSchemas()), false, "ApplyInsToIDBs"));
			upd.add(new NonRecursiveDatalogProgram(clearRelationList(getIdbs(), AtomType.INS, getBuiltInSchemas()), "clearIDBs"));

			//			Update policy rules
			upd.add(new RecursiveDatalogProgram(bidirDels, true, "UpdatePolicyRules"));

			//			Copy idbs from - to d
			upd.add(new NonRecursiveDatalogProgram(copyRelationList(getIdbs(), AtomType.D, AtomType.DEL, getBuiltInSchemas()), "CopyIDBsToD"));
			upd.add(new NonRecursiveDatalogProgram(clearRelationList(getIdbs(), AtomType.DEL, getBuiltInSchemas()), "CopyIDBstoDel"));

			////			Backup all relations
			//			upd.add(new NonRecursiveDatalogProgram(copyRelationList(getEdbs(), AtomType.BCK, AtomType.NONE, getBuiltInSchemas())));
			//			upd.add(new NonRecursiveDatalogProgram(copyRelationList(getIdbs(), AtomType.BCK, AtomType.NONE, getBuiltInSchemas())));
			//			upd.add(new NonRecursiveDatalogProgram(copyRelationList(getMappingRelations(), AtomType.BCK, AtomType.NONE, getBuiltInSchemas())));

			ret.add(upd);

			//			Test-run the maintenance program to compute side effects
			ret.add(preDeletionRules());
			ret.add(deletionRules(builtInSchemas));
			ret.add(postDeletionRules(false, false)); // change last to false to avoid using BCK

			//			Subtract idb^{d} from idb^{-} to get side effects (in {inv}) 
			ret.add(new NonRecursiveDatalogProgram(applyRelonRel(getIdbs(), false, AtomType.INV, AtomType.DEL, AtomType.D, false, true, false, getBuiltInSchemas()), "RemoveDfromDEL"));
			ret.add(new NonRecursiveDatalogProgram(clearRelationList(getIdbs(), AtomType.DEL, getBuiltInSchemas()), "ClearIDBsDEL"));
			ret.add(new NonRecursiveDatalogProgram(clearRelationList(getIdbs(), AtomType.D, getBuiltInSchemas()), "CLearIDBsD"));

			////			Restore rels from backup
			//			ret.add(new NonRecursiveDatalogProgram(moveRelationList(getEdbs(), AtomType.NONE, AtomType.BCK, getBuiltInSchemas())));
			//			ret.add(new NonRecursiveDatalogProgram(moveRelationList(getIdbs(), AtomType.NONE, AtomType.BCK, getBuiltInSchemas())));
			//			ret.add(new NonRecursiveDatalogProgram(moveRelationList(getMappingRelations(), AtomType.NONE, AtomType.BCK, getBuiltInSchemas())));						

			//			Compute lineage of side effects 
			//			HACK to make things work for now
			ret.add(new NonRecursiveDatalogProgram(copyRelationList(getMappingRelations(), AtomType.NEW, AtomType.NONE, getBuiltInSchemas()), "MappingsToNew"));
			ret.add(new NonRecursiveDatalogProgram(copyRelationList(getEdbs(), AtomType.NEW, AtomType.NONE, getBuiltInSchemas()), "EDBsToNew"));
			ret.add(new RecursiveDatalogProgram(lineageRules(), true, "LineageRules"));
			ret.add(new NonRecursiveDatalogProgram(clearRelationList(getMappingRelations(), AtomType.NEW, getBuiltInSchemas()), "NewToMappingRels"));
			ret.add(new NonRecursiveDatalogProgram(clearRelationList(getEdbs(), AtomType.NEW, getBuiltInSchemas()), "ClearNew"));

			//			Subtract edb^{inv} from edb^{-}
			ret.add(new NonRecursiveDatalogProgram(applyRelonRel(getEdbs(), false, AtomType.DEL, AtomType.DEL, AtomType.INV, false, true, true, getBuiltInSchemas()), "InvFromDel"));

			//			Clear inv 
			ret.add(cleanupRelations(AtomType.INV, getMappingRelations(), getEdbs(), getIdbs(), getBuiltInSchemas()));

			//			Now everything should be all set to run actual maintenance program
		}
		return(ret);
	}

	protected List<DatalogSequence> sideEffectFreeUpdatePolicyRules(Map<String, Schema> builtInSchemas) {
		List<DatalogSequence> ret = new ArrayList<DatalogSequence>();
		DatalogSequence prep = new DatalogSequence(false, true);
		DatalogSequence upd = new DatalogSequence(false, true);
		DatalogSequence post1 = new DatalogSequence(false, true);
		DatalogSequence seDetectPrep = new DatalogSequence(false, true);
		DatalogSequence seDetectMaint = new DatalogSequence(false, true);
		DatalogSequence seDetectPost = new DatalogSequence(false, true);
		DatalogSequence subtract1 = new DatalogSequence(false, true);
		DatalogSequence lineagePrep = new DatalogSequence(false, true);
		DatalogSequence lineage = new DatalogSequence(false, true);
		DatalogSequence lineagePost = new DatalogSequence(false, true);
		DatalogSequence subtract2 = new DatalogSequence(false, true);
		DatalogSequence post2 = new DatalogSequence(false, true);

		List<Rule> bidirDels = backwardDelPropagationRules();

		if(bidirDels.size() > 0){
//			Needed because GUI deletions go into _L_DEL relations
			prep.add(new NonRecursiveDatalogProgram(moveLtoP(AtomType.DEL, AtomType.DEL, getBuiltInSchemas()), false, "PrepDEL"));
//			HACKS, to avoid deleting idbs right away
			upd.add(new NonRecursiveDatalogProgram(copyRelationList(getIdbs(), AtomType.INS, AtomType.DEL, getBuiltInSchemas()), false, "CopyDelToIns"));
			upd.add(new NonRecursiveDatalogProgram(applyRelonRel(getIdbs(), true, AtomType.NONE, AtomType.NONE, AtomType.INS, true, false, false, getBuiltInSchemas()), false, "ApplyInsToIDBs"));
			upd.add(new NonRecursiveDatalogProgram(clearRelationList(getIdbs(), AtomType.INS, getBuiltInSchemas()), "ClearINS"));

//			Update policy rules
			upd.add(new RecursiveDatalogProgram(bidirDels, true, "Update Policy"));

//			Copy idbs from - to d
			post1.add(new NonRecursiveDatalogProgram(copyRelationList(getIdbs(), AtomType.D, AtomType.DEL, getBuiltInSchemas()), "DelToD"));
			post1.add(new NonRecursiveDatalogProgram(clearRelationList(getIdbs(), AtomType.DEL, getBuiltInSchemas()), "ClearDEL"));

////			Backup all relations
//			upd.add(new NonRecursiveDatalogProgram(copyRelationList(getEdbs(), AtomType.BCK, AtomType.NONE, getBuiltInSchemas())));
//			upd.add(new NonRecursiveDatalogProgram(copyRelationList(getIdbs(), AtomType.BCK, AtomType.NONE, getBuiltInSchemas())));
//			upd.add(new NonRecursiveDatalogProgram(copyRelationList(getMappingRelations(), AtomType.BCK, AtomType.NONE, getBuiltInSchemas())));

//			ret.add(upd);

//			Test-run the maintenance program to compute side effects
			seDetectPrep.add(preDeletionRules());
			seDetectMaint.add(deletionRules(builtInSchemas));
			seDetectPost.add(postDeletionRules(false, false)); // change last to false to avoid using BCK

//			Subtract idb^{d} from idb^{-} to get side effects (in {inv}) 
			subtract1.add(new NonRecursiveDatalogProgram(applyRelonRel(getIdbs(), false, AtomType.INV, AtomType.DEL, AtomType.D, false, true, false, getBuiltInSchemas()), "ApplyDelToInv"));
			subtract1.add(new NonRecursiveDatalogProgram(clearRelationList(getIdbs(), AtomType.DEL, getBuiltInSchemas()), "ClearDel"));
			subtract1.add(new NonRecursiveDatalogProgram(clearRelationList(getIdbs(), AtomType.D, getBuiltInSchemas()), "ClearD"));

////			Restore rels from backup
//			ret.add(new NonRecursiveDatalogProgram(moveRelationList(getEdbs(), AtomType.NONE, AtomType.BCK, getBuiltInSchemas())));
//			ret.add(new NonRecursiveDatalogProgram(moveRelationList(getIdbs(), AtomType.NONE, AtomType.BCK, getBuiltInSchemas())));
//			ret.add(new NonRecursiveDatalogProgram(moveRelationList(getMappingRelations(), AtomType.NONE, AtomType.BCK, getBuiltInSchemas())));						

//			Compute lineage of side effects 
//			HACK to make things work for now
			lineagePrep.add(new NonRecursiveDatalogProgram(copyRelationList(getMappingRelations(), AtomType.NEW, AtomType.NONE, getBuiltInSchemas()), "LineagePrepCopyToNew"));
			lineagePrep.add(new NonRecursiveDatalogProgram(copyRelationList(getEdbs(), AtomType.NEW, AtomType.NONE, getBuiltInSchemas()), "LinagePrepCopyEDBsToNew"));
			lineage.add(new RecursiveDatalogProgram(lineageRules(), true, "LineageRules"));
			lineagePost.add(new NonRecursiveDatalogProgram(clearRelationList(getMappingRelations(), AtomType.NEW, getBuiltInSchemas()), "LineagePostClearNew"));
			lineagePost.add(new NonRecursiveDatalogProgram(clearRelationList(getEdbs(), AtomType.NEW, getBuiltInSchemas()), "LineagePostClearNewEDBs"));

//			Subtract edb^{inv} from edb^{-}
			subtract2.add(new NonRecursiveDatalogProgram(applyRelonRel(getEdbs(), false, AtomType.DEL, AtomType.DEL, AtomType.INV, false, true, true, getBuiltInSchemas()), "SubInvFromDel"));

//			Clear inv 
			post2.add(cleanupRelations(AtomType.INV, getMappingRelations(), getEdbs(), getIdbs(), getBuiltInSchemas()));

//			Now everything should be all set to run actual maintenance program
		}

		ret.add(prep);
		ret.add(upd);
		ret.add(post1);
		ret.add(seDetectPrep);
		ret.add(seDetectMaint);
		ret.add(seDetectPost);
		ret.add(subtract1);
		ret.add(lineagePrep);
		ret.add(lineage);
		ret.add(lineagePost);
		ret.add(subtract2);
		ret.add(post2);

		return(ret);
	}
	

	private DatalogSequence preDeletionRules() {
		DatalogSequence ret;
		List<Rule> rules = new ArrayList<Rule>();

		ret = new DatalogSequence(false, true);

		rules.addAll(copyRelationList(getEdbs(), AtomType.NEW, AtomType.NONE, getBuiltInSchemas()));
		rules.addAll(copyRelationList(getIdbs(), AtomType.NEW, AtomType.NONE, getBuiltInSchemas()));
		rules.addAll(copyRelationList(getRej(), AtomType.NEW, AtomType.NONE, getBuiltInSchemas()));
		rules.addAll(copyRelationList(getMappingRelations(), AtomType.NEW, AtomType.NONE, getBuiltInSchemas()));
		rules.addAll(copyRelationList(getIncrementallyMaintenableJoinRelations(), AtomType.NEW, AtomType.NONE, getBuiltInSchemas()));
		rules.addAll(copyRelationList(getOuterUnionRelations(), AtomType.NEW, AtomType.NONE, getBuiltInSchemas()));
		
		ret.add(new NonRecursiveDatalogProgram(rules, false, "PreDeletionRules"));
		return ret;
	}

	private DatalogSequence deletionRules(Map<String, Schema> builtInSchemas) {
		DatalogSequence ret;

		ret = new DatalogSequence(false, true);

		// zives 1/10/10 -- moved this below the certain deletions
		//ret.add(edbDeltaApplicationRules(false));

		List<Rule> certainDels = certainDeletionRules();
		ret.add(new NonRecursiveDatalogProgram(certainDels, false, "CertainDeletionRules"));

		// zives 1/10/10 -- moved this to after the certain-deletion rules
		ret.add(edbDeltaApplicationRules(false));
		ret.add(rejDeltaApplicationRules(false));

		DatalogSequence mainLoop = new DatalogSequence(true, true);

		List<Rule> defs = provenanceTblDeletionRules(builtInSchemas);

		mainLoop.add(new NonRecursiveDatalogProgram(defs, false, "ProvTableDeletionRules"));

//		Application of deletions on provenance tables

		List<Rule> provTableUpd = provenanceTblUpdateRules();

		mainLoop.add(new NonRecursiveDatalogProgram(provTableUpd, false, "MainLoopProvTableDelRules"));


//		Copy idb_dels to all dels and cleanup idb_dels
		if(!Config.getStratified())
			mainLoop.add(idbDelCopyAndCleanup());

//		Skip certain deletion rules

		List<Rule> affSet = affectedSetRules(builtInSchemas);
		List<Rule> tempDefs = new ArrayList<Rule>();
		
		for (Rule r : affSet)
			System.out.println(r.toString());

		List<Rule> unfoldedAffSet = unfoldProvDefs(defs, affSet, tempDefs, false);
		
		
		mainLoop.add(new NonRecursiveDatalogProgram(unfoldedAffSet, false, "UnfoldedAffectedSet"));

//		Note: counting of these has to be false if I am not reseting m_dels!
//		Otherwise, it probably should not matter ... but apparently it does ...
//		why are these always non-zero?


		mainLoop.addAll(reachabilityTestingProgram(builtInSchemas));

		mainLoop.add(new NonRecursiveDatalogProgram(unreachableDeletionApplicationRules(),true, "UnreachableDelApplication"));

//		delete inv 
		mainLoop.add(cleanupRelations(AtomType.INV, getMappingRelations(), getEdbs(), getIdbs(), getBuiltInSchemas()));
//		delete rch
		mainLoop.add(cleanupRelations(AtomType.RCH, getMappingRelations(), getEdbs(), getIdbs(), getBuiltInSchemas()));

//		delete del relations - I think I shouldn't
//		It is probably correct either way, but need to think which will perform better
//		mainLoop.add(_dr.cleanupRelations(AtomType.DEL));

		if(!Config.getStratified()){
			// Copy mapping_dels to all dels and cleanup mapping_dels
			mainLoop.add(mappingDelCopyAndCleanup());

			mainLoop.add(cleanupEdbs(AtomType.DEL, getEdbs(), getBuiltInSchemas()));
		}

		ret.add(mainLoop);

//		Apply deltas on IDBs/MappingRels
		ret.add(new NonRecursiveDatalogProgram(idbDeltaApplicationRules(false, true), true, "IDBDeltaApply"));
		ret.add(new NonRecursiveDatalogProgram(mappingDeltaApplicationRules(false, true), true, "MappingDeltaApply"));

//		ret.printString();
//		Debug.println(ret.toString());

		return ret;
	}

	private DatalogSequence postDeletionRules(boolean clearEdbIdbDels, boolean applyDeltas) {
		DatalogSequence ret;
		ret = new DatalogSequence(false, true);
		List<RelationContext> emptyList = new ArrayList<RelationContext>();

		ret.add(new NonRecursiveDatalogProgram(clearRelationList(getMappingRelations(), AtomType.DEL, getBuiltInSchemas()), "PostDelClearDel"));
		ret.add(new NonRecursiveDatalogProgram(clearRelationList(getIncrementallyMaintenableJoinRelations(), AtomType.DEL, getBuiltInSchemas()), "PostDelClearIncr"));
		ret.add(new NonRecursiveDatalogProgram(clearRelationList(getOuterUnionRelations(), AtomType.DEL, getBuiltInSchemas()), "PostDelClearOuterU"));
		if(clearEdbIdbDels){
			ret.add(new NonRecursiveDatalogProgram(clearRelationList(getEdbs(), AtomType.DEL, getBuiltInSchemas()), "PostDelClearEDBDel"));
			ret.add(new NonRecursiveDatalogProgram(clearRelationList(getIdbs(), AtomType.DEL, getBuiltInSchemas()), "PostDelClearIDBDel"));
			ret.add(new NonRecursiveDatalogProgram(clearRelationList(getRej(), AtomType.DEL, getBuiltInSchemas()), "PostDelClearREJDel"));
		}

//		ret.add(new NonRecursiveDatalogProgram(clearRelationList(getEdbs(), AtomType.DEL, getBuiltInSchemas()), true));

		if(!Config.getStratified()){
			ret.add(cleanupRelations(AtomType.ALLDEL, getMappingRelations(), getEdbs(), getIdbs(), getBuiltInSchemas()));			
			ret.add(cleanupRelations(AtomType.ALLDEL, getIncrementallyMaintenableJoinRelations(), emptyList, emptyList, getBuiltInSchemas()));
			ret.add(cleanupRelations(AtomType.ALLDEL, getOuterUnionRelations(), emptyList, emptyList, getBuiltInSchemas()));
		}

//		apply deltas if this is a real run, otherwise just discard NEW
		if(applyDeltas){
			ret.add(applyDeltasToBase(getMappingRelations(), getEdbs(), getIdbs(), getBuiltInSchemas(), "Mapping"));
			ret.add(applyDeltasToBase(getRej(), emptyList, emptyList, getBuiltInSchemas(), "Rejection"));
			ret.add(applyDeltasToBase(getIncrementallyMaintenableJoinRelations(), emptyList, emptyList, getBuiltInSchemas(), "IncrJoin"));
			ret.add(applyDeltasToBase(getOuterUnionRelations(), emptyList, emptyList, getBuiltInSchemas(), "OuterUnion"));
			
//			Recompute real outer join ASRs
	        ret.add(cleanupRelations(AtomType.NONE, getRealOuterJoinRelations(), emptyList, emptyList, getBuiltInSchemas()));
	        ret.add(new NonRecursiveDatalogProgram(getRealOuterJoinRules(), false, "PostDelApplyDeltasToOJ"));
		}else{
			ret.add(cleanupRelations(AtomType.NEW, getMappingRelations(), getEdbs(), getIdbs(), getBuiltInSchemas()));
			ret.add(cleanupRelations(AtomType.NEW, getIncrementallyMaintenableJoinRelations(), emptyList, emptyList, getBuiltInSchemas()));
			ret.add(cleanupRelations(AtomType.NEW, getOuterUnionRelations(), emptyList, emptyList, getBuiltInSchemas()));
		}

		return ret;
	}

	private List<Rule> provenanceTblDeletionRules(Map<String, Schema> builtInSchemas){
		List<Rule> provenanceDels = new ArrayList<Rule>();
		List<Rule> s2pRules = new ArrayList<Rule>();

//		s2pRules.addAll(getSource2ProvRules());
		s2pRules.addAll(getSource2ProvRulesForIns());
		s2pRules.addAll(getIncrementallyMaintenableJoinRules());
		s2pRules.addAll(getOuterUnionRules());

		for (int j = 0; j < s2pRules.size(); j++) {
			Rule mapping_rule = s2pRules.get(j);
			List<Atom> body = new ArrayList<Atom> (mapping_rule.getBody().size());

			Atom mapping_head = mapping_rule.getHead().deepCopy();
			mapping_head.setType(AtomType.DEL);

			for (Atom atom : mapping_rule.getBodyWithoutSkolems()){
				body.add(new Atom(atom, AtomType.NONE));
			}
			for (Atom atom : mapping_rule.getSkolemAtoms()){
				body.add(new Atom(atom, AtomType.NONE));
			}

			if(!Config.getStratified()){
				Atom mAllDel = mapping_head.deepCopy();
				mAllDel.setType(AtomType.ALLDEL);
				mAllDel.negate();
				body.add(mAllDel);
			}

			// Generate a sequence of rules, replacing a single atom with a _DEL relation
			// in each of the rules
			for (int i = 0; i < mapping_rule.getBodyWithoutSkolems().size(); i++) {
				if(i > 0)
					body.set(i-1, new Atom(body.get(i-1), AtomType.NONE));
				
				// Skip if we already have a delta
				if (DeltaRuleGen.hasDeltaRelationVersion(body.get(i), builtInSchemas))
					continue;
				
				//if (!builtInSchemas.containsKey(body.get(i).getSchema().getSchemaId())) {
					body.set(i, new Atom(body.get(i), AtomType.DEL));
	
					Rule newRule = new Rule(mapping_head, body, mapping_rule.getParentMapping(), 
							true, getBuiltInSchemas());
					//				newRule.setReplaceValsWithNullValues();
					provenanceDels.add(newRule);
				//}
			}

		}

		return provenanceDels;
	}

	private List<Rule> certainDeletionRules(){
		List<Rule> certainDels = new ArrayList<Rule>();

//		Deletions from R_L to R should be propagated automatically - no need for reachability test on those
//		Remember to remove them from reachability test when I add them here ...

		for(Rule r : getLocal2PeerRules()){
			Rule newRule = r.deepCopy();
			newRule.getHead().setType(AtomType.DEL);
			for(Atom a : newRule.getBody())
				a.setType(AtomType.DEL);
			certainDels.add(newRule);

		}
		return certainDels;
	}

//	Returns update policy deletions for bidirectional mappings

	private List<Rule> backwardDelPropagationRules(){
		List<Rule> bidirDels = new ArrayList<Rule>();

		for(Mapping mapping : getTranslationRules().getOriginalMappings()){
			if(mapping.isBidirectional()){
				for(Atom a : mapping.getMappingHead()){
					if(a.getDel()){
						Debug.println("Find relation: " + a.getPeer().getId() + "." + a.getSchema().getSchemaId() + "." + a.getRelation().getDbRelName() +"_R");
						Atom newHead2 = a.deepCopy();
						newHead2.setType(AtomType.DEL);

						for(int i = 0; i < mapping.getBody().size(); i++){
							List<Atom> newBody = mapping.copyBody();

							Atom ba = newBody.get(i);
							ba.setType(AtomType.DEL);

							Rule d2 = new Rule(newHead2, newBody, mapping, true, getBuiltInSchemas());

							if(newHead2.hasExistentialVariables(d2)){
								for(Atom h : mapping.getMappingHead()){
									Atom n = h.deepCopy();
									n.setType(AtomType.NONE);
									d2.getBody().add(n);
								}
							}
							bidirDels.add(d2);
						}
					}
				}
				for(Atom a : mapping.getBody()){
					if(a.getDel()){
						Debug.println("Find relation: " + a.getPeer().getId() + "." + a.getSchema().getSchemaId() + "." + a.getRelation().getDbRelName() +"_R");
						Atom newHead2 = a.deepCopy();
						newHead2.setType(AtomType.DEL);

						for(int i = 0; i < mapping.getMappingHead().size(); i++){
							List<Atom> newBody = mapping.copyMappingHead();
							Atom ha = newBody.get(i);
							ha.setType(AtomType.DEL);

							Rule d2 = new Rule(newHead2, newBody, mapping, true, getBuiltInSchemas());

							if(newHead2.hasExistentialVariables(d2)){
								for(Atom h : mapping.getBody()){
									Atom n = h.deepCopy();
									n.setType(AtomType.NONE);
									d2.getBody().add(n);
								}
							}
							bidirDels.add(d2);
						}
					}
				}

			}
		}
		for(Rule r : getLocal2PeerRules()){
			Atom localAtom = r.getBody().get(0);
			Atom peerAtom = r.getHead();

			Atom appLocHead = localAtom.deepCopy();
			appLocHead.setType(AtomType.DEL);

			List<Atom> appLocBody = new ArrayList<Atom>();
			Atom peerMinus = peerAtom.deepCopy();
			peerMinus.setType(AtomType.DEL);

			appLocBody.add(peerMinus);
			appLocBody.add(localAtom.deepCopy());

			Rule appLoc = new Rule(appLocHead, appLocBody, null, true, getBuiltInSchemas());

			bidirDels.add(appLoc);
		}

		return bidirDels;
	}



	private List<Datalog> reachabilityTestingProgram(Map<String, Schema> builtInSchemas){
		List<Datalog> var = new ArrayList<Datalog>();
		var.add(new NonRecursiveDatalogProgram(localDerivabilityHeuristicRules(), false, "LocalDerivabilityHeur"));
		var.get(0).setMeasureExecTime(true);
		var.add(new RecursiveDatalogProgram(derivabilityRules(), true, "Derivability"));
		var.get(1).setMeasureExecTime(true);
		var.add(new RecursiveDatalogProgram(reachabilityTestingRules(builtInSchemas), true, "ReachabilityTest"));
		var.get(2).setMeasureExecTime(true);
		return var;
	}

	private List<Rule> reachabilityTestingRules(Map<String, Schema> builtInSchemas){
		List<Rule> vr = new ArrayList<Rule>();
		//List<DatalogProgram> vr = new ArrayList<DatalogProgram>();


		// For edbs, whatever is in inv is the only thing that will ever be reachable, so replace
		// references with edb_INV
		// For idbs, replace with RCH and not DEL and not ALLDEL
		// Base case has been handled above

//		Use the INV versions of edbs as the edbs of the reachability program
		
		for(Rule r : getLocal2PeerRules()){
			Atom head = new Atom(r.getHead(), AtomType.RCH);
			List<Atom> body = new ArrayList<Atom>();
			
			for(Atom a : r.getBody()){
				if (!builtInSchemas.containsKey(a.getRelationContext().getSchema().getSchemaId()))
					body.add(new Atom(a, AtomType.INV));
			}
			vr.add(new Rule(head, body, r.getParentMapping(), true, getBuiltInSchemas()));
		}	
		
		for(Rule r : getSource2TargetRules()){
			Atom head = new Atom(r.getHead(), AtomType.RCH);
			List<Atom> body = new ArrayList<Atom>();

			for(Atom a : r.getBody()){
				if(getIdbs().contains(a.getRelationContext())){
					Atom foo = new Atom(a, AtomType.RCH);
					body.add(foo);
				}else{ 
					// Don't need to subtract deleted because inverse rules 
					// now join with new versions of edbs

//					if(!a.isSkolem()
//							&& !builtInSchemas.containsKey(a.getRelationContext().getSchema().getSchemaId())
//							){
					if (DeltaRuleGen.hasDeltaRelationVersion(a, builtInSchemas)) {
						Atom foo = new Atom(a, AtomType.INV);
						//ScMappingAtom bar = new ScMappingAtom(a, AtomType.DEL);
						//bar.negate();
						body.add(foo);
						//body.add(bar);
					}else{
						Atom foo = new Atom(a, AtomType.NONE);
						body.add(foo);
					}
				}
			}

			Atom bar = new Atom(r.getHead(), AtomType.DEL);
			bar.negate();
			if(Config.getStratified()){
				bar.setAllStrata();
				body.add(bar);
			}else{
				Atom oldDel = new Atom(r.getHead(), AtomType.ALLDEL);
				oldDel.negate();

				body.add(bar);
				body.add(oldDel);
			}

			vr.add(new Rule(head, body, r.getParentMapping(), true, getBuiltInSchemas()));
			//new SingleRuleDatalogProgram(new Rule(head, body, true)));
		}
		return vr;
	}


	private List<Rule> unreachableDeletionApplicationRules() {
		//protected List<DatalogProgram> unreachableDeletionApplicationRules() {
		List<Rule> vr = new ArrayList<Rule>();
		//List<DatalogProgram> vr = new ArrayList<DatalogProgram>();

		for(RelationContext rel: getIdbs()){
			List<AtomArgument> vars = new ArrayList<AtomArgument>();

			for(int i = 0; i < rel.getRelation().getFields().size(); i++){
				AtomVariable v = new AtomVariable(Mapping.getFreshAutogenVariableName());
				v.setExistential(true);
				vars.add(v);
			}
			Atom head = new Atom(rel, vars, AtomType.DEL);
//			ScMappingAtom head = new ScMappingAtom(rel, vars, AtomType.ALLDEL);
			//ScMappingAtom head2 = new ScMappingAtom(rel, vars, AtomType.LOOPTEST);

			Atom bodyatom1 = new Atom(head, AtomType.INV);
			Atom bodyatom2 = new Atom(head, AtomType.RCH);
			bodyatom2.negate();
//			ScMappingAtom bodyatom3 = new ScMappingAtom(head, AtomType.ALLDEL);
//			bodyatom3.negate();

			List<Atom> body = new ArrayList<Atom>();
			body.add(bodyatom1);
			body.add(bodyatom2);
//			body.add(bodyatom3);

			vr.add(new Rule(head, body, null, true, getBuiltInSchemas()));
			//(new SingleRuleDatalogProgram(new Rule(head, body, true)));
			// I think I don't need this with the new fixpoint operator
			// possible optim: mark the rules to be counted towards fixpoint,
			// don't count everything ... e.g., here count only this rule
			// vr.add(new Rule(head2, body)); 
		}
		return vr;     
	}    

	private List<Rule> provenanceTblUpdateRules(){
		//int len = getMappingRules().size();

		List<Rule> var = new ArrayList<Rule>();
//		List<DatalogProgram> var = new ArrayList<DatalogProgram>();
		List<RelationContext> allMappingRelations = new ArrayList<RelationContext>();
		allMappingRelations.addAll(getMappingRelations());
		allMappingRelations.addAll(getIncrementallyMaintenableJoinRelations());
		allMappingRelations.addAll(getOuterUnionRelations());

		for (RelationContext relCtx : allMappingRelations) {
			List<Rule> v = deltaApplicationRule(relCtx, false, AtomType.DEL, false, true, getBuiltInSchemas());
			for(Rule r : v){
				r.setDeleteFromHead();
//				var.add(new SingleRuleDatalogProgram(r, false));
				var.add(r);
			}
		}
		return var;
	}

	/*
	 * Creates "deltarules" with provrels on the head, rules for computing the 
	 * certain deltas for idbs and rules for computing the possibly affected idb
	 * tuples (inv) that are then check for "reachability"/re-derivation
	 */
	private List<Rule> affectedSetRules(Map<String, Schema> builtInSchemas){

		List<Rule> vr = new ArrayList<Rule>();

		for(Rule r : getProv2TargetRules()){
			List<Atom> body = new ArrayList<Atom>();
			Atom head = new Atom(r.getHead(), AtomType.INV);

//			HACK, but we really only want to turn the first 
//			(i.e., provenance relation) atom to _DEL
			int i = 0;
			for(Atom bar : r.getBody()){
				if(!bar.isNeg()){
					Atom foo = bar.deepCopy();

					if(i > 0 || !DeltaRuleGen.hasDeltaRelationVersion(bar, builtInSchemas))//bar.isSkolem() || builtInSchemas.containsKey(bar.getSchema().getSchemaId()))
						foo.setType(AtomType.NONE);
					else
						foo.setType(AtomType.DEL);
					body.add(foo);
				}
				i++;
			}

//			DEL is now empty anyway
//			ScMappingAtom edbMinus = head.deepCopy();
//			edbMinus.setType(AtomType.DEL);
//			edbMinus.negate();
//			body.add(edbMinus);

			if(Config.getStratified()){
				Atom edbAllMinus = head.deepCopy();
				edbAllMinus.setType(AtomType.DEL);
				edbAllMinus.negate();
				edbAllMinus.setAllStrata();
				body.add(edbAllMinus);
			}else{
				Atom edbAllMinus = head.deepCopy();
				edbAllMinus.setType(AtomType.ALLDEL);
				edbAllMinus.negate();
				body.add(edbAllMinus);
			}

			vr.add(new Rule(head, body, r.getParentMapping(), true, getBuiltInSchemas()));
		}
		return vr;
	}

	private List<Rule> getProv2TargetRules() {
		return subtractFakeRules(getProv2TargetRules(getTranslationRules(), getBuiltInSchemas()));
	}

	private List<Rule> getSource2TargetRules() {
		return subtractFakeRules(getTranslationRules().getSource2TargetRules());
	}

	private DatalogProgram edbDeltaApplicationRules(boolean ins) {
		return deltaApplicationRules(ins, getEdbs(), getBuiltInSchemas());
	}
	
	private DatalogProgram rejDeltaApplicationRules(boolean ins) {
		return deltaApplicationRules(ins, getRej(), getBuiltInSchemas());
	}

	private List<Rule> mappingDeltaApplicationRules(boolean ins, boolean includeKeysOnly) {
		return mappingDeltaApplicationRules(ins, false, includeKeysOnly);
	}

	private DatalogSequence idbDelCopyAndCleanup() {
		return relCopyDeltoAllDel(getIdbs(), getBuiltInSchemas());
	}

	private DatalogSequence mappingDelCopyAndCleanup() {
		return relCopyDeltoAllDel(getMappingRelations(), getBuiltInSchemas());
	}
	private List<Rule> localDerivabilityHeuristicRules() {
			List<Rule> ret = new ArrayList<Rule>();
			
			for(Rule r : getLocal2PeerRules()){
				Rule newRule = r.deepCopy();
				newRule.getHead().setType(AtomType.RCH);
				newRule.getBody().get(0).setType(AtomType.NONE);
				
				Atom goalDir = newRule.getHead().deepCopy();
				goalDir.setType(AtomType.INV);
				newRule.getBody().add(goalDir);
				
				Atom notDel = newRule.getBody().get(0).deepCopy();
				notDel.setType(AtomType.DEL);
				notDel.negate();
				newRule.getBody().add(notDel);
				newRule.setOnlyKeyAndNulls();
				ret.add(newRule);
	
				Atom head2 = newRule.getHead().deepCopy();
				Atom body2 = newRule.getHead().deepCopy();
				head2.setType(AtomType.INV);
	//			head2.negate();
				List<Atom> body = new ArrayList<Atom>();
				
				body.add(head2.deepCopy());
				body2.setType(AtomType.RCH);
				body2.negate();
				body.add(body2);
				
				Rule newRule2 = new Rule(head2, body, r.getParentMapping(), r.getBuiltInSchemas());
				newRule2.setDeleteFromHead();
				newRule2.setOnlyKeyAndNulls();
				ret.add(newRule2);
			}
			return ret;
		}

	private List<Rule> moveLtoP(AtomType headType, AtomType bodyType, Map<String, Schema> builtInSchemas) {
		List<Rule> ret = new ArrayList<Rule>();
	
		for(Rule r : getLocal2PeerRules()){
			RelationContext rel1 = r.getHead().getRelationContext();
			RelationContext rel2 = r.getBody().get(0).getRelationContext();
	
			Rule m = relMove(rel1, headType, rel2, bodyType, builtInSchemas);
			ret.add(m);
		}
		return ret;
	}

	private List<Rule> mappingDeltaApplicationRules(boolean ins, boolean skipold,
			boolean includeKeysOnly) {
				return deltaApplicationRules(getMappingRelations(), ins, skipold, includeKeysOnly, getBuiltInSchemas());
			}

	private List<Rule> lineageRules() {
		return lineageRules(getSource2ProvRulesForProvQ(),
				getProv2TargetRulesForProvQ(), 
				getEdbs(), getIdbs());
	}

	private List<Rule> derivabilityRules() {
			List<Rule> dRules = derivabilityRules(getSource2ProvRulesForProvQ(), //getMappingRules(), 
					getProv2TargetRulesForProvQ(), // getMappingProjectionRules(),
					getEdbs(), 
					getIdbs());
	//		return OuterJoinUnfolder.unfoldOuterJoins(dRules, getOuterJoinRelations(), 
	//				AtomType.NEW, AtomType.INV, getBuiltInSchemas());
			return dRules;
		}

	private List<Rule> derivabilityRules(List<Rule> source2provRules, List<Rule> prov2targetRules, List<RelationContext> edbs,
			List<RelationContext> idbs) {
			
					List<Rule> vr = new ArrayList<Rule>();
					List<Rule> mInvToEdbInv = new ArrayList<Rule>();
					List<Rule> idbInvToMInv = new ArrayList<Rule>();
					List<Rule> invertedMappingRules = new ArrayList<Rule>();
			
			//		Substitute idbs with prov2target rules in body of source2prov rules -> result
			//		invert result + mappingProjections ...
					boolean skipFakeMappings = true;
			
			//		Only works correctly for acyclic mappings
					if(skipFakeMappings && Config.isAcyclicSchema()){
						List<Rule> realInvertedSource2provRules = new ArrayList<Rule>();
						List<Rule> realSource2provRules = new ArrayList<Rule>();
						List<Rule> realProv2TargetRules = new ArrayList<Rule>();
						List<Rule> l2pRules = new ArrayList<Rule>();
			
						for(Rule r : source2provRules) {
							if(!r.isFakeMapping()){
								realSource2provRules.add(r);
								if(r.getSkolemAtoms().size() == 0){
									List<Rule> inv = r.invertForReachabilityTest(true, true, edbs, false);
									for(Rule rr : inv){
										if(!rr.onlyKeyAndNulls())
											rr.setOnlyKeyAndNulls();
										realInvertedSource2provRules.add(rr);
									}
								}
							}
						}
						for(Rule r : getLocal2PeerRules()) {
							realProv2TargetRules.add(r);
							if(r.getSkolemAtoms().size() == 0){
								List<Rule> inv = r.invertForReachabilityTest(true, true, edbs);
								for(Rule rr : inv){
									if(!rr.onlyKeyAndNulls())
										rr.setOnlyKeyAndNulls();
									l2pRules.add(rr);
								}
							}
						}
						List<Rule> unfoldedl2pRules = unfoldIdbs(l2pRules, realInvertedSource2provRules, idbs);
						mInvToEdbInv.addAll(unfoldedl2pRules); // inv edbs in terms of mappings 
						mInvToEdbInv.addAll(l2pRules); // inv edbs in terms of idbs, which are the input of this program
			
						for(Rule r : prov2targetRules) {
			//				I think the following is wrong after all ... switching back to rules
			//				for(Mapping m : prov2targetMappings) {			
			//				List<Rule> inv = Rule.invertMappingForReachabilityTest(m, true, true, edbs, true, db);
							if(!r.isFakeMapping()){
								realProv2TargetRules.add(r);
								List<Rule> inv = r.invertForReachabilityTest(true, true, edbs);
								for(Rule rr : inv){
									if(!rr.onlyKeyAndNulls())
										rr.setOnlyKeyAndNulls();
			
									idbInvToMInv.add(rr);
								}
							}
						}
			
			//			Maybe I should invert first and unfold afterwards ...
			//			Otherwise it seems to produce redundant rules
			//			List<Rule> unfoldedMappingRules = unfoldIdbs(source2provRules, prov2targetRules, idbs);
						List<Rule> unfoldedMappingRules = unfoldIdbs(realSource2provRules, realProv2TargetRules, idbs);
			
						for(Rule r : unfoldedMappingRules){
							List<Rule> inv = r.invertForReachabilityTest(false, true, edbs);
							for(Rule rr : inv){
								if(!rr.onlyKeyAndNulls())
									rr.setOnlyKeyAndNulls();
			
								if(!invertedMappingRules.contains(rr))
									invertedMappingRules.add(rr);
							}
						}
					}else{ // Old code, treating local2Peer mappings as real mappings
						for(Rule r : source2provRules) {
							if(r.getSkolemAtoms().size() == 0){
								List<Rule> inv = r.invertForReachabilityTest(true, false, edbs);
								for(Rule rr : inv){
									if(!rr.onlyKeyAndNulls())
										rr.setOnlyKeyAndNulls();
									mInvToEdbInv.add(rr);
								}
							}
						}
			
						for(Rule r : prov2targetRules) {
			//				I think the following is wrong after all ... switching back to rules
			//				for(Mapping m : prov2targetMappings) {			
			//				List<Rule> inv = Rule.invertMappingForReachabilityTest(m, true, true, edbs, true, db);
							List<Rule> inv = r.invertForReachabilityTest(true, true, edbs);
							for(Rule rr : inv){
								if(!rr.onlyKeyAndNulls())
									rr.setOnlyKeyAndNulls();
			
								idbInvToMInv.add(rr);
							}
						}
			
			//			Maybe I should invert first and unfold afterwards ...
			//			Otherwise it seems to produce redundant rules
						List<Rule> unfoldedMappingRules = unfoldIdbs(source2provRules, prov2targetRules, idbs);
			
						for(Rule r : unfoldedMappingRules){
							List<Rule> inv = r.invertForReachabilityTest(false, true, edbs);
							for(Rule rr : inv){
								if(!rr.onlyKeyAndNulls())
									rr.setOnlyKeyAndNulls();
			
								if(!invertedMappingRules.contains(rr))
									invertedMappingRules.add(rr);
							}
						}
					}
			
			//		Replace Mapping relation atoms with Join relations
			//		if(outerJoinRelations == null || outerJoinRelations.size() == 0){
					vr.addAll(mInvToEdbInv);
					vr.addAll(invertedMappingRules);
					vr.addAll(idbInvToMInv);

			
					return vr;
				}

	/**
	 * Convenience methods, to avoid passing parameters for local methods in this class
	 * @return
	 */
	private List<Rule> getSource2ProvRulesForProvQ() {
		return subtractFakeRules(DeltaRuleGen.getSource2ProvRulesForProvQ(getTranslationRules()));
	}

	private List<Rule> getProv2TargetRulesForProvQ() {
		return subtractFakeRules(DeltaRuleGen.getProv2TargetRulesForProvQ(getTranslationRules(), getBuiltInSchemas()));
	}

	

	private static DatalogProgram cleanupEdbs(AtomType typ, List<RelationContext> edbs, Map<String, Schema> builtInSchemas) {
		List<Rule> ret = new ArrayList<Rule>();

		ret.addAll(clearRelationList(edbs/*getEdbs()*/, typ, builtInSchemas));

		DatalogProgram p = new NonRecursiveDatalogProgram(ret,false, "CleanEDBs");
		return p;
	}
	
	private static List<Rule> applyRelonRel(List<RelationContext> rels, boolean pos, AtomType resType,
			AtomType relType, AtomType deltaType, boolean skipold, boolean includeKeysOnly, boolean deleteFromHead, Map<String, Schema> builtInSchemas){

		List<Rule> vr = new ArrayList<Rule>();

		for(RelationContext r : rels){
			vr.addAll(deltaApplicationRule(r, pos, resType, relType, deltaType, skipold, includeKeysOnly, deleteFromHead, builtInSchemas));
		}
		return vr;
	}
	
	private static DatalogProgram deltaApplicationRules(boolean ins, 
			List<RelationContext> relations, Map<String, Schema> builtInSchemas){
		//List<DatalogProgram> vr = new ArrayList<DatalogProgram>();
		List<Rule> vr = new ArrayList<Rule>();

		for(RelationContext relation : relations) { //getEdbs()){
			List<Rule> v;
			if(ins)
				v = deltaApplicationRule(relation, ins, AtomType.INS, false, builtInSchemas);
			else
				v = deltaApplicationRule(relation, ins, AtomType.DEL, false, builtInSchemas);

			for(Rule r : v){ 
				vr.add(r);
				//vr.add(new SingleRuleDatalogProgram(r));
			}
		}
		//seq.add(new NonRecursiveDatalogProgram(vr));
		//NonRecursiveDatalogProgram ret = new NonRecursiveDatalogProgram(vr);
		return new NonRecursiveDatalogProgram(vr, false, "ApplyEDBDeltas");//seq;
		//return ret;
	}
	
	private static DatalogSequence relCopyDeltoAllDel(List<RelationContext> rels, Map<String, Schema> builtInSchemas) {
		//List<Rule> ret = new ArrayList<Rule>();
		List<Datalog> ret = new ArrayList<Datalog>();

		List<Rule> copy = new ArrayList<Rule>();
		for(RelationContext rel : rels){
			copy.add(relCopy(rel, AtomType.ALLDEL, rel, AtomType.DEL, true, builtInSchemas));
		}
		ret.add(new NonRecursiveDatalogProgram(copy, false, "CopyDelToAllDel"));

		List<Rule> clean = new ArrayList<Rule>();
		for(RelationContext rel : rels){
			clean.add(relCleanup(rel, AtomType.DEL, builtInSchemas));
		}
		ret.add(new NonRecursiveDatalogProgram(clean, false, "ClearDel"));

		//DatalogProgram p = new NonRecursiveDatalogProgram(ret,false);
		//return p;

		return new DatalogSequence(false, ret, false);
		//		return new DatalogSequence(false, ret);
	}
	
	private static List<Rule> getProv2TargetRules (ITranslationRules translationRules, Map<String, Schema> builtInSchemas)
	{
		return subtractFakeRules(MappingsInversionMgt.splitMappingsHeads(translationRules.getProv2TargetMappings(), builtInSchemas)); 
	} 
	
	private static Rule relMove(RelationContext relation1, AtomType headType, RelationContext relation2, AtomType bodyType, Map<String, Schema> builtInSchemas){
		Rule newRule = relCopy(relation1, headType, relation2, bodyType, false, builtInSchemas);
		newRule.setClearNcopy();
		return newRule;
	}
	
	private static List<Rule> lineageRules(List<Rule> source2provRules, 
			List<Rule> prov2targetRules, 
			List<RelationContext> edbs, List<RelationContext> idbs){

		List<Rule> vr = new ArrayList<Rule>();

//		Substitute idbs with prov2target rules in body of source2prov rules -> result
//		invert result + mappingProjections ...

		for(Rule r : source2provRules) {
			if(r.getSkolemAtoms().size() == 0){
				List<Rule> inv = r.invertForReachabilityTest(true, false, edbs);
				for(Rule rr : inv){
					if(!rr.onlyKeyAndNulls())
						rr.setOnlyKeyAndNulls();
					vr.add(rr);
				}
			}
		}
//		Maybe I should invert first and unfold afterwards ...
//		Otherwise it seems to produce redundant rules
		List<Rule> unfoldedMappingRules = unfoldIdbs(source2provRules, prov2targetRules, idbs);

		for(Rule r : unfoldedMappingRules){
			List<Rule> inv = r.invertForReachabilityTest(false, true, edbs);
			for(Rule rr : inv){
				if(!rr.onlyKeyAndNulls())
					rr.setOnlyKeyAndNulls();
				if(!vr.contains(rr))
					vr.add(rr);
			}
		}

		for(Rule r : prov2targetRules) {
//			for(Mapping m : prov2targetMappings) {			
//			List<Rule> inv = Rule.invertMappingForReachabilityTest(m, true, true, edbs, true, db);
			List<Rule> inv = r.invertForReachabilityTest(true, true, edbs);
			for(Rule rr : inv){
				if(!rr.onlyKeyAndNulls())
					rr.setOnlyKeyAndNulls();

				vr.add(rr);
			}
		}

//		FOR NO-OU CASE, BUT CAUSES BLOWUP - WOULD ONLY WORK WITH SELECT DISTINCT BUT DON'T THINK
//		THAT IT WOULD BE FASTER ... (fewer rules, but even with distinct it would have to compute the big join ...)

//		for(ScMapping m : _engine.getProjBefInv()){
//		List<Rule> inv = Rule.invertMappingForReachabilityTest(m, true, true, getEdbs());
//		for(Rule rr : inv){
//		//vr.add(rr);
//		rr.setOnlyKeyAndNulls();
//		rr.setDistinct();
//		vr.add(rr);//new SingleRuleDatalogProgram(rr));
//		}
//		}

		return vr;
	}
	
	private static List<Rule> deltaApplicationRule(RelationContext relation, boolean pos, AtomType type, 
			boolean skipold, Map<String, Schema> builtInSchemas) {
		return deltaApplicationRule(relation, pos, AtomType.NEW, AtomType.NONE, type, skipold, false, true, builtInSchemas);
	}

	private static List<Rule> unfoldIdbs(List<Rule> rules, List<Rule> origDefs, List<RelationContext> idbs){
//		protected static List<Rule> unfoldIdbs(List<Rule> rules, List<Mapping> origDefs, List<RelationContext> idbs){
		List<Rule> newRules = new ArrayList<Rule>();
		newRules.addAll(rules);
		List<Rule> defs = new ArrayList<Rule>();

		for(Rule r : origDefs){
			Rule cutnot = r.deepCopy();
			if(r.getBody().get(r.getBody().size() - 1).isNeg()){
				cutnot.getBody().remove(cutnot.getBody().size()-1);
				cutnot.renameExistentialVars();
			}
			defs.add(cutnot);
		}

		//		defs.addAll(local2PeerRules);

		for(int j = 0; j < newRules.size(); j++){
			Rule r = newRules.get(j);
			int i = 0;
			//for(i = 0; i < r.getBody().size() && !isIdb(r.getBody().get(i).getRelationContext()); i++);
			for(i = 0; i < r.getBody().size() && !idbs.contains(r.getBody().get(i).getRelationContext()); i++);
			if(i < r.getBody().size()){
				newRules.remove(j);
//				try{
				newRules.addAll(r.substituteAtom(i, defs, true));
//				}catch(UnsupportedDisjunctionException e){
////				I think this will never happen here anyway, 
////				because I have "cut" the negated clauses above
//				e.printStackTrace();
//				}
				j--;
			}
		}

		return newRules;
	}

	/* (non-Javadoc)
	 * @see edu.upenn.cis.orchestra.deltaRules.DeltaRuleGen#createRules(java.util.Map)
	 */
	@Override
	protected void createRules(Map<String, Schema> builtInSchemas) {
		List<DatalogSequence> ret = new ArrayList<DatalogSequence>();
		if (bidirectional) {
			if (Config.getAllowSideEffects()) {
				ret.addAll(updatePolicyRules());
			} else {
				ret.addAll(sideEffectFreeUpdatePolicyRules(builtInSchemas));

			}
		}
		ret.add(preDeletionRules());
		ret.add(deletionRules(builtInSchemas));
		ret.add(postDeletionRules(true, true));
		_deltaRules = new DeletionDeltaRules(ret, bidirectional);
	}
	
	
}
