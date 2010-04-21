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

import edu.upenn.cis.orchestra.datalog.Datalog;
import edu.upenn.cis.orchestra.datalog.DatalogSequence;
import edu.upenn.cis.orchestra.datalog.NonRecursiveDatalogProgram;
import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datalog.atom.Atom.AtomType;
import edu.upenn.cis.orchestra.datamodel.ITranslationRules;
import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.RelationContext;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.mappings.MappingsIOMgt;
import edu.upenn.cis.orchestra.mappings.MappingsInversionMgt;
import edu.upenn.cis.orchestra.mappings.MappingsTranslationMgt;
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.util.XMLParseException;

/**
 * Rule generator for insertion delta rules
 * 
 * @author zives, gkarvoun
 *
 */
public class InsertionDeltaRuleGen extends DeltaRuleGen {
	public InsertionDeltaRuleGen (OrchestraSystem system, ITranslationRules translationRules, Map<String, Schema> builtInSchemas)
	{
		super(system, translationRules, builtInSchemas);
	}

	/**
	 * Creates a new {@code InsertionDeltaRuleGen} from serialized state. Used for testing.
	 * 
	 * @param translationStateDoc
	 * @param builtInSchemasDoc
	 * @param system 
	 * @throws XMLParseException
	 */
	InsertionDeltaRuleGen(Document translationStateDoc,
			Document builtInSchemasDoc, OrchestraSystem system) throws XMLParseException {
		super(translationStateDoc, builtInSchemasDoc, system);
	}
	
	private DatalogSequence preInsertion(Map<String, Schema> builtInSchemas) {
		DatalogSequence ret;
		ret = new DatalogSequence(false, true);
        List<Rule> init = new ArrayList<Rule>();
        
        init.addAll(copyRelationList(getEdbs(), AtomType.NEW, AtomType.NONE, builtInSchemas));
        init.addAll(copyRelationList(getRej(), AtomType.NEW, AtomType.NONE, builtInSchemas));
//        init.addAll(copyRelationList(getEdbs(), AtomType.NEW, AtomType.INS));
        init.addAll(copyRelationList(getIdbs(), AtomType.NEW, AtomType.NONE, builtInSchemas));
        init.addAll(copyRelationList(getMappingRelations(), AtomType.NEW, AtomType.NONE, builtInSchemas));
        init.addAll(copyRelationList(getIncrementallyMaintenableJoinRelations(), AtomType.NEW, AtomType.NONE, builtInSchemas));
        init.addAll(copyRelationList(getOuterUnionRelations(), AtomType.NEW, AtomType.NONE, builtInSchemas));
        
        ret.add(new NonRecursiveDatalogProgram(init, true));
		return ret;
	}

	private DatalogSequence postInsertion(Map<String, Schema> builtInSchemas) {
		DatalogSequence ret;
		ret = new DatalogSequence(false, true);
		List<RelationContext> emptyList = new ArrayList<RelationContext>();
		
		
		ret.add(applyDeltasToBase(getMappingRelations(), getEdbs(), getIdbs(), builtInSchemas));
		ret.add(applyDeltasToBase(getRej(), emptyList, emptyList, builtInSchemas));
		ret.add(applyDeltasToBase(getIncrementallyMaintenableJoinRelations(), emptyList, emptyList, builtInSchemas));
		ret.add(applyDeltasToBase(getOuterUnionRelations(), emptyList, emptyList, builtInSchemas));

		//			Recompute real outer join ASRs
		ret.add(cleanupRelations(AtomType.NONE, getRealOuterJoinRelations(), emptyList, emptyList, builtInSchemas));
		ret.add(new NonRecursiveDatalogProgram(getRealOuterJoinRules(), false));
		
		
        ret.add(cleanupRelations(AtomType.INS, getMappingRelations(), getEdbs(), getIdbs(), builtInSchemas));
        ret.add(cleanupRelations(AtomType.INS, getRej(), emptyList, emptyList, builtInSchemas));
        ret.add(cleanupRelations(AtomType.INS, getIncrementallyMaintenableJoinRelations(), emptyList, emptyList, builtInSchemas));
        ret.add(cleanupRelations(AtomType.INS, getOuterUnionRelations(), emptyList, emptyList, builtInSchemas));
        
		return ret;
	}
    


	/**
	 * Creates the datalog program sequence (including copies/deletions) to
	 * apply a set of insertions.
	 * 
	 */
    private DatalogSequence createInsertionProgramSequence(Map<String, Schema> builtInSchemas) {
        DatalogSequence ret = new DatalogSequence(false, true);

       	ret.add(new NonRecursiveDatalogProgram(copyRelationList(getEdbs(), AtomType.NEW, AtomType.INS, builtInSchemas), true));
       	ret.add(new NonRecursiveDatalogProgram(copyRelationList(getRej(), AtomType.NEW, AtomType.INS, builtInSchemas), true));
       	List<Rule> l2p = new ArrayList<Rule>();
       	for(Rule r : getLocal2PeerRules()) {
       		l2p.addAll(insertionRules(r, false, builtInSchemas));
        }
       	ret.add(new NonRecursiveDatalogProgram(l2p));
       	
       	List<Datalog> progList = new ArrayList<Datalog>();

        List<Rule> defs = new ArrayList<Rule>();
        List<Rule> combinedMappings = new ArrayList<Rule>();
        List<Rule> idbIns = new ArrayList<Rule>();        

        for(Rule r : getSource2ProvRulesForIns()) {
       		defs.addAll(insertionRules(r, false, builtInSchemas));
        }

        boolean existCombined = false;
        for(Rule r : getIncrementallyMaintenableJoinRules()) {
        	existCombined = true;
        	combinedMappings.addAll(insertionRules(r, true, builtInSchemas));
        }
        for(Rule r : getOuterUnionRules()) {
        	existCombined = true;
        	combinedMappings.addAll(insertionRules(r, true, builtInSchemas));
        }
        
        for(Rule r : getProv2TargetRulesForIns()) {
        	idbIns.addAll(insertionRules(r, false, builtInSchemas));
        }

		List<Rule> newDefs = new ArrayList<Rule>();
		List<Rule> unfoldedIdbIns = unfoldProvDefs(defs, idbIns, newDefs, false);

		progList.add(new NonRecursiveDatalogProgram(newDefs, true));
		if(existCombined){
			progList.add(new NonRecursiveDatalogProgram(combinedMappings, true));
		}
		progList.add(new NonRecursiveDatalogProgram(unfoldedIdbIns, true));
    	
//        NonRecursiveDatalogProgram appl = new NonRecursiveDatalogProgram(idbDeltaApplicationRules(true, false), false);
//		Count4fixpoint because of initial copying of data from local2peer relations 
        NonRecursiveDatalogProgram appl = new NonRecursiveDatalogProgram(idbDeltaApplicationRules(true, false), true);
        progList.add(appl);

        NonRecursiveDatalogProgram applMap = new NonRecursiveDatalogProgram((copyRelationList(getMappingRelations(), AtomType.NEW, AtomType.INS, builtInSchemas)));
        progList.add(applMap);
//        NonRecursiveDatalogProgram applOJ = new NonRecursiveDatalogProgram((copyRelationList(getOuterJoinRelations(), AtomType.NEW, AtomType.INS, db)));
//        progList.add(applOJ);
        
        DatalogSequence p = new DatalogSequence(true, progList, false);
        ret.add(p);

        List<Rule> mappingAppl = new ArrayList<Rule>();
//        mappingAppl.addAll(copyRelationList(getMappingRelations(), AtomType.NEW, AtomType.INS, db));
        mappingAppl.addAll(copyRelationList(getIncrementallyMaintenableJoinRelations(), AtomType.NEW, AtomType.INS, builtInSchemas));
        mappingAppl.addAll(copyRelationList(getOuterUnionRelations(), AtomType.NEW, AtomType.INS, builtInSchemas));
        
        List<Rule> trash = new ArrayList<Rule>();
        List<Rule> unfoldedMappingAppl = unfoldProvDefs(defs, mappingAppl, trash, false);
        
//        ret.add(new NonRecursiveDatalogProgram(mappingAppl, true));
        ret.add(new NonRecursiveDatalogProgram(unfoldedMappingAppl, true));
        
        return ret;
    }	

	//Note: some atoms are shared between different rules. If these atoms need to be modified 
	// independently in another process, we should deep copy all atoms.
	
	private List<Rule> getProv2TargetRulesForIns() {
		return subtractFakeRules(getProv2TargetRulesForIns(getTranslationRules(), getBuiltInSchemas()));
	}

	/**
	 * Generate all insertion delta rules for a given rule 
	 * @param r Rule for which to generate the delta rules
	 * @return Delta rules
	 */
	private static List<Rule> insertionRules (Rule r, boolean allStrata, Map<String, Schema> builtInSchemas)
    //private static List<DatalogProgram> insertionRules (Rule r)
	{
		AtomType type = AtomType.INS;
		
		Atom head = new Atom (r.getHead(), type);
		if(allStrata)
			head.setAllStrata();
		
		List<Atom> body = new ArrayList<Atom> ();
		for (Atom atom : r.getBody()){
			Atom a = new Atom (atom, AtomType.NONE);
			if(allStrata)
				a.setAllStrata();
			body.add (a);
		}
		List<Rule> deltas = new ArrayList<Rule> ();
		//List<DatalogProgram> deltas = new ArrayList<DatalogProgram> ();
		int size = body.size();
//		if(body.get(size - 1).isNeg()){
//			size = size - 1;
//		}
		
		int lastInd = -1;
		for (int i = 0 ; i < size ; i++)
		{
			while(i < size && (body.get(i).isSkolem() || body.get(i).isNeg())){
				i++;
			}
			
			if (lastInd >= 0){
//				body.set(lastInd, new Atom(body.get(i-1), AtomType.NEW));
				body.get(lastInd).setType(AtomType.NEW);
//				body.set(lastInd, new Atom(body.get(lastInd), AtomType.NEW));
			}
//			body.set (i, new Atom(body.get(i), type));
			if(i < size){
				body.get(i).setType(type);

				lastInd = i;

				Rule deltaRule = new Rule(head, body, r.getParentMapping(), builtInSchemas);
//				if(replaceValsWithNullVals)

//				Greg: I think we should not do this now that we have real skolems
//				if(!Config.isWideProvenance())
//					deltaRule.setReplaceValsWithNullValues();
				
				deltas.add(deltaRule);
				//deltas.add(new SingleRuleDatalogProgram(new Rule(head, body)));
			}
		}
		return deltas;
	}
	
	
	private static List<Rule> getProv2TargetRulesForIns (ITranslationRules translationRules, Map<String, Schema> builtInSchemas)
	{
		return subtractFakeRules(MappingsIOMgt.inOutTranslationR(MappingsInversionMgt.splitMappingsHeads(getProv2TargetMappingsForIns(translationRules), 
				builtInSchemas), 
				true)); 
	}  
	
	/**
	 * "ForIns" rules are joined with mapping heads to insert in provenance 
	 * relations those derivations that could be obtained by extending a chase 
	 * homomorphism to use existing tuples to satisfy the target (head) of the
	 * mapping, instead of producing new ones with labeled nulls 
	 * 
	 * @return
	 */

	private static List<Mapping> getProv2TargetMappingsForIns (ITranslationRules translationRules)
	{
		return subtractFakeMappings(MappingsTranslationMgt.computeProv2TargetMappingsForIns(
				translationRules.getProv2TargetMappings()));
	}

	/* (non-Javadoc)
	 * @see edu.upenn.cis.orchestra.deltaRules.DeltaRuleGen#createRules(java.util.Map)
	 */
	@Override
	protected void createRules(Map<String, Schema> builtInSchemas) {
		List<DatalogSequence> ret = new ArrayList<DatalogSequence>();

		ret.add(preInsertion(builtInSchemas));
		ret.add(createInsertionProgramSequence(builtInSchemas));

		ret.add(postInsertion(builtInSchemas));
		_deltaRules = new InsertionDeltaRules(ret);
	}

	
}
