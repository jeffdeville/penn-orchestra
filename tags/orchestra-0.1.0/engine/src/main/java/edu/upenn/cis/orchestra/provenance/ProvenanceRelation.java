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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.Debug;
import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datalog.atom.AtomArgument;
import edu.upenn.cis.orchestra.datalog.atom.AtomConst;
import edu.upenn.cis.orchestra.datalog.atom.AtomVariable;
import edu.upenn.cis.orchestra.datamodel.AbstractRelation;
import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.PrimaryKey;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationContext;
import edu.upenn.cis.orchestra.datamodel.RelationField;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.StringType;
import edu.upenn.cis.orchestra.datamodel.exceptions.IncompatibleTypesException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnknownRefFieldException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnsupportedTypeException;
import edu.upenn.cis.orchestra.dbms.BuiltinFunctions;
import edu.upenn.cis.orchestra.dbms.IDb;
import edu.upenn.cis.orchestra.dbms.sql.generation.ISqlStatementGen;
import edu.upenn.cis.orchestra.exchange.OuterUnionColumn;
import edu.upenn.cis.orchestra.exchange.RuleFieldMapping;
import edu.upenn.cis.orchestra.exchange.TranslationRuleGen;
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.mappings.RuleEqualityAtom;

public class ProvenanceRelation extends Relation {

	public ProvenanceRelation(Schema schema, int relationID) {
		super(schema, relationID);
		_type = ProvRelType.SINGLE;
	}

	public ProvenanceRelation(Schema schema, int relationID,
			AbstractRelation tab, boolean materialized) {
		super(schema, relationID, tab, materialized, false);
		_type = ProvRelType.SINGLE;
		if (Config.getTempTables())
			super._dbSchema = ISqlStatementGen.sessionSchema;
	}

	public ProvenanceRelation(Schema schema, AbstractRelation tab, String nam,
			String dbName, String dbSchema, boolean materialized) {
		super(schema, tab, nam, dbName, dbSchema, materialized, false);
		_type = ProvRelType.SINGLE;
		if (Config.getTempTables())
			super._dbSchema = ISqlStatementGen.sessionSchema;
	}

	public ProvenanceRelation(String dbCatalog, String dbSchema,
			String dbRelName, String name, String description,
			boolean materialized, List<RelationField> fields) {
		super(dbCatalog, dbSchema, dbRelName, name, description, 
				materialized, false, fields);
		if (Config.getTempTables())
			super._dbSchema = ISqlStatementGen.sessionSchema;

		_type = ProvRelType.SINGLE;
	}

	public ProvenanceRelation(String dbCatalog, String dbSchema,
			String dbRelName, String name, String description,
			boolean materialized, List<RelationField> fields, String pkName,
			List<String> pkFieldNames) throws UnknownRefFieldException {
		super(dbCatalog, dbSchema, dbRelName, name, description, 
				materialized, false, fields, pkName, pkFieldNames);
		if (Config.getTempTables())
			super._dbSchema = ISqlStatementGen.sessionSchema;
		_type = ProvRelType.SINGLE;
	}

	public ProvenanceRelation(Relation relation) {
		super(relation);
		if (Config.getTempTables())
			super._dbSchema = ISqlStatementGen.sessionSchema;
		_type = ProvRelType.SINGLE;
	}

	public List<AtomArgument> provRelAtomArgsForMappingUnion(int mappingIndex, boolean dontCareOthers){
		List<AtomArgument> headVars = new ArrayList<AtomArgument> (getColumns().size());

		//		MRULE attribute
		if(getType().equals(ProvRelType.OUTER_UNION))
			if(dontCareOthers){
				AtomVariable v = new AtomVariable("_");
				//				v.setExistential(true);
				headVars.add(v);
				v.setType(new IntType(false, true));
			}else
				headVars.add(new AtomConst(Integer.toString(mappingIndex)));

		//		Iterate over each column.  See if, for this mapping, we have any source
		//		columns.  Otherwise, add a "NULL" column.
		for (ProvenanceRelationColumn c: getColumns()) {
			AtomArgument sourceArg = c.getSourceArgs().get(mappingIndex);
			if(dontCareOthers){
				AtomVariable v = new AtomVariable("_");
				//				v.setExistential(true);
				v.setType(c.getColumn().getType());
				headVars.add(v);
			}else{
				if (//i < c.getSourceVariables().size() && 
						(sourceArg == null) && (c.getSourceColumns().size() == 1) && 
						//						(c.getSourceColumns().get(0).equals(OuterUnionColumn.ORIGINAL_NULL))
						(c.getSourceColumns().get(0).get(0).getName().equals(OuterUnionColumn.ORIGINAL_NULL))
				){
					headVars.add(new AtomVariable("-"));
					//					new AtomVariable(Mapping.getFreshAutogenVariableName());
				}else if (//i >= c.getSourceVariables().size() || 
						(sourceArg == null)) {
					if(c.getColumn().getSQLTypeName().contains("INT") || (c.getColumn().getSQLTypeName().contains("DECIMAL"))){
						headVars.add(new AtomConst(Integer.toString(Integer.MIN_VALUE)));
					}else if(c.getColumn().getSQLTypeName().contains("CHAR")){
						headVars.add(new AtomConst("UND"));
					}else{
						headVars.add(new AtomConst("UND"));
					}
				} else {
					headVars.add(sourceArg);
				}
			}
		}
		return headVars;
	}

	public List<AtomArgument> provRelAtomArgsForMappingJoin(int mappingIndexStart, int mappingIndexEnd, boolean dontCareOthers){
		List<AtomArgument> headVars = new ArrayList<AtomArgument> (getColumns().size());

		//		Iterate over each column.  See if, for this mapping, we have any source
		//		columns.  Otherwise, add a "NULL" column.
		if(!isInnerJoinRel(getType())) {
			for (int j = 0; j < getMappings().size(); j++){
				if(j >= mappingIndexStart && j <= mappingIndexEnd){
					AtomConst c1 = new AtomConst("1");
					c1.setType(new IntType(false,true));
					headVars.add(c1);
				}else{
					if(dontCareOthers){
						AtomVariable v = new AtomVariable("_");
						//						v.setExistential(true);
						v.setType(new IntType(false,true));
						headVars.add(v);
					}else{
						AtomConst c1 = new AtomConst("0");
						c1.setType(new IntType(false,true));
						headVars.add(c1);
					}
				}
			}
		}

		for (ProvenanceRelationColumn c : getColumns()){
			if(!c.isSpecialAttr()){ // Normal attributes
				AtomArgument sourceArg = null;
				for(int i = mappingIndexStart; i <= mappingIndexEnd; i++){
					if(sourceArg == null)
						if(i < c.getSourceColumns().size() && c.getSourceColumns().get(i) != null)
							sourceArg = c.getSourceArgs().get(i);
				}

				if(sourceArg == null){
					if(dontCareOthers){
						AtomVariable v = new AtomVariable("_");
						//						v.setExistential(true);
						v.setType(c.getColumn().getType());
						headVars.add(v);
					}else{
						//						headVars.add(new AtomVariable("-"));
						Debug.println(c.getColumn().getSQLTypeName());
						if(c.getColumn().getSQLTypeName().contains("INT") || (c.getColumn().getSQLTypeName().contains("DECIMAL"))){
							headVars.add(new AtomConst(Integer.toString(Integer.MIN_VALUE)));
						}else if(c.getColumn().getSQLTypeName().contains("CHAR")){
							headVars.add(new AtomConst("UND"));
						}else{
							headVars.add(new AtomConst("UND"));
						}
					}
				}else{
					//					Should never be > 1, so look at first element only
					headVars.add(sourceArg);
				}
			}else{ // edbbits etc.
				Debug.println("footsa");
			}
		}
		return headVars;
	}

	public List<AtomArgument> provRelAtomArgsForMappingJoinProj(int mappingIndex){
		List<AtomArgument> args = new ArrayList<AtomArgument> (getColumns().size());

		//		Iterate over each column.  See if, for this mapping, we have any source
		//		columns.  Otherwise, add a "NULL" column.

		for (ProvenanceRelationColumn c: getColumns()) {
			AtomArgument sourceArg = null;
			int i = mappingIndex;
			if(i < c.getSourceColumns().size() && c.getSourceColumns().get(i) != null){
				sourceArg = c.getSourceArgs().get(i);
				args.add(sourceArg);
			}else{
				args.add(new AtomVariable("-"));
				//				args.add(new AtomVariable(Mapping.getFreshAutogenVariableName()));
			}
		}
		return args;
	}

	public static Atom getProvRelAtom(Mapping mapping) throws IncompatibleTypesException{
		List<AtomArgument> allArgs = new ArrayList<AtomArgument>();
		List<RuleFieldMapping> rf = mapping.getAppropriateRuleFieldMapping();

		for(RuleFieldMapping rfm : rf){
			allArgs.add(rfm.srcArg);
		}

		RelationContext relCtx = mapping.getProvenanceRelation();
		return new Atom(relCtx, allArgs);
	}

	public static void splitSkolemizedMappingSingle(Mapping mapping, List<Rule> source2prov, List<Mapping> prov2target, Map<String, Schema> builtInSchemas) throws IncompatibleTypesException{

		//		List<AtomArgument> allArgs = new ArrayList<AtomArgument>();
		//		List<RuleFieldMapping> rf = mapping.getAppropriateRuleFieldMapping();

		//		for(RuleFieldMapping rfm : rf){
		//		allArgs.add(rfm.srcArg);
		//		}

		//		RelationContext relCtx = mapping.getProvenanceRelation();

		//		Atom provRelHead = new Atom(relCtx, allArgs);

		Atom provRelHead = getProvRelAtom(mapping);

		// Don't create any rules with built-in relations in the head
		// (And likewise don't create projection rules from these)
		if (!BuiltinFunctions.isBuiltInAtom(provRelHead, builtInSchemas)) {
			Rule s2p;
			Mapping proj;

			
			//				Source to provenance
			s2p = new Rule(provRelHead, mapping.copyBody(), mapping, builtInSchemas);
			//				Provenance to target 
			List<Atom> p2tBody = new ArrayList<Atom>();
			Atom p2tBodyAtom = provRelHead.deepCopy();
			p2tBodyAtom.deskolemizeAllVars();
			p2tBody.add(p2tBodyAtom);

			List<Atom> p2tHead = mapping.copyMappingHead();
			for(Atom a : p2tHead)
				a.deskolemizeAllVars();
			proj = new Mapping("MH-PROJ" + mapping.getId(), "MH-PROJ" + mapping.getId(), true, 1, p2tHead, p2tBody);
			
			s2p.setId("SRC-PRV" + mapping.getId());
			s2p.setFakeMapping(mapping.isFakeMapping());
			proj.setFakeMapping(mapping.isFakeMapping());

			s2p.setDerivedFrom(mapping.getId());
			proj.setDerivedFrom(mapping.getId());
			source2prov.add(s2p);
			prov2target.add(proj);

			Debug.println(s2p.toString());
			Debug.println(proj.toString());
			//			System.err.println(s2p.getId() + " from " + s2p.getDerivedFrom() + ": " + s2p.toString());
			//			System.err.println(proj.getId() + " from " + proj.getDerivedFrom() + ": " + proj.toString());

		}
	}

	public void splitSkolemizedMappingUnion(Mapping mapping, int mappingIndex, Peer peer, Schema schema, 
			List<Rule> source2prov, List<Mapping> prov2Target, Map<String, Schema> builtInSchemas) {

		List<AtomArgument> headVars = provRelAtomArgsForMappingUnion(mappingIndex, false);

		//		Create the appropriate atom of the provenance Relation for this mapping
		RelationContext relCtx = new RelationContext(this, schema, peer, true);
		Atom mapping_head = new Atom(relCtx, headVars);

		//		Rule s2p;
		//		//		Create the appropriate source to provenance mapping
		//		if(Config.isWideProvenance()){ // Wide provenance relations
		//			s2p = new Rule(mapping_head, mapping.getBody(), mapping, db);
		//		}else{
		//			s2p = new Rule(mapping_head, mapping.copyBodyWithoutSkolems(), mapping, db);
		//		}
		//		//		s2p = new Rule(mapping_head, mapping.copyBody(), mapping, db);
		//		s2p.setDerivedFrom(mapping.getId());
		//		s2p.setId("SRC-PRV" + mapping.getId());
		//		s2p.setFakeMapping(mapping.isFakeMapping());
		Rule s2p = createUnionRuleForMapping(mapping_head, mapping, builtInSchemas, false);
		source2prov.add(s2p);
		Debug.println(s2p.toString());

		//		Create the appropriate provenance to target mapping
		List<Atom> p2tHead = mapping.copyMappingHead();
		List<Atom> p2tBody = new ArrayList<Atom>();

		
		Atom p2tBodyAtom = mapping_head.deepCopy();
		p2tBodyAtom.deskolemizeAllVars();
		p2tBody.add(p2tBodyAtom);

		for(Atom a : p2tHead)
			a.deskolemizeAllVars();
		
		Mapping proj = new Mapping(p2tHead, p2tBody);
		proj.setId("MH-PROJ" + mapping.getId());
		proj.setDerivedFrom(mapping.getId());
		proj.setFakeMapping(mapping.isFakeMapping());
		prov2Target.add(proj);
		Debug.println(proj.toString());
	}

	/**
	 * 
	 * @param head the provenance relation atom to be put at the head of this rule
	 * @param mappings the mappings to be outer joined (in the order they are given)
	 * @param builtInSchemas
	 * @return
	 */
	public Rule createJoinRuleForMappings(Atom head, List<Mapping> mappings, Map<String, Schema> builtInSchemas, boolean useMappingRels){
		List<Atom> ret = new ArrayList<Atom>();
		List<RuleEqualityAtom> equalities = new ArrayList<RuleEqualityAtom>();

		try{
			if(!useMappingRels){
				ret.addAll(mappings.get(0).copyBody());
			}else{
				ret.add(getProvRelAtom(mappings.get(0)));
			}
			for(int i = 1; i < mappings.size(); i++){
				Mapping prev = mappings.get(i-1);
				Mapping current = mappings.get(i);

				if(!useMappingRels){
					ret.addAll(current.copyBody());
				}else{
					ret.add(getProvRelAtom(current));
				}

				List<RuleFieldMapping> prevRFM = prev.getAppropriateRuleFieldMapping();
				List<RuleFieldMapping> currRFM = current.getAppropriateRuleFieldMapping();

				for(RuleFieldMapping pRFM : prevRFM){
					List<RelationField> targetFields = pRFM.trgColumns;
					if(targetFields.size() > 0){
						for(RuleFieldMapping cRFM : currRFM){
							List<RelationField> sourceFields = cRFM.srcColumns;
							if(sourceFields.size() > 0){
								boolean found = false;
								for(RelationField tf : targetFields){
									if(!found){
										for(RelationField sf : sourceFields){
											if(!found && tf.equals(sf)){
												equalities.add(new RuleEqualityAtom(pRFM.srcArg, cRFM.srcArg));
												found = true;
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		for(Atom a : ret){
			a.deskolemizeAllVars();
		}

		Rule r = new Rule(head, ret, mappings.get(0), builtInSchemas);
		for(RuleEqualityAtom a : equalities)
			r.addEqAtom(a);
		r.eliminateEqualities();

		r.setFakeMapping(mappings.get(0).isFakeMapping());
		r.setId("SRC-PRV" + mappings.get(0).getId());
		r.setDerivedFrom(mappings.get(0).getId());
		return r;
	}

	public static List<List<RuleFieldMapping>> findInnerJoinEqualColumns(List<Mapping> mappings){
		Map<AtomArgument, List<RuleFieldMapping>> equivalenceClasses = new HashMap<AtomArgument, List<RuleFieldMapping>>();
		List<RuleEqualityAtom> equalities = new ArrayList<RuleEqualityAtom>();
		List<List<RuleFieldMapping>> ret = new ArrayList<List<RuleFieldMapping>>();

		try{

			for(int i = 1; i < mappings.size(); i++){
				Mapping prev = mappings.get(i-1);
				Mapping current = mappings.get(i);

				List<RuleFieldMapping> prevRFM = prev.getAppropriateRuleFieldMapping();
				List<RuleFieldMapping> currRFM = current.getAppropriateRuleFieldMapping();

				for(RuleFieldMapping pRFM : prevRFM){
					List<RelationField> targetFields = pRFM.trgColumns;
					if(targetFields.size() > 0){
						for(RuleFieldMapping cRFM : currRFM){
							List<RelationField> sourceFields = cRFM.srcColumns;
							if(sourceFields.size() > 0){
								boolean found = false;
								for(RelationField tf : targetFields){
									if(!found){
										for(RelationField sf : sourceFields){
											if(!found && tf.equals(sf)){
												List<RuleFieldMapping> pRFMeqClass;
												List<RuleFieldMapping> cRFMeqClass;

												if(equivalenceClasses.containsKey(pRFM.srcArg)) {
													pRFMeqClass = equivalenceClasses.get(pRFM.srcArg);
												} else {
													pRFMeqClass = new ArrayList<RuleFieldMapping>();
													pRFMeqClass.add(pRFM);
												}

												if(equivalenceClasses.containsKey(cRFM.srcArg)){
													cRFMeqClass = equivalenceClasses.get(cRFM.srcArg);

												} else {
													cRFMeqClass = new ArrayList<RuleFieldMapping>();
													cRFMeqClass.add(cRFM);
												}

												List<RuleFieldMapping> mergedEqClass = new ArrayList<RuleFieldMapping>();
												mergedEqClass.addAll(pRFMeqClass);
												mergedEqClass.addAll(cRFMeqClass);
												for(RuleFieldMapping a : mergedEqClass){
													equivalenceClasses.put(a.srcArg, mergedEqClass);
												}

												equalities.add(new RuleEqualityAtom(pRFM.srcArg, cRFM.srcArg));
												found = true;
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}catch(Exception e){
			e.printStackTrace();
		}

		for(List<RuleFieldMapping> eqClass : equivalenceClasses.values()){
			if(!ret.contains(eqClass))
				ret.add(eqClass);
		}

		return ret;
	}



	/**
	 * 
	 * @param head the provenance relation atom to be put at the head of this rule
	 * @param mappings the mappings to be outer joined (in the order they are given)
	 * @param builtInSchemas
	 * @return
	 */
	public Rule createUnionRuleForMapping(Atom head, Mapping mapping, Map<String, Schema> builtInSchemas, boolean useMappingRels){
		List<Atom> ret = new ArrayList<Atom>();
		//		List<RuleEqualityAtom> equalities = new ArrayList<RuleEqualityAtom>();

		try{
			if(!useMappingRels){
				ret.addAll(mapping.copyBody());
			}else{
				ret.add(getProvRelAtom(mapping));
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		for(Atom a : ret){
			a.deskolemizeAllVars();
		}
		Rule r = new Rule(head, ret, mapping, builtInSchemas);

		r.setDerivedFrom(mapping.getId());
		r.setId("SRC-PRV" + mapping.getId());
		r.setFakeMapping(mapping.isFakeMapping());
		return r;
	}

	public List<Rule> outerJoinPathRules(RelationContext relCtx, int firstIndex, Map<String, Schema> builtInSchemas, List<Rule> source2prov, boolean dontCareOthers) {
		List<Mapping> path = new ArrayList<Mapping>();

		for(int j = firstIndex; j < getMappings().size(); j++){
			Mapping mm = getMappings().get(j);
			path.add(mm);

			// Create mapping for this path
			List<AtomArgument> headVars2 = provRelAtomArgsForMappingJoin(firstIndex, j, dontCareOthers);
			Atom mapping_head2 = new Atom(relCtx, headVars2);
			mapping_head2.deskolemizeAllVars();					
			Rule s2pj = createJoinRuleForMappings(mapping_head2, path, builtInSchemas, false);

			//					s2pj.setFakeMapping(m.isFakeMapping());
			//					s2pj.setId("SRC-PRV" + m.getId());
			//					s2pj.setDerivedFrom(m.getId());

			source2prov.add(s2pj);
			Debug.println(s2pj.toString());
		}
		return source2prov;
	}

	public List<Rule> rightOuterJoinPathRules(RelationContext relCtx, int firstIndex, Map<String, Schema> builtInSchemas, List<Rule> source2prov, boolean dontCareOthers) {
		List<Mapping> path = new ArrayList<Mapping>();

		for(int j = firstIndex; j < getMappings().size(); j++){
			Mapping mm = getMappings().get(j);
			path.add(mm);
		}

		for(int j = firstIndex; j < getMappings().size(); j++){
			Mapping mm = getMappings().get(j);
			path.remove(mm);

			// Create mapping for this path
			List<AtomArgument> headVars2 = provRelAtomArgsForMappingJoin(firstIndex, j, dontCareOthers);
			Atom mapping_head2 = new Atom(relCtx, headVars2);
			mapping_head2.deskolemizeAllVars();
			Rule s2pj = createJoinRuleForMappings(mapping_head2, path, builtInSchemas, false);

			//					s2pj.setFakeMapping(m.isFakeMapping());
			//					s2pj.setId("SRC-PRV" + m.getId());
			//					s2pj.setDerivedFrom(m.getId());

			source2prov.add(s2pj);
			Debug.println(s2pj.toString());
		}
		return source2prov;
	}

	public List<Rule> innerJoinPathRule(RelationContext relCtx, int firstIndex, Map<String, Schema> builtInSchemas, List<Rule> source2prov) {
		List<Mapping> path = new ArrayList<Mapping>();

		for(int j = firstIndex; j < getMappings().size(); j++){
			Mapping mm = getMappings().get(j);
			path.add(mm);
		}

		// Create mapping for this path
		List<AtomArgument> headVars2 = provRelAtomArgsForMappingJoin(firstIndex, getMappings().size()-1, false);
		Atom mapping_head2 = new Atom(relCtx, headVars2);
		mapping_head2.deskolemizeAllVars();					
		Rule s2pj = createJoinRuleForMappings(mapping_head2, path, builtInSchemas, false);

		//					s2pj.setFakeMapping(m.isFakeMapping());
		//					s2pj.setId("SRC-PRV" + m.getId());
		//					s2pj.setDerivedFrom(m.getId());

		source2prov.add(s2pj);
		Debug.println(s2pj.toString());

		return source2prov;
	}

	/**
	 * @deprecated
	 * 
	 * Only used in BasicEngine.computeDeltaRules code that is never accessed, 
	 * since we now create joins through console commands, not "automatically" 
	 * via the outerjoin parameter in global/local.properties
	 */
	@Deprecated
	public void splitSkolemizedMappingsJoin(Peer peer, Schema schema,
			List<Rule> source2prov, List<Mapping> prov2Target,
			Map<String, Schema> builtInSchemas) {

		RelationContext relCtx = new RelationContext(this, schema, peer, true);

		if (isFullOuterJoinRel(getType())) {
			// Full Outer Join
			for (int i = 0; i < getMappings().size(); i++) {
				outerJoinPathRules(relCtx, i, builtInSchemas, source2prov,
						false);
			}
		} else if (isLeftOuterJoinRel(getType())) {
			outerJoinPathRules(relCtx, 0, builtInSchemas, source2prov, false);
		} else if (isRightOuterJoinRel(getType())) {
			rightOuterJoinPathRules(relCtx, 0, builtInSchemas, source2prov,
					false);
		} else if (isInnerJoinRel(getType())) {
			innerJoinPathRule(relCtx, 0, builtInSchemas, source2prov);
		}

		// We need to consider each mapping individually + each subpath of
		// mappings
		for (int i = 0; i < getMappings().size(); i++) {
			Mapping m = getMappings().get(i);

			List<Atom> p2tHead = m.copyMappingHead();
			List<Atom> p2tBody = new ArrayList<Atom>();

			List<AtomArgument> projVars = provRelAtomArgsForMappingJoinProj(i);
			// Atom p2tBodyAtom = mapping_head1.deepCopy();
			Atom p2tBodyAtom = new Atom(relCtx, projVars);
			p2tBodyAtom.deskolemizeAllVars();
			p2tBody.add(p2tBodyAtom);

			for (Atom a : p2tHead)
				a.deskolemizeAllVars();

			Mapping proj = new Mapping(p2tHead, p2tBody);
			proj.setFakeMapping(m.isFakeMapping());
			proj.setDerivedFrom(m.getId());
			proj.setId("MH-PROJ" + m.getId());
			prov2Target.add(proj);
			Debug.println(proj.toString());
		}
	}

	/**
	 * Computes the set of Outer Union/Join mapping rules, projection rules, and population rules
	 */
	public void getSplitMappings(Peer peer, Schema schema, List<Rule> source2prov, List<Mapping> prov2Target, Map<String, Schema> builtInSchemas) 
	throws IncompatibleTypesException {
		if(isJoinRel(getType())){
			splitSkolemizedMappingsJoin(peer, schema, source2prov, prov2Target, builtInSchemas);
		}else{
			for (int i = 0; i < getMappings().size(); i++) {
				Mapping mapping = getMappings().get(i);

				if(mapping.isBidirectional()){
					List<Mapping> expandedMappings = TranslationRuleGen.expandBidirectionalMapping(mapping);
					for(Mapping m : expandedMappings){
						splitSkolemizedMappingSingle(m, source2prov, prov2Target, builtInSchemas);
					}
					//					Debug.println("TODO: need to (deskolemize/)expand/reskolemize");
					//					assert(false);
				}else if(getType().equals(ProvRelType.SINGLE)){
					splitSkolemizedMappingSingle(mapping, source2prov, prov2Target, builtInSchemas);
				}else if(getType().equals(ProvRelType.OUTER_UNION)){
					splitSkolemizedMappingUnion(mapping, i, peer, schema, source2prov, prov2Target, builtInSchemas);
				}
			}
		} 
		return;
	}

	//	public Map<RelationContext, List<Rule>> invertOJMappingsMap(IDb db) {
	//	List<Rule> ojMappings = outerJoinMappings(db);
	//	Map<RelationContext, List<Rule>> ret = new HashMap<RelationContext, List<Rule>>();

	//	for(Rule r : ojMappings){
	//	for(Atom a : r.getBody()){
	//	Rule rr = new Rule(a.deepCopy(), r.getHead().deepCopy(), r, db);
	//	RelationContext relCtx = a.getRelationContext();
	//	if(ret.get(relCtx) != null){
	//	ret.put(relCtx, new ArrayList<Rule>());
	//	}
	//	ret.get(relCtx).add(rr);
	//	}
	//	}
	//	return ret;
	//	}

	//	public List<Rule> invertOJMappings(IDb db){
	//	List<Rule> ojMappings = outerJoinMappings(db);
	//	List<Rule> ret = new ArrayList<Rule>();

	//	for(Rule r : ojMappings){
	//	for(Atom a : r.getBody()){
	//	Rule rr = new Rule(a.deepCopy(), r.getHead().deepCopy(), r, db);

	//	ret.add(rr);
	//	}
	//	}
	//	return ret;
	//	}


	/*
	 * Inverse mappings needed for new outer join unfolding algorithm
	 * 
	 * @author gregkar
	 */
	public List<Mapping> getInverseOuterJoinMappings(Map<String, Schema> builtInSchemas) {
		List<Mapping> invMappings = new ArrayList<Mapping>();

		List<Rule> rules = outerJoinMappingsForInversion(builtInSchemas);

		//		for(int i = rules.size(); i >= 0; i--){
		for(int i = 0; i < rules.size(); i++){
			Rule r = rules.get(i);
			List<Atom> mappingHead = new ArrayList<Atom>();
			for(Atom a : r.getMappingHead()) {
				mappingHead.add(a.deepCopy());
			}

//			if(isRealOuterJoinRel(getType()) || isInnerJoinRel(getType())) {
			if(isJoinRel(getType())) {	
				for(Atom a : mappingHead) {
					Set<String> checkList = new HashSet<String>();
					
					int j = 0;
					for(j = 0; j < a.getValues().size(); j++) {
						AtomArgument arg = a.getValues().get(j);

						if(arg instanceof AtomConst){
							AtomConst val = (AtomConst)arg;
							if((j < getMappings().size() && val.getValue().equals(new Integer(0))) ||
							   (val.getValue().equals(Integer.MIN_VALUE) || (val.getValue().equals("UND")))){
								AtomVariable v = new AtomVariable("_");
								v.setType(val.getType());
								a.getValues().set(j, v);
							}
						}else if(arg instanceof AtomVariable){
							if(a.getRelation().getPrimaryKey().getFields().contains(a.getRelation().getField(j))) {
								AtomVariable var = (AtomVariable)arg;
								if(checkList.contains(var.toString())){
									AtomVariable v = new AtomVariable("_");
									v.setType(var.getType());
									a.getValues().set(j, v);
								}else{
									checkList.add(var.toString());
								}
							}
						}
					}
				}
			}
			Mapping m = new Mapping(r.getBody(), mappingHead);
			invMappings.add(m);
		}

		return invMappings;
	}

	public List<Rule> getInverseOuterJoinRules(Map<String, Schema> builtInSchemas) {
		List<Rule> invRules = new ArrayList<Rule>();

		if (ProvRelType.OUTER_JOIN.equals(getType())
				|| ProvRelType.LEFT_OUTER_JOIN.equals(getType())
				|| ProvRelType.RIGHT_OUTER_JOIN.equals(getType())
				|| ProvRelType.INNER_JOIN.equals(getType())) {

			// We need to consider each mapping individually
			Peer peer = getMappings().get(0).getProvenanceRelation().getPeer();
			Schema schema = getMappings().get(0).getProvenanceRelation()
					.getSchema();
			RelationContext relCtx = new RelationContext(this, schema, peer,
					true);

			for (int i = 0; i < getMappings().size(); i++) {
				try {
					Mapping m = getMappings().get(i);
					List<AtomArgument> bodyVars = provRelAtomArgsForMappingJoin(
							i, i, true);
					Atom joinRelAtom = new Atom(relCtx, bodyVars);
					joinRelAtom.deskolemizeAllVars();
					Atom mappingAtom = getProvRelAtom(m);
					mappingAtom.deskolemizeAllVars();
					Rule r = new Rule(mappingAtom, joinRelAtom, m,
							builtInSchemas);
					Debug.println(r.toString());
					invRules.add(r);
				} catch (Exception e) {
					// This should never happen anyway - if this exception
					// existed
					// it would have been thrown earlier, when the mapping
					// relation
					// was first created
					e.printStackTrace();
				}
			}

		}
		return invRules;
	}

	public List<Rule> getInverseOuterUnionRules(Map<String, Schema> builtInSchemas) {
		List<Rule> invRules = new ArrayList<Rule>();

		if(ProvRelType.OUTER_UNION.equals(getType())){
			//				We need to consider each mapping individually
			Peer peer = getMappings().get(0).getProvenanceRelation().getPeer();
			Schema schema = getMappings().get(0).getProvenanceRelation().getSchema();
			RelationContext relCtx = new RelationContext(this, schema, peer, true);

			for(int i = 0; i < getMappings().size(); i++){
				try{
					Mapping m = getMappings().get(i);
					List<AtomArgument> bodyVars = provRelAtomArgsForMappingUnion(i, true);
					Atom joinRelAtom =  new Atom(relCtx, bodyVars);
					joinRelAtom.deskolemizeAllVars();
					Atom mappingAtom = getProvRelAtom(m);
					mappingAtom.deskolemizeAllVars();
					Rule r = new Rule(mappingAtom, joinRelAtom, m, builtInSchemas);
					Debug.println(r.toString());
					invRules.add(r);
				}catch (Exception e) {
					//						This should never happen anyway - if this exception existed
					//						it would have been thrown earlier, when the mapping relation
					//						was first created
					e.printStackTrace();
				}
			}
		}
		return invRules;
	}

	public Map<Integer, List<Rule>> outerJoinPaths(RelationContext relCtx, int firstIndex, Map<String, Schema> builtInSchemas, Map<Integer, List<Rule>> orderedOJMappings, boolean dontCareOthers, boolean addAntiJoins){
		List<Rule> ojMappings = new ArrayList<Rule>();
		List<Mapping> path = new ArrayList<Mapping>();
		List<Mapping> negativeAtoms = new ArrayList<Mapping>();
		boolean negateFirst = false;

		if(addAntiJoins && firstIndex > 0){
			negativeAtoms.add(getMappings().get(firstIndex-1));
			path.add(getMappings().get(firstIndex-1));
			negateFirst = true;
		}

		for(int j = firstIndex; j < getMappings().size(); j++){
			//			List<Mapping> pathWithNegativeAtoms = new ArrayList<Mapping>();
			Mapping mm = getMappings().get(j);
			boolean negateLast = false;

			path.add(mm);

			if(addAntiJoins && j+1 < getMappings().size()) {
				negativeAtoms.add(getMappings().get(j+1));
				path.add(getMappings().get(j+1));
				negateLast = true;
			}
			//			Create mapping for this path
			List<AtomArgument> headVars2 = provRelAtomArgsForMappingJoin(firstIndex, j, dontCareOthers);
			Atom mapping_head2 = new Atom(relCtx, headVars2);
			mapping_head2.deskolemizeAllVars();
			Rule s2pj = createJoinRuleForMappings(mapping_head2, path, builtInSchemas, true);
			//			Rule s2pj = createJoinRuleForMappings(mapping_head2, pathWithNegativeAtoms, db, true);

			if(addAntiJoins) {
				if(negateLast){
					s2pj.getBody().get(s2pj.getBody().size()-1).negate();
					path.remove(path.size()-1);
				}
				if(negateFirst){
					s2pj.getBody().get(0).negate();
					Atom neg = s2pj.getBody().remove(0);
					s2pj.getBody().add(neg);
				}
				//				if(negativeAtoms.size() > 0){
				//					for(int i = 0; i < negativeAtoms.size(); i++){
				//						s2pj.getBody().get(s2pj.getBody().size()-1-i).negate();
				//					}
				//					negativeAtoms.remove(negativeAtoms.size()-1);
				//				}
			}

			ojMappings.add(s2pj);
			if(orderedOJMappings.get(path.size()) == null){
				orderedOJMappings.put(path.size(), new ArrayList<Rule>());
			}
			orderedOJMappings.get(path.size()).add(s2pj);
			Debug.println(s2pj.toString());

		}	
		return orderedOJMappings;
	}
	
	public Map<Integer, List<Rule>> rightOuterJoinPaths(RelationContext relCtx, int firstIndex, Map<String, Schema> builtInSchemas, Map<Integer, List<Rule>> orderedOJMappings, boolean dontCareOthers, boolean addAntiJoins){
		List<Rule> ojMappings = new ArrayList<Rule>();
		List<Mapping> path = new ArrayList<Mapping>();
		//		List<Mapping> negativeAtoms = new ArrayList<Mapping>();		
		boolean negateFirst = false;

		if(addAntiJoins && firstIndex > 0){
			path.add(getMappings().get(firstIndex-1));
			negateFirst = true;
		}

		for(int j = firstIndex; j < getMappings().size(); j++){
			Mapping mm = getMappings().get(j);
			path.add(mm);
		}

		for(int j = firstIndex; j < getMappings().size(); j++){
			//			List<Mapping> pathWithNegativeAtoms = new ArrayList<Mapping>();
			Mapping mm = getMappings().get(j);

			//							Create mapping for this path
			List<AtomArgument> headVars2 = provRelAtomArgsForMappingJoin(j, getMappings().size(), dontCareOthers);
			Atom mapping_head2 = new Atom(relCtx, headVars2);
			mapping_head2.deskolemizeAllVars();
			Rule s2pj = createJoinRuleForMappings(mapping_head2, path, builtInSchemas, true);
			//			Rule s2pj = createJoinRuleForMappings(mapping_head2, pathWithNegativeAtoms, db, true);

			ojMappings.add(s2pj);
			if(orderedOJMappings.get(path.size()) == null){
				orderedOJMappings.put(path.size(), new ArrayList<Rule>());
			}
			orderedOJMappings.get(path.size()).add(s2pj);


			if(addAntiJoins){
				if(negateFirst){
					s2pj.getBody().get(0).negate();
					Atom neg = s2pj.getBody().remove(0);
					s2pj.getBody().add(neg);
					path.remove(0);
				}
				negateFirst = true;
			}else{
				path.remove(mm);	
			}
			Debug.println(s2pj.toString());

		}	
		return orderedOJMappings;
	}

	public Map<Integer, List<Rule>> innerJoinPath(RelationContext relCtx, int firstIndex, Map<String, Schema> builtInSchemas, Map<Integer, List<Rule>> orderedOJMappings){
		List<Rule> ojMappings = new ArrayList<Rule>();
		List<Mapping> path = new ArrayList<Mapping>();

		for(int j = firstIndex; j < getMappings().size(); j++){
			Mapping mm = getMappings().get(j);
			path.add(mm);
		}

		//							Create mapping for this path
		List<AtomArgument> headVars2 = provRelAtomArgsForMappingJoin(firstIndex, getMappings().size()-1, false);
		Atom mapping_head2 = new Atom(relCtx, headVars2);
		mapping_head2.deskolemizeAllVars();
		Rule s2pj = createJoinRuleForMappings(mapping_head2, path, builtInSchemas, true);
		ojMappings.add(s2pj);
		if(orderedOJMappings.get(path.size()) == null){
			orderedOJMappings.put(path.size(), new ArrayList<Rule>());
		}
		orderedOJMappings.get(path.size()).add(s2pj);
		Debug.println(s2pj.toString());	
		return orderedOJMappings;
	}

	//	public List<Rule> outerJoinMappingsForMaintenance(IDb db) {
	//		if (isRealOuterJoinRel(getType())){
	////		Create special "outer join" rules
	//			Peer peer = getMappings().get(0).getProvenanceRelation().getPeer();
	//			Schema schema = getMappings().get(0).getProvenanceRelation().getSchema();
	//			RelationContext relCtx = new RelationContext(this, schema, peer, true);
	//			
	//			
	//		}else{
	//			return outerJoinMappingsForInversion(db);
	//		}
	//	}

	public List<Rule> outerJoinMappingsForMaintenance(Map<String, Schema> builtInSchemas) {

		if (isRealOuterJoinRel(getType())){
			//		Add appropriate anti-joins to ensure there is no redundancy	
			return outerJoinMappings(builtInSchemas, true);
		}else{
			return outerJoinMappings(builtInSchemas, false);
		}
	}

	public List<Rule> outerJoinMappingsForInversion(Map<String, Schema> builtInSchemas) {
		return outerJoinMappings(builtInSchemas, false);
	}
    public List<Rule> outerJoinMappings(Map<String, Schema> builtInSchemas, boolean addAntiJoins) {		Map<Integer, List<Rule>> orderedOJMappings = new HashMap<Integer, List<Rule>>();

		if(isJoinRel(getType())){

			//				We need to consider each mapping individually + each subpath of mappings
			Peer peer = getMappings().get(0).getProvenanceRelation().getPeer();
			Schema schema = getMappings().get(0).getProvenanceRelation().getSchema();
			RelationContext relCtx = new RelationContext(this, schema, peer, true);

			try{
				if(isFullOuterJoinRel(getType())){
					//				Full Outer Join
					for(int i = 0; i < getMappings().size(); i++){				
						orderedOJMappings = outerJoinPaths(relCtx, i, builtInSchemas, orderedOJMappings, false, addAntiJoins);
					}
				}else if(isLeftOuterJoinRel(getType())) {
					orderedOJMappings = outerJoinPaths(relCtx, 0, builtInSchemas, orderedOJMappings, false, addAntiJoins);
				}else if(isRightOuterJoinRel(getType())) {
					orderedOJMappings = rightOuterJoinPaths(relCtx, 0, builtInSchemas, orderedOJMappings, false, addAntiJoins);
				}else if(isInnerJoinRel(getType())) {
					orderedOJMappings = innerJoinPath(relCtx, 0, builtInSchemas, orderedOJMappings);
				}
			}catch (Exception e) {
				//						This should never happen anyway - if this exception existed
				//						it would have been thrown earlier, when the mapping relation
				//						was first created
				e.printStackTrace();
			}
		}

		//		return ojMappings;

		//  Returned "ordered" mappings instead
		List<Rule> ret = new ArrayList<Rule>();

		//		Only include tuples that appear in at least a path of length 2
		for(int i = getMappings().size(); i > 1; i--){
			//		for(int i = getMappings().size(); i >= 1; i--){
			if(orderedOJMappings.containsKey((Integer)i))
				ret.addAll(orderedOJMappings.get((Integer)i));
		}
		return ret;
	}

	public List<Rule> outerUnionMappings(Map<String, Schema> builtInSchemas) {
		List<Rule> ouMappings = new ArrayList<Rule>();

		if(ProvRelType.OUTER_UNION.equals(getType())){

			//				We need to consider each mapping individually + each subpath of mappings
			Peer peer = getMappings().get(0).getProvenanceRelation().getPeer();
			Schema schema = getMappings().get(0).getProvenanceRelation().getSchema();
			RelationContext relCtx = new RelationContext(this, schema, peer, true);

			for(int i = 0; i < getMappings().size(); i++){
				try{
					Mapping m = getMappings().get(i);
					
					//						Create mapping for ith
					//						List<AtomArgument> headVars1 = provRelAtomArgsForMappingJoin(i, i);
					
					//						Create the appropriate atom of the provenance Relation for this mapping
					//						Atom mapping_head1 = new Atom(relCtx, headVars1);
					
					List<AtomArgument> headVars2 = provRelAtomArgsForMappingUnion(i, false);
					Atom mapping_head2 = new Atom(relCtx, headVars2);
					mapping_head2.deskolemizeAllVars();
					Rule s2pj = createUnionRuleForMapping(mapping_head2, m, builtInSchemas, true);
					ouMappings.add(s2pj);
					Debug.println(s2pj.toString());
				}catch (Exception e) {
					//						This should never happen anyway - if this exception existed
					//						it would have been thrown earlier, when the mapping relation
					//						was first created
					e.printStackTrace();
				}
			}
		}
		return ouMappings;
	}

	/**
	 * Create an outer union of two outer union relations
	 *  
	 * @param rel1 1st outer union parameter
	 * @param rel2 2nd outer union parameter
	 * @return The outer union relation schema
	 * @throws UnsupportedTypeException
	 * @throws IncompatibleTypesException
	 */	
	public static ProvenanceRelation union(ProvenanceRelation rel1, ProvenanceRelation rel2) throws UnsupportedTypeException, IncompatibleTypesException {
		List<ProvenanceRelation> rels = new ArrayList<ProvenanceRelation>();
		rels.add(rel1);
		rels.add(rel2);
		return createUnionProvRelSchema(rels, ProvRelType.OUTER_UNION);
	}

	/**
	 * Convert a "plain" ProvenanceRelation into one that can be unioned
	 * 
	 * @param rel1
	 * @return The outer union relation schema
	 * @throws UnsupportedTypeException
	 * @throws IncompatibleTypesException
	 */
	public static ProvenanceRelation createSingleProvRelSchema(ProvenanceRelation rel1) throws UnsupportedTypeException, IncompatibleTypesException {
		List<ProvenanceRelation> rels = new ArrayList<ProvenanceRelation>();
		rels.add(rel1);
		return createUnionProvRelSchema(rels, ProvRelType.SINGLE);
	}

	/**
	 * Create an outer union of a list of outer union relations
	 *  
	 * @param rels outer union parameters
	 * @return The outer union relation schema
	 * @throws UnsupportedTypeException
	 * @throws IncompatibleTypesException
	 */	
	public static ProvenanceRelation createUnionProvRelSchema(List<ProvenanceRelation> rels, ProvRelType type) throws UnsupportedTypeException, IncompatibleTypesException {
		List<Mapping> allMappings = new ArrayList<Mapping>();
		String name = "";
		String catalog = "";
		String schema = "";
		int i = 0;
		for(ProvenanceRelation rel : rels){
			allMappings.addAll(rel.getMappings());
			if(i == 0){
				name = rel.getName();
				catalog = rel.getDbCatalog();
				schema = rel.getDbSchema();
			}else{
				name = name + "_U_" + rel.getName();
			}
			i++;
		}
		List<List<RuleFieldMapping>> rfmappings = new ArrayList<List<RuleFieldMapping>>();
		for(Mapping m : allMappings){
			rfmappings.add(m.getAppropriateRuleFieldMapping());
		}

		ProvenanceRelation res =  createUnionProvRelSchema(name, rfmappings, catalog, schema, type);

		List<Relation> rels2 = new ArrayList<Relation>();
		rels2.addAll(rels);
		res.deriveLabeledNulls(rels2);

		res.setMappings(allMappings);
		res.setRels(rels);

		return res;
	}

	/**
	 * Create an outer union of a list of mappings
	 * 
	 * @param relName the name of the resulting relation
	 * @param rfmappings the rule field mappings of the corresponding mappings
	 * @param dbCatalog
	 * @param dbSchema
	 * @return The outer union schema
	 * @throws UnsupportedTypeException
	 * @throws IncompatibleTypesException
	 */
	protected static ProvenanceRelation createUnionProvRelSchema(String relName, List<List<RuleFieldMapping>> rfmappings, 
			String dbCatalog, String dbSchema) throws UnsupportedTypeException, IncompatibleTypesException {

		return createUnionProvRelSchema(relName, rfmappings, dbCatalog, dbSchema, ProvRelType.OUTER_UNION);
	}

	private static void ensureNotEmpty(RuleFieldMapping rm) {
		if (rm.srcColumns.size() == 0 && rm.srcArg == null){
			Debug.println("NULL FIELD?");
			rm.srcColumns.add(new RelationField(OuterUnionColumn.ORIGINAL_NULL, "", 
					new StringType(true, true, false, 1)));
		}
	}

	protected static ProvenanceRelation createUnionProvRelSchema(String relName, List<List<RuleFieldMapping>> rfmappings, 
			String dbCatalog, String dbSchema, ProvRelType type) throws UnsupportedTypeException, IncompatibleTypesException {

		Set<RuleFieldMapping> alreadyAdded = new HashSet<RuleFieldMapping>();

		int inx = 0;
		List<ProvenanceRelationColumn> columns = new ArrayList<ProvenanceRelationColumn>();

		int fldInx = 0;
		for (List<RuleFieldMapping> rel : rfmappings) {
			// Scan each mapping in the list and see if it is already in the
			// target schema.  If not, we need to add it.
			List<ProvenanceRelationColumn> matchedColumns = new ArrayList<ProvenanceRelationColumn>();

			for (RuleFieldMapping rm : rel) {
				boolean fnd = false;
				ProvenanceRelationColumn orig = null;

				if(rm.trgColumns.size() > 0){
					// Look at the target columns to determine if we should merge
					Iterator<RelationField> it = rm.trgColumns.iterator();
					RelationField name = null;
					while (it.hasNext() && !fnd) {
						// See if it is part of the output relation -- if so,
						// then it must already be part of the prov relation
						name = it.next();
						// Find the corresponding entry in alreadyAdded.
						// Merge in all of our source columns.
						orig = getMatchingTargetColumn(columns, name);
						if(orig != null){
							if(matchedColumns.contains(orig)){
								fnd = false;
							}else{
								fnd = true;
								matchedColumns.add(orig);
							}
						}else{
							fnd = false;
						}
					}
				}else{
					// No target columns - look at the source columns instead
					Iterator<RelationField> it = rm.srcColumns.iterator();

					RelationField name = null;
					while (it.hasNext() && !fnd) {
						name = it.next();
						// Merge in all of our source columns.
						orig = getMatchingSourceColumn(columns, name);
						if(orig != null){
							if(matchedColumns.contains(orig)){
								fnd = false;
							}else{
								fnd = true;
								matchedColumns.add(orig);
							}
						}else{
							fnd = false;
						}
					}

				}


				//				If it isn't in the existing target columns, then add a new column
				//				if (!fnd || rm.trgColumns.size() == 0 || rm.srcColumns.size() == 0) {
				if (!fnd) {

					//				No source columns - this is an existential variable in the target
					//				What if there are no target columns?
					ensureNotEmpty(rm);

					RelationField oldField = rm.outputField;
					RelationField nField = new RelationField("C" + Integer.toString(fldInx),
							oldField.getDescription(), oldField.isNullable(), 
							oldField.getSQLTypeName());
					fldInx++;

					orig = new ProvenanceRelationColumn(nField, rm.isIndex, inx,
							rm.srcColumns, rm.trgColumns, rm.srcArg);

					columns.add(orig);
					alreadyAdded.add(rm);

					assert(orig.getSourceColumns().get(inx) == rm.srcColumns);
					assert(orig.getDistinguishedColumns().get(inx) == rm.trgColumns);
					assert(orig.getSourceArgs().get(inx) == rm.srcArg);
					assert(orig.getSourceColumns().size() == inx + 1);
					assert(orig.getSourceArgs().size() == inx + 1);
					assert(orig.getDistinguishedColumns().size() == inx + 1);
				} else { 
					// found, but may need to set key, if not set
					if(rm.isIndex && !orig.isIndex())
						orig.setIndex(true);

					ensureNotEmpty(rm);

					// The next line should have no effect since the col is already
					// there:
					if (inx < orig.getSourceArgs().size()) {
						orig.getSourceArgs().set(inx, rm.srcArg);
						if (orig.getSourceColumns().get(inx) == null)
							orig.getSourceColumns().set(inx, rm.srcColumns);
						else
							orig.getSourceColumns().get(inx).addAll(rm.srcColumns);
						if (orig.getDistinguishedColumns().get(inx) == null)
							orig.getDistinguishedColumns().set(inx, rm.trgColumns);
						else
							orig.getDistinguishedColumns().get(inx).addAll(rm.trgColumns);
					} else {
						orig.getSourceArgs().add(rm.srcArg);
						orig.getSourceColumns().add(rm.srcColumns);
						orig.getDistinguishedColumns().add(rm.trgColumns);
					}
					assert(orig.getSourceColumns().size() == inx + 1);
					assert(orig.getSourceArgs().size() == inx + 1);
					assert(orig.getDistinguishedColumns().size() == inx + 1);
				}
				// Even the columns all out by padding with nulls

				for (ProvenanceRelationColumn o : columns) {
					if (o.getDistinguishedColumns().size() <= inx)
						o.getDistinguishedColumns().add(null);
					if (o.getSourceColumns().size() <= inx)
						o.getSourceColumns().add(null);
					if (o.getSourceArgs().size() <= inx)
						o.getSourceArgs().add(null);
				}
				assert(orig.getSourceColumns().size() == inx + 1);
				assert(orig.getSourceArgs().size() == inx + 1);
				assert(orig.getDistinguishedColumns().size() == inx + 1);

			}
			inx++;
		}

		ProvenanceRelation ret = createOuterUnionRelation(relName, columns, dbCatalog, dbSchema, type);
		ret.setType(type);
		createProvRelKey(ret, 0);
		return ret;
	}

	/**
	 * Create an outer union of a list of mappings
	 * 
	 * @param relName the name of the resulting relation
	 * @param rfmappings the rule field mappings of the corresponding mappings
	 * @param dbCatalog
	 * @param dbSchema
	 * @return The outer union schema
	 * @throws UnsupportedTypeException
	 * @throws IncompatibleTypesException
	 */
	public static ProvenanceRelation unionMergeTargetOnly(String relName, List<List<RuleFieldMapping>> rfmappings, 
			String dbCatalog, String dbSchema) throws UnsupportedTypeException, IncompatibleTypesException {

		Set<RuleFieldMapping> alreadyAdded = new HashSet<RuleFieldMapping>();

		int inx = 0;
		List<ProvenanceRelationColumn> columns = new ArrayList<ProvenanceRelationColumn>();

		for (List<RuleFieldMapping> rel : rfmappings) {
			// Scan each mapping in the list and see if it is already in the
			// target schema.  If not, we need to add it.
			List<ProvenanceRelationColumn> matchedColumns = new ArrayList<ProvenanceRelationColumn>();

			for (RuleFieldMapping rm : rel) {
				boolean fnd = false;
				ProvenanceRelationColumn orig = null;

				// Look at the target columns to determine if we should merge
				Iterator<RelationField> it = rm.trgColumns.iterator();
				RelationField name = null;
				while (it.hasNext() && !fnd) {
					// See if it is part of the output relation -- if so,
					// then it must already be part of the prov relation
					name = it.next();
					// Find the corresponding entry in alreadyAdded.
					// Merge in all of our source columns.
					orig = getMatchingTargetColumn(columns, name);
					if(orig != null){
						if(matchedColumns.contains(orig)){
							fnd = false;
						}else{
							fnd = true;
							matchedColumns.add(orig);
						}
					}else{
						fnd = false;
					}
				}


				//				If it isn't in the existing target columns, then add a new column
				if (!fnd || rm.trgColumns.size() == 0 || rm.srcColumns.size() == 0) {

					//					No source columns - this is an existential variable in the target
					//					What if there are no target columns?
					ensureNotEmpty(rm);

					orig = new ProvenanceRelationColumn(rm.outputField, rm.isIndex, inx,
							rm.srcColumns, rm.trgColumns, rm.srcArg);

					columns.add(orig);
					alreadyAdded.add(rm);

					assert(orig.getSourceColumns().get(inx) == rm.srcColumns);
					assert(orig.getDistinguishedColumns().get(inx) == rm.trgColumns);
					assert(orig.getSourceArgs().get(inx) == rm.srcArg);
					assert(orig.getSourceColumns().size() == inx + 1);
					assert(orig.getSourceArgs().size() == inx + 1);
					assert(orig.getDistinguishedColumns().size() == inx + 1);
				} else {
					ensureNotEmpty(rm);

					// The next line should have no effect since the col is already
					// there:
					if (inx < orig.getSourceArgs().size()) {
						orig.getSourceArgs().set(inx, rm.srcArg);
						if (orig.getSourceColumns().get(inx) == null)
							orig.getSourceColumns().set(inx, rm.srcColumns);//rm.srcColumns);
						else
							orig.getSourceColumns().get(inx).addAll(rm.srcColumns);//rm.srcColumns);
						if (orig.getDistinguishedColumns().get(inx) == null)
							orig.getDistinguishedColumns().set(inx, rm.trgColumns);
						else
							orig.getDistinguishedColumns().get(inx).addAll(rm.trgColumns);
					} else {
						orig.getSourceArgs().add(rm.srcArg);
						orig.getSourceColumns().add(rm.srcColumns);//rm.srcColumns);
						orig.getDistinguishedColumns().add(rm.trgColumns);
					}
					assert(orig.getSourceColumns().size() == inx + 1);
					assert(orig.getSourceArgs().size() == inx + 1);
					assert(orig.getDistinguishedColumns().size() == inx + 1);
				}
				// Even the columns all out by padding with nulls

				for (ProvenanceRelationColumn o : columns) {
					if (o.getDistinguishedColumns().size() <= inx)
						o.getDistinguishedColumns().add(null);
					if (o.getSourceColumns().size() <= inx)
						o.getSourceColumns().add(null);
					if (o.getSourceArgs().size() <= inx)
						o.getSourceArgs().add(null);
				}
				assert(orig.getSourceColumns().size() == inx + 1);
				assert(orig.getSourceArgs().size() == inx + 1);
				assert(orig.getDistinguishedColumns().size() == inx + 1);

			}
			inx++;
		}

		ProvenanceRelation ret = createOuterUnionRelation(relName, columns, dbCatalog, dbSchema, ProvRelType.OUTER_UNION);
		ret.setType(ProvRelType.OUTER_UNION);
		createProvRelKey(ret, 0);
		return ret;
	}


	/**
	 * Create the schema for the outer union relation
	 * 
	 * @param relName
	 * @param fm
	 * @return
	 * @throws UnsupportedTypeException 
	 */
	public static ProvenanceRelation createOuterUnionRelation(String relName,
			List<ProvenanceRelationColumn> columns, String dbCatalog,
			String dbSchema, ProvRelType type) throws UnsupportedTypeException {
		List<RelationField> fields = new ArrayList<RelationField>();
		//		Set<Integer> indexes = new HashSet<Integer>();

		//TODO: Is DB datatype important here??
		if(type.equals(ProvRelType.OUTER_UNION))
			fields.add(new RelationField(ProvenanceRelation.MRULECOLNAME, "Mapping rule ID", false, "INTEGER"));
		//		int i = 0;
		for (ProvenanceRelationColumn m : columns) {
			RelationField nField = m.getColumn();
			//				new RelationField("C" + Integer.toString(i),//m.getColumn().getName(),
			//					m.getColumn().getDescription(), m.getColumn().isNullable(), 
			//					m.getColumn().getSQLTypeName());
			fields.add(nField);//m.getColumn());
			//			if (m.isIndex())
			//				indexes.add(i + 1);
			//			i++;
		}

		// Find a rule, any rule...
		//		Rule aHeadRule = (Rule)oum.getRFMappings().iterator().next().get(0).rule;
		//		ProvenanceRelation rel =  new ProvenanceRelation(aHeadRule.getHead().getRelation().getDbCatalog(), 
		//				aHeadRule.getHead().getRelation().getDbSchema(),
		//				"p"+relName, "p"+relName, 
		//				"Cache relation for p" + relName,
		//				true,
		//				fields
		//		);

		ProvenanceRelation rel =  new ProvenanceRelation(dbCatalog, 
				dbSchema,
				"p"+relName, "p"+relName, 
				//				"Cache relation for p" + relName,
				"p" + relName,
				true,
				fields
		);

		//		rel.setOUMapping(oum);
		rel.setColumns(columns);

		return rel;
	}

	/**
	 * Defines the executable (SQL DDL, etc.) statement(s) corresponding
	 * to constructing the outer union schema
	 * 
	 * @param rel
	 * @param fm
	 * @param withLogging
	 * @return
	 */
	public static Set<Integer> createProvRelKey(final ProvenanceRelation rel, int numMappings) { 
		final Set<Integer> indexes = new HashSet<Integer>();
		List<ProvenanceRelationColumn> columns = rel.getColumns();
		final List<String> indexFields = new ArrayList<String> ();

		//		int i = 1;
		for (final ProvenanceRelationColumn m: columns) {
			if (m.isIndex())
				indexFields.add(m.getColumn().getName());
			//			indexes.add(i-1);
			//			i++;
		}

		//		final List<String> indexFields = new ArrayList<String> ();

		//		for (final Integer index : indexes)
		//		indexFields.add (rel.getField(index).getName());

		// Put these "after" the real key

		// Hack to add MRULE to the key ...
		if(rel.getType().equals(ProvRelType.OUTER_UNION)){
			indexes.add(0);
			indexFields.add("MRULE");
			//		}else if(rel.getType().equals(ProvRelType.OUTER_JOIN) || rel.getType().equals(ProvRelType.LEFT_OUTER_JOIN) || rel.getType().equals(ProvRelType.INNER_JOIN)){
		}else if(isOuterJoinRel(rel.getType())){
			for(int j = 0; j < numMappings; j++){
				indexes.add(j);
				indexFields.add(rel.getFields().get(j).getName());
			}
		}

		try
		{
			final PrimaryKey key = new PrimaryKey ("P" + rel.getName() + "_PK",
					rel,
					indexFields);
			rel.setPrimaryKey(key);
		}catch (final UnknownRefFieldException ex){}

		return indexes;
	}


	/**
	 * 
	 * @param s2p1 1st set of source2provenance mappings
	 * @param p2t1 1st set of provenance2target mappings
	 * @param s2p2 2nd set of source2provenance mappings
	 * @param p2t2 2nd set of provenance2target mappings
	 * @param s2pOut unioned (output) set of source2provenance mappings
	 * @param p2tOut unioned (output) set of provenance2target mappings
	 * @return the new (unioned) provenance relations
	 */
	//	protected static ProvenanceRelation union(ProvenanceRelation rel1, ProvenanceRelation rel2){
	////	"Collect" input provenance relations
	//	List<ProvenanceRelation> inRels = new ArrayList<ProvenanceRelation>();

	//	for(Mapping m : s2p1)
	//	for(Atom a : m.getBody())
	//	inRels.add((ProvenanceRelation)a.getRelation());

	//	if(inRels.size() > 1)
	//	assert(false);

	//	ProvenanceRelation unionRel = inRels.get(0);
	//	ProvenanceRelation newRel = (ProvenanceRelation)s2p2.getBody().get(0).getRelation();


	////	Figure out schema of output provenance relation(s)
	////	Should we assume that all parameters have the same provenance relation in the "middle"?	
	//	int i = 0;
	//	for(Mapping m : )


	////	For each old mapping, create appropriate union relation atom and rewrite old mappings
	////	Seems to make sense to go through s2p/p2t at the same time (I think it is 1-1)	

	//	}

	/**
	 * Create an outer join of two outer union relations
	 *  
	 * @param rel1 1st outer join parameter
	 * @param rel2 2nd outer join parameter
	 * @return The outer join relation schema
	 * @throws UnsupportedTypeException
	 * @throws IncompatibleTypesException
	 */	
	public static ProvenanceRelation createJoinProvRelSchema(ProvenanceRelation rel1, ProvenanceRelation rel2, ProvRelType joinType, IDb db) throws UnsupportedTypeException, IncompatibleTypesException {
		List<ProvenanceRelation> rels = new ArrayList<ProvenanceRelation>();
		rels.add(rel1);
		rels.add(rel2);
		return createJoinProvRelSchema(rels, joinType);
	}

	public static ProvenanceRelation createJoinProvRelSchema(List<ProvenanceRelation> rels, ProvRelType joinType) throws UnsupportedTypeException, IncompatibleTypesException {
		List<Mapping> allMappings = new ArrayList<Mapping>();
		String name = "";
		String catalog = "";
		String schema = "";
		int i = 0;
		for(ProvenanceRelation rel : rels){
			if(!rel.getType().equals(ProvRelType.SINGLE)){
				assert(false);
				Debug.println("Nested join/union not implemented yet");
				return null;
			}

			allMappings.addAll(rel.getMappings());
			if(i == 0){
				name = rel.getName();
				catalog = rel.getDbCatalog();
				schema = rel.getDbSchema();
			}else{
				name = name + "_J_" + rel.getName();
			}
			i++;
		}

		ProvenanceRelation res = null;

		List<List<RuleFieldMapping>> rfmappings = new ArrayList<List<RuleFieldMapping>>();

		if(isInnerJoinRel(joinType)) {
			rfmappings = findInnerJoinEqualColumns(allMappings);
		}else{
			for(Mapping m : allMappings){
				rfmappings.add(m.getAppropriateRuleFieldMapping());
			}
		}

		res =  createJoinProvRelSchema(name, rfmappings, allMappings.size(), catalog, schema, joinType);

		List<Relation> rels2 = new ArrayList<Relation>();
		rels2.addAll(rels);
		res.deriveLabeledNulls(rels2);

		res.setMappings(allMappings);
		res.setRels(rels);
		return res;
	}

	protected static ProvenanceRelation createJoinProvRelSchema(String relName, List<List<RuleFieldMapping>> rfmappings,
			int numMappings, String dbCatalog, String dbSchema, ProvRelType joinType) throws UnsupportedTypeException, IncompatibleTypesException {

		int inx = 0;
		int fldInx = 0;
		List<ProvenanceRelationColumn> columns = new ArrayList<ProvenanceRelationColumn>();

		for(List<RuleFieldMapping> rel : rfmappings){

			if(isInnerJoinRel(joinType)){
				//			Create only one attribute for each list of RFMs (same equivalence class)
				boolean isIndex = false;
				List<RelationField> srcColumns = new ArrayList<RelationField>();
				List<RelationField> trgColumns = new ArrayList<RelationField>();
				AtomArgument srcArg;

				if(rel.size() > 0){
					RuleFieldMapping rfm = rel.get(0);
					RelationField oldField = rfm.outputField;
					RelationField nField = new RelationField("C" + Integer.toString(fldInx),
							oldField.getDescription(), oldField.isNullable(), 
							oldField.getSQLTypeName());
					fldInx++;
					srcArg = rfm.srcArg;

					for (RuleFieldMapping rm : rel) {
						srcColumns.addAll(rm.srcColumns);
						trgColumns.addAll(rm.trgColumns);
						isIndex = isIndex || rm.isIndex;
					}

					ProvenanceRelationColumn col = new ProvenanceRelationColumn(nField, isIndex, 0,
							srcColumns, trgColumns, srcArg);

					columns.add(col);
				}
			}else{
				//			Create an attribute for each attribute in one of the mappings
				for (RuleFieldMapping rm : rel) {
					RelationField oldField = rm.outputField;
					RelationField nField = new RelationField("C" + Integer.toString(fldInx),
							oldField.getDescription(), oldField.isNullable(), 
							oldField.getSQLTypeName());
					fldInx++;

					ProvenanceRelationColumn col = new ProvenanceRelationColumn(nField, rm.isIndex, inx,
							rm.srcColumns, rm.trgColumns, rm.srcArg);

					columns.add(col);
				}
			}
			inx++;

		}

		ProvenanceRelation ret = createOuterJoinRelation(relName, columns, numMappings, dbCatalog, dbSchema, joinType);

		ret.setType(joinType);
		createProvRelKey(ret, numMappings);
		return ret;
	}

	/**
	 * Create the schema for the outer join relation.
	 * 
	 * @param relName
	 * @param fm
	 * @return
	 * @throws UnsupportedTypeException 
	 */
	public static ProvenanceRelation createOuterJoinRelation(String relName,
			List<ProvenanceRelationColumn> columns, int nMappings, String dbCatalog,
			String dbSchema, ProvRelType joinType) throws UnsupportedTypeException {
		List<RelationField> fields = new ArrayList<RelationField>();

		//		Create boolean attributes, to identify the parts of this
		//		outer join tuple that are actually there/not padded with nulls
		if(!isInnerJoinRel(joinType)) {
			for(int i = 0; i < nMappings; i++){
				//			StringType foo = new StringType(false, false, 1);
				IntType bar = new IntType(false, true);
				RelationField mField = new RelationField("M" + Integer.toString(i),
						"M" + Integer.toString(i), false, 
						bar.getSQLTypeName());
				fields.add(mField);
			}
		}

		for (ProvenanceRelationColumn m : columns) {
			RelationField nField = m.getColumn();
			fields.add(nField);//m.getColumn());
		}

		ProvenanceRelation rel =  new ProvenanceRelation(dbCatalog, 
				dbSchema,
				"p"+relName, "p"+relName, 
				//				"Cache relation for p" + relName,
				"p" + relName,
				true,
				fields
		);

		rel.setColumns(columns);
		return rel;
	}

	/**
	 * Finds the ProvenanceRelationColumn that corresponds to the same distinguished
	 * variable in the head of the mapping rule
	 * 
	 * @param matchSet
	 * @param name
	 * @return
	 */
	protected static ProvenanceRelationColumn getMatchingTargetColumn(List<ProvenanceRelationColumn> columns,
			RelationField name) {
		//		String name) {
		for (ProvenanceRelationColumn c : columns) {
			Iterator<List<RelationField>> it2 = c.getDistinguishedColumns().iterator();
			while (it2.hasNext()) {
				List<RelationField> orig = it2.next();

				if (orig != null && orig.contains(name))
					return c;
			}
		}
		return null;
	}

	/**
	 * Finds the ProvenanceRelationColumn that corresponds to the same distinguished
	 * variable in the head of the mapping rule
	 * 
	 * @param matchSet
	 * @param name
	 * @return
	 */
	protected static ProvenanceRelationColumn getMatchingSourceColumn(List<ProvenanceRelationColumn> columns,
			RelationField name) {
		//		String name) {
		for (ProvenanceRelationColumn c : columns) {
			Iterator<List<RelationField>> it2 = c.getSourceColumns().iterator();
			while (it2.hasNext()) {
				List<RelationField> orig = it2.next();

				if (orig != null && orig.contains(name))
					return c;
			}
		}
		return null;
	}



	//	public void setRFMappings(List<List<RuleFieldMapping>> mappings) {
	//	this.rfmappings = mappings;
	//	}
	//	/**
	//	* @return the rule field mappings
	//	*/
	//	public List<List<RuleFieldMapping>> getRFMappings() {
	//	return rfmappings;
	//	}

	/**
	 * @param mappings the mappings to set
	 */
	public void setMappings(List<Mapping> mappings) {
		_mappings = mappings;
	}
	/**
	 * @return the mappings
	 */
	public List<Mapping> getMappings() {
		return _mappings;
	}

	//	public void setRFMappings(List<List<RuleFieldMapping>> mappings) {
	//	this.rfmappings = mappings;
	//	}
	//	/**
	//	* @return the rule field mappings
	//	*/
	//	public List<List<RuleFieldMapping>> getRFMappings() {
	//	return rfmappings;
	//	}

	/**
	 * @param rels the provenance relations to "combine"
	 */
	public void setRels(List<ProvenanceRelation> rels) {
		_rels = rels;
	}

	/**
	 * @return the provenance relations that this "combines"
	 */
	public List<ProvenanceRelation> getRels() {
		return _rels;
	}

	/**
	 * @param columns the columns to set
	 */
	public void setColumns(List<ProvenanceRelationColumn> columns) {
		_columns = columns;
	}
	/**
	 * @return the columns
	 */
	public List<ProvenanceRelationColumn> getColumns() {
		return _columns;
	}

	public void setType(ProvRelType type){
		_type = type;
	}

	public ProvRelType getType(){
		return _type;
	}

	//	public void setRFMappings(List<List<RuleFieldMapping>> mappings) {
	//	this.rfmappings = mappings;
	//	}
	//	/**
	//	* @return the rule field mappings
	//	*/
	//	public List<List<RuleFieldMapping>> getRFMappings() {
	//	return rfmappings;
	//	}

	private static final long serialVersionUID = 1L;

	private List<ProvenanceRelationColumn> _columns;

	protected List<Mapping> _mappings;

	protected List<ProvenanceRelation> _rels;

	//	protected List<List<RuleFieldMapping>> rfmappings;

	public enum ProvRelType 
	{
		SINGLE,   // Single mapping 
		OUTER_UNION,  // Outer Union of Mappings
		SIMULATED_OUTER_JOIN,	// Outer Join of Mappings simulated by a set of inner joins
		SIMULATED_LEFT_OUTER_JOIN,	// Left Outer Join of Mappings simulated by a set of inner joins
		SIMULATED_RIGHT_OUTER_JOIN,	// Right Outer Join of Mappings simulated by a set of inner joins
		OUTER_JOIN,	// Real Outer Join of Mappings
		LEFT_OUTER_JOIN,	// Real Left Outer Join of Mappings
		RIGHT_OUTER_JOIN,	// Real Right Outer Join of Mappings
		INNER_JOIN // Inner Join of Mappings
	};

	protected ProvRelType _type;

	public static String MRULECOLNAME = "MRULE";

	/** 
	 * Get the database relation name for this relation
	 * (can be different from Orchestra relation name which 
	 * must be unique for a given Orchestra schema) 
	 * @return Database table name
	 */
	public String getDbSchema ()
	{
		if (Config.getTempTables())
			return ISqlStatementGen.sessionSchema;
		else
			return _dbSchema;
	}
	/**
	 * Get the fully qualified id of this relation over the underlying database.<BR>
	 * The id is computed based on the DbCatalog, the DbSchema and the DbName. Thus this id 
	 * is supposed to be unique for a given database.
	 * @return The relation id
	 * @see Schema
	 */  
	public String getFullQualifiedDbId ()
	{
		//	 TODO:LOW Could a schema be defined over multiple dbs? Sounds not likely to occur with orchestra for 
		//	 materialized schemas but what about coming virtual schemas? 
		if (Config.getTempTables())
			return getFullQualifiedDbId(getDbCatalog(), ISqlStatementGen.sessionSchema, getName());
		else
			return getFullQualifiedDbId(getDbCatalog(), getDbSchema(), getName());
	}

	public String getFullQualifiedOUProvDbId ()
	{
		//	 TODO:LOW Could a schema be defined over multiple dbs? Sounds not likely to occur with orchestra for 
		//	 materialized schemas but what about coming virtual schemas? 
		if (Config.getTempTables())
			return getFullQualifiedDbId(getDbCatalog(), ISqlStatementGen.sessionSchema, "p" + getName());
		else
			return getFullQualifiedDbId(getDbCatalog(), getDbSchema(), "p" + getName());
	}

	/**
	 * Compute a relation fully qualified db name based on the DbCatalog, 
	 * the DbSchema and the DbName
	 * @param catalog DbCatalog for the relation id to compute
	 * @param schema  DbSchema for the relation id to compute
	 * @param name  Relation's name
	 * @return Relation id
	 * @see Schema
	 */
	public static String getFullQualifiedDbId (String catalog, String schema, String name)
	{
		if (Config.isDB2()) {
			String id="";
			if (catalog != null)
				id = catalog + ".";
			if (Config.getTempTables())
				id += ISqlStatementGen.sessionSchema + ".";
			else
				id += schema + ".";
			id += name;
			return id;
		} else
			return Relation.getFullQualifiedDbId(catalog, schema, name);
	}

	public static boolean isJoinRel(ProvRelType type)
	{
		return ProvRelType.SIMULATED_OUTER_JOIN.equals(type) || 
		ProvRelType.SIMULATED_LEFT_OUTER_JOIN.equals(type) ||
		ProvRelType.SIMULATED_RIGHT_OUTER_JOIN.equals(type) || 
		ProvRelType.OUTER_JOIN.equals(type) || 
		ProvRelType.LEFT_OUTER_JOIN.equals(type) ||
		ProvRelType.RIGHT_OUTER_JOIN.equals(type) ||
		ProvRelType.INNER_JOIN.equals(type) ;
	}

	public static boolean isOuterJoinRel(ProvRelType type)
	{
		return ProvRelType.SIMULATED_OUTER_JOIN.equals(type) || 
		ProvRelType.SIMULATED_LEFT_OUTER_JOIN.equals(type) ||
		ProvRelType.SIMULATED_RIGHT_OUTER_JOIN.equals(type) || 
		ProvRelType.OUTER_JOIN.equals(type) || 
		ProvRelType.LEFT_OUTER_JOIN.equals(type) ||
		ProvRelType.RIGHT_OUTER_JOIN.equals(type) ;
	}

	public static boolean isSimulatedOuterJoinRel(ProvRelType type)
	{
		return ProvRelType.SIMULATED_OUTER_JOIN.equals(type) || 
		ProvRelType.SIMULATED_LEFT_OUTER_JOIN.equals(type) ||
		ProvRelType.SIMULATED_RIGHT_OUTER_JOIN.equals(type) ;
	}

	public static boolean isRealOuterJoinRel(ProvRelType type)
	{
		return ProvRelType.OUTER_JOIN.equals(type) || 
		ProvRelType.LEFT_OUTER_JOIN.equals(type) ||
		ProvRelType.RIGHT_OUTER_JOIN.equals(type) ;
	}

	public static boolean isFullOuterJoinRel(ProvRelType type) 
	{
		return ProvRelType.SIMULATED_OUTER_JOIN.equals(type) || 
		ProvRelType.OUTER_JOIN.equals(type) ;
	}

	public static boolean isLeftOuterJoinRel(ProvRelType type) 
	{
		return ProvRelType.SIMULATED_LEFT_OUTER_JOIN.equals(type) || 
		ProvRelType.LEFT_OUTER_JOIN.equals(type) ;
	}

	public static boolean isRightOuterJoinRel(ProvRelType type) 
	{
		return ProvRelType.SIMULATED_RIGHT_OUTER_JOIN.equals(type) || 
		ProvRelType.RIGHT_OUTER_JOIN.equals(type) ;
	}

	public static boolean isInnerJoinRel(ProvRelType type) 
	{
		return ProvRelType.INNER_JOIN.equals(type) ;
	}
}
