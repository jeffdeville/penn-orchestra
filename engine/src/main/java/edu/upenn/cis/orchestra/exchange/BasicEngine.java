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
package edu.upenn.cis.orchestra.exchange;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.datalog.DatalogEngine;
import edu.upenn.cis.orchestra.datalog.DatalogSequence;
import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datalog.atom.Atom.AtomType;
import edu.upenn.cis.orchestra.datamodel.ITranslationRules;
import edu.upenn.cis.orchestra.datamodel.ITranslationState;
import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationContext;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.exceptions.IncompatibleKeysException;
import edu.upenn.cis.orchestra.datamodel.exceptions.IncompatibleTypesException;
import edu.upenn.cis.orchestra.datamodel.exceptions.RelationUpdateException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnsupportedTypeException;
import edu.upenn.cis.orchestra.datamodel.iterators.ResultSetIterator;
import edu.upenn.cis.orchestra.dbms.DbFactory;
import edu.upenn.cis.orchestra.dbms.IDb;
import edu.upenn.cis.orchestra.deltaRules.DeletionDeltaRuleGen;
import edu.upenn.cis.orchestra.deltaRules.DeltaRuleGen;
import edu.upenn.cis.orchestra.deltaRules.IDeltaRuleGen;
import edu.upenn.cis.orchestra.deltaRules.IDeltaRules;
import edu.upenn.cis.orchestra.deltaRules.InsertionDeltaRuleGen;
import edu.upenn.cis.orchestra.exchange.exceptions.MappingNotFoundException;
import edu.upenn.cis.orchestra.exchange.flatfile.FileDb;
import edu.upenn.cis.orchestra.mappings.MappingTopologyTest;
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.provenance.ProvenanceRelation;
import edu.upenn.cis.orchestra.provenance.ProvenanceRelation.ProvRelType;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.XMLParseException;

/**
 * 
 * @author gkarvoun
 * 
 * Basic abstraction of an update exchange engine, generally associated with a particular
 * class of DBMS (e.g., Tukwila, XML engine, SQL engine).
 *
 */
public abstract class BasicEngine implements IEngine {

	private ITranslationState _state;

	protected OrchestraSystem _system;
	protected IDb _mappingDb;

	/** The delta rules for insertion. */
	protected IDeltaRules _insertionRules;
	
	/** The delta rules for deletion. */
	protected IDeltaRules _deletionRules;
	
//	protected IDb _updateDb;

	public ITranslationState getState(){
		return _state;
	}

	public IDb getMappingDb() {
		return _mappingDb;
	}

//	public IDb getUpdateDb() {
//	return _updateDb;
//	}

	public List<DatalogSequence> getIncrementalDeletionProgram() {
		return _deletionRules.getCode();
	}

	/**
	 * Returns a {@code Document} representing the incremental deletion program
	 * of this engine.
	 * 
	 * @return a {@code Document} representing the incremental deletion program
	 *         of this engine.
	 */
	public Document serializeIncrementalDeletionProgram() {
		return _deletionRules.serialize();
	}
	
	public List<DatalogSequence> getIncrementalInsertionProgram() {
		return _insertionRules.getCode();
	}

	/**
	 * Returns a {@code Document} representing the incremental insertion program
	 * of this engine.
	 * 
	 * @return a {@code Document} representing the incremental insertion program
	 *         of this engine.
	 */
	public Document serializeIncrementalInsertionProgram() {
		return _insertionRules.serialize();
	}
	
	/**
	 * Constructor for update exchange engine 
	 * 
	 * @param mappingDb
	 * @param system
	 * @throws Exception
	 */
	public BasicEngine(IDb mappingDb, 
//			IDb updateDb, 
			OrchestraSystem system) throws Exception {
		_mappingDb = mappingDb;
//		_updateDb = updateDb;
		_system = system;
		if (_system.getMappingEngine() == null)
			_system.setMappingEngine(this);
		
		MappingTopologyTest.markLabeledNulls(_system.getAllSystemMappings(true));
		
		if (!MappingTopologyTest.isWeaklyAcyclic(_system.getAllSystemMappings(true), true))
			throw new RuntimeException("Mappings are not weakly acyclic!");
		
		syncTableSchemas(_system);
		
		ITranslationRuleGen transRuleGen = TranslationRuleGen.newInstance(_system
				.getAllSystemMappings(true), _system.getAllUserRelations(),
				_mappingDb.getBuiltInSchemas(), _system.isBidirectional());
		transRuleGen.computeTranslationRules(_system.getPeers(), _system.getTrustMapping());
		_state = transRuleGen.getState();
		
		//DomUtils.write(_state.serialize(_mappingDb.getBuiltInSchemas()), new
		//FileWriter("expectedTranslationRules.xml"));
		computeDeltaRules(_state);
		//DomUtils.write(_insertionRules.serialize(), new
		//FileWriter("expectedInsertionRules.xml"));
		//write(_insertionRules.serializeAsCode(), new FileWriter("expectedInsertionCode.xml"));
		//DomUtils.write(_deletionRules.serialize(), new
		//FileWriter("expectedDeletionRules.xml"));
		//write(_deletionRules.serializeAsCode(), new FileWriter("expectedDeletionCode.xml"));
		
		repairSchema();
		
		finishMappingSchemas(system);
		//testMappingSchemas(system);
	}
	
	protected abstract void syncTableSchemas(OrchestraSystem system) throws RelationUpdateException;
	
	public abstract void repairSchema() throws Exception;
	
	protected void finishMappingSchemas(OrchestraSystem system) throws RelationUpdateException {
		/*
		for (Mapping m : system.getAllSystemMappings(false)) {
			for (Atom a : m.getMappingHead()) {
				a.getRelation().markFinished();
				a.getSchema().markFinished();
			}

			for (Atom a : m.getBody()) {
				a.getRelation().markFinished();
				a.getSchema().markFinished();
			}
		}*/
		for (Schema s : system.getAllSchemas())
			s.markFinished();
	}
	
	/**
	 * Ensure that all mappings and schemas have the correct number of columns
	 * 
	 * @param system
	 * @throws RelationUpdateException
	 */
	protected void testMappingSchemas(OrchestraSystem system) throws RelationUpdateException {
		for (Mapping m : system.getAllSystemMappings(false)) {
			for (Atom a : m.getMappingHead())
				if (a.getValues().size() != a.getRelation().getNumCols())
					throw new RelationUpdateException("Mismatch in mapping " + m.getId() + ": " +
							a.getValues() + " mis-aligned with " + a.getRelation().getRelationName());

			for (Atom a : m.getBody())
				if (a.getValues().size() != a.getRelation().getNumCols())
					throw new RelationUpdateException("Mismatch in mapping " + m.getId() + ": " +
							a.getValues() + " mis-aligned with " + a.getRelation().getRelationName());
		}
		/*
		for (Mapping m : system.getAllSystemMappings(false)) {
			for (Atom a : m.getMappingHead())
				if (a.getValues().size() != a.getRelation().getNumCols())
					throw new RelationUpdateException("Mismatch in mapping " + m.getId() + ": " +
							a.getValues() + " mis-aligned with " + a.getRelation().getRelationName());

			for (Atom a : m.getBody())
				if (a.getValues().size() != a.getRelation().getNumCols())
					throw new RelationUpdateException("Mismatch in mapping " + m.getId() + ": " +
							a.getValues() + " mis-aligned with " + a.getRelation().getRelationName());
		}*/
	}

	
	/**
	 * Creates the set of delta rules for the system
	 */
	public void computeDeltaRules() {
		//if (_provenancePrep == null)
		//	_provenancePrep = createProvenanceStorage();

		IDeltaRuleGen insRuleGen = new InsertionDeltaRuleGen(_system, getState(), getMappingDb().getBuiltInSchemas());
		_insertionRules = insRuleGen.getDeltaRules();

		IDeltaRuleGen delRuleGen = new DeletionDeltaRuleGen(_system, getState(), getMappingDb().getBuiltInSchemas(), _system.isBidirectional());
		_deletionRules = delRuleGen.getDeltaRules();

	}
	
	/**
	 * Creates the set of delta rules for the system
	 */
	private void computeDeltaRules(ITranslationRules translationRules) {
		//if (_provenancePrep == null)
		//	_provenancePrep = createProvenanceStorage();

		IDeltaRuleGen insRuleGen = new InsertionDeltaRuleGen(_system, translationRules, getMappingDb().getBuiltInSchemas());
		_insertionRules = insRuleGen.getDeltaRules();

		if (Config.getDebug()) {
			try {
				PrintWriter pw = new PrintWriter(new File("insert.txt"));
				pw.println("** Insertion **");
				for (DatalogSequence seq : _insertionRules.getCode())
					pw.println(seq.toString());
				
				pw.println();
				pw.close();
			} catch (IOException ioe) {
				ioe.printStackTrace();
			}
		}

		IDeltaRuleGen delRuleGen = new DeletionDeltaRuleGen(_system, translationRules, getMappingDb().getBuiltInSchemas(), _system.isBidirectional());
		_deletionRules = delRuleGen.getDeltaRules();

		if (Config.getDebug()) {
			try {
				PrintWriter pw = new PrintWriter(new File("delete.txt"));
				pw.println("** Deletion **");
				for (DatalogSequence seq : _deletionRules.getCode())
					pw.println(seq.toString());
				pw.println();
				pw.close();
			} catch (IOException ioe) {
				ioe.printStackTrace();
			}
		}
		
		if (Config.getDebug()) {
			DatalogEngine de = new DatalogEngine(getMappingDb());
			try {
				_insertionRules.generate(de);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				_deletionRules.generate(de);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public void cleanupPreparedStmts() {
		System.out.println("NET: Cleanup prepared statements");
		if(_insertionRules != null){
			_insertionRules.cleanupPreparedStmts();
		}
		
		if(_deletionRules != null){
			_insertionRules.cleanupPreparedStmts();
		}
	}


	/**
	 * Creates a set of translation rules
	 * 
	 * @param dao
	 * @return
	 */

	public List<Rule> computeTranslationRules() throws Exception {
		return Collections.unmodifiableList(getState().getSource2TargetRules());
	}
	

	public void unionMappingRelations(String[] mappingRels) throws UnsupportedTypeException, 
	IncompatibleTypesException, MappingNotFoundException {
		List<RelationContext> rels = new ArrayList<RelationContext>();
		List<ProvenanceRelation> prels = new ArrayList<ProvenanceRelation>();

		List<RelationContext> outerUnionRels = new ArrayList<RelationContext>();
		outerUnionRels.addAll(getState().getOuterUnionRelations()); 

		for(int i = 0; i < mappingRels.length; i++){
			RelationContext relCtx = getState().getProvenanceRelationForMapping(mappingRels[i]);
			rels.add(relCtx);
			prels.add((ProvenanceRelation)relCtx.getRelation());
		}

		if(rels.size() > 0){
			ProvenanceRelation newRel = ProvenanceRelation.createUnionProvRelSchema(prels, ProvRelType.OUTER_UNION);
			RelationContext newRelCtx = new RelationContext(newRel, rels.get(0).getSchema(),
					rels.get(0).getPeer(), true);
			outerUnionRels.add(newRelCtx);
		}

		getState().setOuterUnionRels(outerUnionRels);
		computeDeltaRules();
	}

	public void joinMappingRelations(String[] mappingRels, ProvRelType joinType) throws UnsupportedTypeException, 
	IncompatibleTypesException, MappingNotFoundException {
		List<RelationContext> rels = new ArrayList<RelationContext>();
		List<ProvenanceRelation> prels = new ArrayList<ProvenanceRelation>();

		List<RelationContext> joinRels = new ArrayList<RelationContext>();
		
		if(ProvenanceRelation.isRealOuterJoinRel(joinType))
			joinRels.addAll(getState().getRealOuterJoinRelations());
		else if (ProvenanceRelation.isInnerJoinRel(joinType))
			joinRels.addAll(getState().getInnerJoinRelations());
		else
			joinRels.addAll(getState().getSimulatedOuterJoinRelations());
		
		for(int i = 0; i < mappingRels.length; i++){
			RelationContext relCtx = getState().getProvenanceRelationForMapping(mappingRels[i]);
			rels.add(relCtx);
			prels.add((ProvenanceRelation)relCtx.getRelation());
		}

		if(rels.size() > 0){
			ProvenanceRelation newRel = ProvenanceRelation.createJoinProvRelSchema(prels, joinType);
			RelationContext newRelCtx = new RelationContext(newRel, rels.get(0).getSchema(),
					rels.get(0).getPeer(), true);
			joinRels.add(newRelCtx);
		}
		
		if(ProvenanceRelation.isRealOuterJoinRel(joinType))
			getState().setRealOuterJoinRels(joinRels);
		else if (ProvenanceRelation.isInnerJoinRel(joinType))
			getState().setInnerJoinRels(joinRels);
		else
			getState().setSimulatedOuterJoinRels(joinRels);
		
		computeDeltaRules();
	}

	public void close() throws Exception {
		if (_mappingDb != null) {
			_mappingDb.disconnect();
			_mappingDb = null;
		}
//		if (_updateDb != null) {
//		_updateDb.disconnect();
//		_updateDb = null;
//		}
	}

	public void commit() throws Exception {
		_mappingDb.commit();
	}

	public void finalize() throws Exception {
		_mappingDb.finalize();
	}

//	public void reset() throws IOException, ParseException, Exception {
//	_d.resetCounters();
//	clearAllTables();
////	finalize();
//	commit();
//	_d.resetCounters();
//	}

	public void reset() throws Exception {
		throw new Exception("SHOULD NEVER GO HERE!");
	}

	public void softReset() throws Exception {
		throw new Exception("SHOULD NEVER GO HERE!");
	}

	public void clean() throws IOException, Exception {
		_mappingDb.resetCounters();
		dropAllTables();
		_mappingDb.resetCounters();
		finalize();
	}

	//protected abstract CreateProvenanceStorage createProvenanceStorage();

	//protected abstract CreateAuxiliaryStorage createAuxiliaryStorage();
	
	protected void computeProvenanceRelation(Mapping mapping, int i, List<RelationContext> mappingRels){

	}

	protected abstract List<String> createProvenanceTables(List<RelationContext> mappingRels);	
	protected abstract List<String> createPeerRelations(Schema sc);

	
	/**
	 * Computes the names of all of the tables in the Orchestra system.
	 * Package-level because it's SQL-specific
	 * 
	 * @param syst
	 * @param includeMappings: include the mapping relations
	 * @param includeDeltas: include the delta relations
	 * @param includeBase: include the base relations
	 */
	public static List<String> getNamesOfAllTables(final OrchestraSystem syst, 
			final boolean includeMappings,
			final boolean includeDeltas, final boolean includeBase) {
		final ArrayList<String> tables = new ArrayList<String>();

		for (final Peer p : syst.getPeers()){
			for (final Schema sc : p.getSchemas()){
				for (final Relation rel : sc.getRelations()){
					if (includeBase)
						tables.add(rel.getFullQualifiedDbId());

					if (includeDeltas)
						for (final Atom.AtomType type : Atom.AtomType.values()){
							String suffix = Atom.typeToString(type);

							//if((rel.getFullQualifiedDbId().endsWith("_L") || rel.getFullQualifiedDbId().endsWith("_R"))
							if (rel.isInternalRelation() && type != AtomType.RCH){

							}else if(type != AtomType.RCH && (!Config.getStratified() || type != AtomType.ALLDEL)
									&& (syst.isBidirectional() || type != AtomType.D)
							){
								if(!suffix.equals("")){
									suffix = "_" + suffix;
								}

								//							tables.add(rel.getFullQualifiedDbId() + "_" + type.toString());
								tables.add(rel.getFullQualifiedDbId() + suffix);
							}
						}
				}
			}
		}

		if(includeMappings){
			List<RelationContext> rels = null;

			if(syst.getMappingEngine() != null && syst.getMappingEngine().getMappingRelations() != null){
				rels = syst.getMappingEngine().getMappingRelations();
			}else{
				try{
					rels = TranslationRuleGen.computeProvenanceRelations(syst.getPeers(), syst.getAllSystemMappings(true));
				}catch(IncompatibleTypesException e){
					e.printStackTrace();
				}catch(IncompatibleKeysException ke){
					ke.printStackTrace();       	
				}
			}

			for(RelationContext rel : rels){

				for (final Atom.AtomType type : Atom.AtomType.values()) {
					String suffix = Atom.typeToString(type);

					if(type != AtomType.RCH && (!Config.getStratified() || type != AtomType.ALLDEL)
							&& (syst.isBidirectional() || type != AtomType.D)
					){
						if(!suffix.equals("")){
							suffix = "_" + suffix;
						}

						tables.add((rel.getRelation().getDbSchema()==null?"": rel.getRelation().getDbSchema()+".")
								+ rel.getRelation().getName() + suffix
						);
					}
				}
			}
		}

		return tables;
	}

	public static List<String> getNamesOfAllTablesFromDeltas(/*DeltaRules deltas,*/ final OrchestraSystem syst, boolean allTypes, boolean includeMappings, boolean includeOJ){
		List<String> names = new ArrayList<String>();
		getNamesOfAllTablesFromDeltas(syst, allTypes, includeMappings, includeOJ, names);
		return names;
	}
	
	public static void getNamesOfAllTablesFromDeltas(/*DeltaRules deltas,*/ final OrchestraSystem syst, boolean allTypes, boolean includeMappings, boolean includeOJ,
			Collection<String> names){
//		ArrayList<RelationContext> tables = new ArrayList<RelationContext>();
		HashMap<RelationContext, Integer> tables = new HashMap<RelationContext, Integer>();

		for(RelationContext rel : syst.getMappingEngine().getEdbs()){
			if(rel.getRelation().getName().endsWith(Relation.LOCAL))
				tables.put(rel, 0);
			else
				tables.put(rel, 1);
		}

		for(RelationContext rel : syst.getMappingEngine().getRej()){	
			tables.put(rel, 0);
		}

		for(RelationContext rel : syst.getMappingEngine().getIdbs()){	
			tables.put(rel, 1);
		}

		if(includeMappings){
//			for(RelationContext rel : syst.getMappingEngine().getMappingRelations()){
			for(RelationContext rel : syst.getMappingEngine().getState().getRealMappingRelations()){
				if(!tables.containsKey(rel)){
					tables.put(rel, 2);
				}
			}
		}

		if(includeOJ){
			for(RelationContext rel : syst.getMappingEngine().getState().getSimulatedOuterJoinRelations()){
				if(!tables.containsKey(rel))
					tables.put(rel, 2);
			}
			for(RelationContext rel : syst.getMappingEngine().getState().getInnerJoinRelations()){
				if(!tables.containsKey(rel))
					tables.put(rel, 2);
			}
			for(RelationContext rel : syst.getMappingEngine().getState().getRealOuterJoinRelations()){
				if(!tables.containsKey(rel))
					tables.put(rel, 2);
			}
		}

		for(RelationContext rel : tables.keySet()){

			if(allTypes){
				for (final Atom.AtomType type : Atom.AtomType.values()){
					String suffix = Atom.typeToString(type);

					if(((tables.get(rel) == 1) || (type != AtomType.RCH)) &&
							(!Config.getStratified() || type != AtomType.ALLDEL) 
							&& (syst.isBidirectional() || type != AtomType.D)
					){
						if(!suffix.equals("")){
							suffix = "_" + suffix;
						}
						if(!names.contains(rel.getRelation().getFullQualifiedDbId() + suffix))	
							names.add(rel.getRelation().getFullQualifiedDbId() + suffix);
					}
				}
			}else{
				if(!names.contains(rel.getRelation().getFullQualifiedDbId()))	
					names.add(rel.getRelation().getFullQualifiedDbId());
			}		
		}
	}

	protected List<RelationContext> getEdbs(){
		if (_state == null)
			return null;
		return _state.getEdbs(_mappingDb.getBuiltInSchemas());
	}

	private List<RelationContext> getRej(){
		if (_state == null)
			return null;
		return _state.getRej(_system);
	}

	protected List<RelationContext> getIdbs(){
		if (_state == null)
			return null;
		return _state.getIdbs(_mappingDb.getBuiltInSchemas());
	}

	public List<RelationContext> getMappingRelations(){
		if (_state == null)
			return null;
		return _state.getMappingRelations();
	}

	public List<Rule> getTranslationRules() {
		List<Rule> ret = new ArrayList<Rule>();
		ret.addAll(getState().getSource2ProvRules());
		ret.addAll(getState().getProv2TargetRules(getMappingDb().getBuiltInSchemas()));
		return ret;
	}

	/**
	 * Given a relation context, see what mapping tables were
	 * mapped into it.  The basis of generating inverse rules.
	 * 
	 * @param rel
	 * @return
	 */
	public Map<Atom,Rule> getMappingAtomsFor(RelationContext rel) {
		Map<Atom,Rule> atoms = new HashMap<Atom,Rule>();
		for (Rule m : DeltaRuleGen.getProv2TargetRulesForProvQ(getState(), getMappingDb().getBuiltInSchemas())) {
			if ((m.getHead().getRelationContext().equals(rel))) {
				for (Atom b : m.getBody())
					if (!b.isNeg())
						atoms.put(b, m);
			}
		}

		for (Rule m : DeltaRuleGen.getSource2ProvRulesForProvQ(getState())) {
			if ((m.getHead().getRelationContext().equals(rel))) {
				for (Atom b : m.getBody())
					if (!b.isNeg())
						atoms.put(b, m);
			}
		}
		return atoms;
	}

	/** Evaluate a query and return results with iterator */
	public ResultSetIterator<Tuple> evalQueryRule(Rule r) throws Exception {
		if (!_mappingDb.isConnected()) {
			_mappingDb.connect();
		}
		return _mappingDb.evalQueryRule(r);
	}

	/** Evaluate a query and return results with iterator */
	public List<ResultSetIterator<Tuple>> evalRuleSet(List<Rule> r, String semiringName, boolean provenanceQuery) throws Exception {
		if (!_mappingDb.isConnected()) {
			_mappingDb.connect();
		}
		return _mappingDb.evalRuleSet(r, semiringName, provenanceQuery);
	}

	/** Evaluate an update (insertion or deletion) and return count of tuples updated */
	public int evalUpdateRule(Rule rule) throws Exception {
		if (!_mappingDb.isConnected()) {
			_mappingDb.connect();
		}
//		if (_updateDb != null && !_updateDb.isConnected()) {
//		_updateDb.connect();
//		}
		int trans = _mappingDb.evalUpdateRule(rule);
//		int store = _updateDb != null ? _updateDb.evalUpdateRule(rule) : trans;
//		assert(store == trans);
		return trans;
	}

	public void serialize(Document doc, Element el) {
		Element trans = DomUtils.addChild(doc, el, "mappings");
		_mappingDb.serialize(doc, trans);
//		if (_updateDb != null) {
//		Element update = DomUtils.addChild(doc, el, "updates");
//		_updateDb.serialize(doc, update);
//		}
	}

	public static BasicEngine deserialize(OrchestraSystem catalog, Map<String, Schema> builtInSchemas, Element el) throws Exception {
		Element trans = DomUtils.getChildElementByName(el, "mappings");
		if (trans == null) {
			trans = el;
		}
		IDb db = DbFactory.deserializeDb(catalog, builtInSchemas, trans);
		/*		Element update = DomUtils.getChildElementByName(el, "updates");
		IDb updateDb;
		if (update == null) {
			updateDb = null;
		} else {
			updateDb = deserializeDb(catalog, update);
		}
		 */
		try {
			return ExchangeEngineFactory.getEngine(catalog, trans.getAttribute("type"), db);
		} catch (UnsupportedTypeException ut) { 
			 throw new XMLParseException("Unknown database type: " + trans.getAttribute("type"), el);
		} catch (Exception e) {
			throw e;
		}
	}

	public void importUpdates(Peer specificPeer, String dir, ArrayList<String> succeeded,
			ArrayList<String> failed) throws IOException {
		if (specificPeer != null)
			importPeerRelationData(dir, specificPeer, succeeded, failed);
		else
			for (Peer p : _system.getPeers())
				importPeerRelationData(dir, p, succeeded, failed);
	}

	private List<String> getPeerFileNames(Peer p, Schema s, Relation r, String dir) {
		List<String> l = new ArrayList<String>();
		String prefix = dir + File.separator + 
		p.getId() + Config.getProperty("importNameSeparator") + 
		s.getSchemaId() + Config.getProperty("importNameSeparator") + 
		r.getName();
//		l.add(prefix + "_L_INS" + "." + Config.getImportExtension());
//		l.add(prefix + "_L_DEL" + "." + Config.getImportExtension());
		l.add(prefix + "." + Config.getImportExtension());
		return l;
	}

	private void importPeerRelationData(String dir, Peer p, ArrayList<String> succeeded,
			ArrayList<String> failed) throws IOException {
		for (Schema s : p.getSchemas()) {
			System.out.println("Importing " + s.getSchemaId());
			for (Relation r: s.getRelations()) {
				if (!r.isInternalRelation()) {
					List<String> strs = getPeerFileNames(p, s, r, dir);
					for(String str : strs){
						try {
							FileReader fr = new FileReader(str);
							fr.close();
							try {
								String name = str.substring(str.lastIndexOf(File.separator), str.lastIndexOf("."));
								_mappingDb.importRelation(new FileDb(
										dir, 
//										p.getId() + Config.getProperty("importNameSeparator") + 
//										s.getSchemaId() + Config.getProperty("importNameSeparator") + 
//										r.getName(),
										name,
										Config.getImportExtension(), 
										r, 
										Config.getProperty("importColumnSeparator").charAt(0)), 
										s.getRelation(r.getLocalName()),//r.getName() + "_L"),
										true /* By default, replace all */,
										r, s, p, _system.getRecDb(p.getId()));
//								_mappingDb.importRelation(new FileDb(null, str), 
//								s.getRelation(r.getLocalName()),//r.getName() + "_L"),
//								true /* By default, replace all */,
//								r, s, p, _system.getRecDb(p.getId()));

								succeeded.add(str);
							} catch (Exception e) {
								e.printStackTrace();
								throw new IOException("Unable to import data: " + e.getMessage());
							}
						} catch (FileNotFoundException fnf) {
							System.out.println("Warning -- did not find file: " + fnf.getMessage());
							failed.add(str);
						}
					}
				}
			}
		}
	}
}
