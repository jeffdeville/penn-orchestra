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
package edu.upenn.cis.orchestra.datamodel;

import static edu.upenn.cis.orchestra.OrchestraUtil.newArrayList;
import static edu.upenn.cis.orchestra.util.DeserializationUtils.deserializeRelationContexts;
import static edu.upenn.cis.orchestra.util.DeserializationUtils.deserializeVerboseMappings;
import static edu.upenn.cis.orchestra.util.DeserializationUtils.deserializeVerboseRules;
import static edu.upenn.cis.orchestra.util.DomUtils.getChildElementByName;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.deltaRules.DeltaRuleGen;
import edu.upenn.cis.orchestra.exchange.exceptions.MappingNotFoundException;
import edu.upenn.cis.orchestra.mappings.MappingsIOMgt;
import edu.upenn.cis.orchestra.mappings.MappingsInversionMgt;
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.provenance.ProvenanceRelation;
import edu.upenn.cis.orchestra.provenance.ProvenanceRelation.ProvRelType;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.XMLParseException;

public class TranslationState implements ITranslationState, Serializable {
//from BasicEngine
	
	private static final long serialVersionUID = 42;

	private final List<RelationContext> _rels;
	/**
	 * List of the edb/idb relations in those rules
	 */
	private List<RelationContext> _edbs;
	private List<RelationContext> _idbs;
	private List<RelationContext> _rej;
	
	
	private List<RelationContext> _mappingRels;
	private List<RelationContext> _realMappingRels;
	
	private List<RelationContext> _simulatedOuterJoinRels = new ArrayList<RelationContext>();
	private List<RelationContext> _realOuterJoinRels = new ArrayList<RelationContext>();
	private List<RelationContext> _outerUnionRels = new ArrayList<RelationContext>();
	private List<RelationContext> _innerJoinRels = newArrayList();

//	Original mappings
	private List<Mapping> _originalMappings;
	
//	OuterUnionMapping structures ... where are they used?
//	public List<OuterUnionMapping> _ouMappings;
	
//	In/out rules. For ins we need to conjoin with not R_r
//	For del it is not necessary
//	public List<Rule> _baseInsRules;


	/**
	 * List of input rules
	 */

	private List<Rule> _source2TargetRules;
	private List<Rule> _local2PeerRules;
	private List<Mapping> _prov2targetMappings;
	private List<Rule> _source2provRules;
	
	public TranslationState(List<Mapping> mappings, List<RelationContext> rels) {
		_originalMappings = Collections.unmodifiableList(mappings);
				
		_rels = Collections.unmodifiableList(rels);
	}
	
	/**
	 * Populates the state.
	 * 
	 * @param edbs
	 * @param idbs
	 * @param rej
	 * @param localToPeerRules
	 * @param originalMappings
	 * @param realOuterJoinRels
	 * @param innerJoinRels
	 * @param simulatedOuterJoinRels
	 * @param outerUnionRelations
	 * @param provToTargetMappings
	 * @param realMappingRelations
	 * @param sourceToProvRules
	 * @param sourceToTargetRules
	 * @param rels
	 */
	private TranslationState(
			 List<RelationContext> edbs,
			 List<RelationContext> idbs,
			 List<RelationContext> rej,
			 List<Rule> localToPeerRules,
			 List<Mapping> originalMappings,
			 List<RelationContext> realOuterJoinRels,
			 List<RelationContext> innerJoinRels,
			 List<RelationContext> simulatedOuterJoinRels,
			 List<RelationContext> outerUnionRelations,
			 List<Mapping> provToTargetMappings,
			 List<RelationContext> realMappingRelations,
			 List<Rule> sourceToProvRules, List<Rule> sourceToTargetRules, List<RelationContext> rels) {
		_edbs = edbs;
		_idbs = idbs;
		_rej = rej;
		_local2PeerRules = localToPeerRules;
		_originalMappings = originalMappings;
		_realOuterJoinRels = realOuterJoinRels;
		_simulatedOuterJoinRels = simulatedOuterJoinRels;
		_innerJoinRels = innerJoinRels;
		_outerUnionRels = outerUnionRelations;
		_prov2targetMappings = provToTargetMappings;
		_realMappingRels = realMappingRelations;
		_source2provRules = sourceToProvRules;
		_source2TargetRules = sourceToTargetRules;
		_rels = rels;
	}
	
	public void setMappings(List<Mapping> mappings){
		_originalMappings = Collections.unmodifiableList(mappings);
	}
	
	/**
	 * Get the EDB relations
	 * @return EDB relations
	 */
	public synchronized List<RelationContext> getEdbs(Map<String, Schema> builtInSchemas){
		if(_edbs != null)
			return _edbs;
		return getEdbsIdbs(builtInSchemas, true);
	}

	/**
	 * Get the rejection relations
	 * @return rejection relations
	 */
	public synchronized List<RelationContext> getRej(OrchestraSystem system){
		if(_rej != null)
			return _rej;
		return getRejTables(system);
	}
	
	/**
	 * Get the IDB relations
	 * @return IDB relations
	 */
	public synchronized List<RelationContext> getIdbs(Map<String, Schema> builtInSchemas){
		if(_idbs != null)
			return _idbs;
		return getEdbsIdbs(builtInSchemas, false);
	}
	
	private synchronized List<RelationContext> getEdbsIdbs(Map<String, Schema> builtInSchemas, boolean returnEdbs) 
	{
//		It would be a lot cleaner if this was:
//		idbs: all real relations
//		edbs: all _L, _R
		List<Rule> prov2targetRules = MappingsIOMgt.inOutTranslationR(getProv2TargetRules(builtInSchemas), true);

		if(getSource2ProvRules() != null &
			prov2targetRules != null && 
			getLocal2PeerRules() != null && 
			_rels != null){
			
			List<RelationContext> edbsTemp = new ArrayList<RelationContext>();
			List<RelationContext> idbsTemp = new ArrayList<RelationContext>();
			
			List<Mapping> mr = new ArrayList<Mapping>();
			mr.addAll(getSource2ProvRules());
			mr.addAll(prov2targetRules);
			mr.addAll(getLocal2PeerRules());
			extractEdbsIdbs(mr, _rels, edbsTemp, idbsTemp);
			_edbs = Collections.unmodifiableList(edbsTemp);
			_idbs = Collections.unmodifiableList(idbsTemp);
			if(returnEdbs) {
				return _edbs;
			} else {
				return _idbs;
			}
		}
		return null;
	}

	private synchronized List<RelationContext> getRejTables(OrchestraSystem system){
		List<RelationContext> ret = new ArrayList<RelationContext>();
		for(Peer p : system.getPeers()){
			for(Schema s : p.getSchemas()){
				for(Relation r : s.getRelations()){
					try{
						Relation rr = s.getRelation(r.getLocalRejDbName());
						ret.add(new RelationContext(rr, s, p, false));
					}catch(Exception e){
					}
				}
			}
		}
		_rej = Collections.unmodifiableList(ret);
		return _rej;
	}
	
	/**
	 * Get the list of relations for storing mappings
	 */
	public synchronized List<RelationContext> getMappingRelations() {
		return _mappingRels;
	}

	public synchronized List<RelationContext> getRealMappingRelations() {
		return _realMappingRels;
	}
	
	public synchronized List<RelationContext> getSimulatedOuterJoinRelations() {
		return _simulatedOuterJoinRels;
	}
	
	public synchronized List<RelationContext> getRealOuterJoinRelations() {
		return _realOuterJoinRels;
	}

	public synchronized List<RelationContext> getOuterUnionRelations() {
		return _outerUnionRels;
	}
	
	public synchronized void setMappingRels(List<RelationContext> mappingRels){
		_mappingRels = Collections.unmodifiableList(mappingRels);
	}

	public synchronized void setRealMappingRels(List<RelationContext> realMappingRels){
		_realMappingRels = Collections.unmodifiableList(realMappingRels);
	}

	public synchronized void setSimulatedOuterJoinRels(List<RelationContext> outerJoinRels){
		_simulatedOuterJoinRels = Collections.unmodifiableList(outerJoinRels);
	}
	
	public synchronized void setRealOuterJoinRels(List<RelationContext> outerJoinRels){
		_realOuterJoinRels = Collections.unmodifiableList(outerJoinRels);
	}
	
	public synchronized void setOuterUnionRels(List<RelationContext> outerUnionRels){
		_outerUnionRels = Collections.unmodifiableList(outerUnionRels);
	}

	public synchronized List<Rule> getSource2TargetRules() {
		return Collections.unmodifiableList(DeltaRuleGen.subtractFakeRules(_source2TargetRules));
	}

	public synchronized void setSource2TargetRules(List<Rule> source2TargetRules) {
		_source2TargetRules = Collections.unmodifiableList(source2TargetRules);
	}
	
	public synchronized List<Mapping> getOriginalMappings() {
		return _originalMappings;
	}
	
	public synchronized List<Mapping> getProv2TargetMappings(){
		return Collections.unmodifiableList(DeltaRuleGen.subtractFakeMappings(_prov2targetMappings));
	}	
	
	public synchronized List<Rule> getProv2TargetRules(Map<String, Schema> builtInSchemas){
		return Collections.unmodifiableList(DeltaRuleGen.subtractFakeRules(MappingsInversionMgt.splitMappingsHeads(_prov2targetMappings, builtInSchemas)));	
	}
	
	public synchronized void setProv2TargetMappings (List<Mapping> p2t) {
		_prov2targetMappings = Collections.unmodifiableList(p2t);
	}
	
	public synchronized List<Rule> getSource2ProvRules(){
		return Collections.unmodifiableList(DeltaRuleGen.subtractFakeRules(_source2provRules));
	}
	
	public synchronized void setSource2ProvRules(List<Rule> rules){
		_source2provRules = Collections.unmodifiableList(rules);
	}
	
	public synchronized void setLocal2PeerRules(List<Rule> local){
		_local2PeerRules = Collections.unmodifiableList(local);
	}

	public synchronized List<Rule> getLocal2PeerRules(){
		return _local2PeerRules;
	}
	
	/**
	 * From the set of rules extract the list of EDB and IDB relations
	 * Add the result to the _edb and _idb attributes 
	 * @param rules Rules from which to extract edb/idb relations
	 */
	private synchronized void extractEdbsIdbs (List<Mapping> rules, List<RelationContext> rels,
			List<RelationContext> edbs, List<RelationContext> idbs)
	{
		// In this map a relation viewed in at least one rule head has value false, true otherwise
		Map<AbstractRelation, Boolean> edbsMap = new HashMap<AbstractRelation, Boolean> ();
		Map<AbstractRelation, RelationContext> contextMap = new HashMap<AbstractRelation, RelationContext> ();
//		edbs.clear();
//		idbs.clear();
		
		for (Mapping r : rules)
		{
			for(Atom ma : r.getMappingHead()){
				if(!ma.isSkolem() && !_mappingRels.contains(ma.getRelationContext())){
					edbsMap.put(ma.getRelation(), false);
					contextMap.put(ma.getRelation(), ma.getRelationContext());
				}
			}
			
			for (Atom atom : r.getBody()){
				if(!atom.isSkolem() && !_mappingRels.contains(atom.getRelationContext())){
					if (!edbsMap.containsKey(atom.getRelation()))
					{
						edbsMap.put(atom.getRelation(), true);
						contextMap.put(atom.getRelation(), atom.getRelationContext());
					}
				}
			}
		}
	
		for(RelationContext rc : rels){
			AbstractRelation r = rc.getRelation();
			if(!edbsMap.containsKey(r)){
				edbsMap.put(r, true);
				contextMap.put(r, rc);
			}
		}
	
		// Extract the list of relations for which the map says it's an EDB relation
		for (Map.Entry<AbstractRelation, Boolean> entry : edbsMap.entrySet())
			if (entry.getValue().booleanValue()){
				if(!entry.getKey().getName().endsWith(Relation.REJECT))
					edbs.add (contextMap.get(entry.getKey()));
			}else{
				idbs.add (contextMap.get(entry.getKey()));
			}
	}
	

	
	public RelationContext getProvenanceRelationForMapping(String mappingId) throws MappingNotFoundException
	{
		List<RelationContext> provRels = getMappingRelations();

		for(int j = 0; j < provRels.size(); j++){
			ProvenanceRelation provRel = (ProvenanceRelation)provRels.get(j).getRelation();
			if(ProvRelType.SINGLE.equals(provRel.getType())){
				Mapping m = provRel.getMappings().get(0);
				if(mappingId.equals(m.getId())){
					return provRels.get(j);
				}
			}
		}
		throw new MappingNotFoundException(mappingId);
	}
	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.upenn.cis.orchestra.datamodel.ITranslationRules#serialize(OrchestraSystem, Map<String, Schema>)
	 */
	@Override
	public Document serialize(OrchestraSystem system, Map<String, Schema> builtInSchemas) {
		Document doc = DomUtils.createDocument();
		Element state = doc.createElement("translationState");
		state.appendChild(Mapping.serializeVerbose(doc,getOriginalMappings(),
				"originalMappings"));
		state.appendChild(Mapping.serializeVerbose(doc,
			getSource2TargetRules(), "source2TargetRules"));
		state.appendChild(Mapping.serializeVerbose(doc, getLocal2PeerRules(),
				"local2PeerRules"));
		state.appendChild(Mapping.serializeVerbose(doc,
				getProv2TargetMappings(), "prov2TargetMappings"));
		state.appendChild(Mapping.serializeVerbose(doc, getSource2ProvRules(),
				"source2ProvRules"));
		state.appendChild(RelationContext.serialize(doc, _rels, "rels"));
		state.appendChild(RelationContext.serialize(doc,
			getRealMappingRelations(), "realMappingRelations"));
		state.appendChild(RelationContext.serialize(doc, getEdbs(builtInSchemas), "edbs"));
		state.appendChild(RelationContext.serialize(doc, getIdbs(builtInSchemas), "idbs"));
		state.appendChild(RelationContext.serialize(doc, getRej(system), "rej"));
		state.appendChild(RelationContext.serialize(doc,
			getRealOuterJoinRelations(), "realOuterJoinRelations"));
		state.appendChild(RelationContext.serialize(doc,
				getSimulatedOuterJoinRelations(), "simulatedOuterJoinRelations"));
		state.appendChild(RelationContext.serialize(doc,
				getInnerJoinRelations(), "innerJoinRelations"));
		state.appendChild(RelationContext.serialize(doc,
			getOuterUnionRelations(), "outerUnionRelations"));
		doc.appendChild(state);
		return doc;
	}
	
	/**
	 * Returns a new {@code ITranslationState}, populated from {@code
	 * translationStateDoc} .
	 * 
	 * @param translationStateDoc a {@code Document} produced by {@code
	 *            serialize()}
	 * @param system 
	 * @return a new {@code ITranslationState}, populated from {@code
	 *         translationStateDoc}
	 * @throws XMLParseException
	 */
	public static ITranslationState deserialize(Document translationStateDoc, OrchestraSystem system)
			throws XMLParseException {
		Element root = translationStateDoc.getDocumentElement();
		List<Mapping> originalMappings = deserializeVerboseMappings(getChildElementByName(
				root, "originalMappings"), system);
		List<Rule> sourceToTargetRules = deserializeVerboseRules(getChildElementByName(
				root, "source2TargetRules"), system);
		List<Rule> localToPeerRules = deserializeVerboseRules(getChildElementByName(
				root, "local2PeerRules"), system);
		List<Mapping> provToTargetMappings = deserializeVerboseMappings(getChildElementByName(
				root, "prov2TargetMappings"), system);
		List<Rule> sourceToProvRules = deserializeVerboseRules(getChildElementByName(
				root, "source2ProvRules"), system);
		List<RelationContext> realMappingRelations = deserializeRelationContexts(getChildElementByName(
				root, "realMappingRelations"), system);
		List<RelationContext> edbs = deserializeRelationContexts(getChildElementByName(
				root, "edbs"), system);
		List<RelationContext> idbs = deserializeRelationContexts(getChildElementByName(
				root, "idbs"), system);
		List<RelationContext> rej = deserializeRelationContexts(getChildElementByName(
				root, "rej"), system);
		List<RelationContext> realOuterJoinRelations = deserializeRelationContexts(getChildElementByName(
				root, "realOuterJoinRelations"), system);
		List<RelationContext> simulatedOuterJoinRelations = deserializeRelationContexts(getChildElementByName(
				root, "simulatedOuterJoinRelations"), system);
		List<RelationContext> innerJoinRelations = deserializeRelationContexts(getChildElementByName(
				root, "innerJoinRelations"), system);
		List<RelationContext> outerUnionRelations = deserializeRelationContexts(getChildElementByName(
				root, "outerUnionRelations"), system);
		List<RelationContext> rels = deserializeRelationContexts(getChildElementByName(root, "rels"), system);
		ITranslationState newTranslationStates = new TranslationState(edbs,
				idbs, rej, localToPeerRules, originalMappings,
				realOuterJoinRelations, innerJoinRelations,
				simulatedOuterJoinRelations, outerUnionRelations,
				provToTargetMappings, realMappingRelations, sourceToProvRules, sourceToTargetRules, rels);
		return newTranslationStates;
	}

	/**
	 * @param _innerJoinRels the _innerJoinRels to set
	 */
	public void setInnerJoinRels(List<RelationContext> innerJoinRels) {
		this._innerJoinRels = Collections.unmodifiableList(innerJoinRels);
	}

	/**
	 * @return the _innerJoinRels
	 */
	public List<RelationContext> getInnerJoinRelations() {
		return _innerJoinRels;
	}
	
}
