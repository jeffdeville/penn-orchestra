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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import edu.upenn.cis.orchestra.exchange.exceptions.MappingNotFoundException;
import edu.upenn.cis.orchestra.mappings.Rule;

/**
 * These are the translation state methods not used directly for delta rule generation.
 * @author John Frommeyer
 *
 */
public interface ITranslationState extends
		ITranslationRules {

	/**
	 * Sets the local 2 peer rules.
	 * 
	 * @param local2peerRules
	 */
	void setLocal2PeerRules(List<Rule> local2peerRules);

	/**
	 * Sets the original mappings.
	 * 
	 * @param mappings
	 */
	void setMappings(List<Mapping> mappings);

	/**
	 * Sets the mapping relations.
	 * 
	 * @param allMappingRels
	 */
	void setMappingRels(List<RelationContext> allMappingRels);

	/**
	 * Sets the real mapping relations.
	 * 
	 * @param realMappingRels
	 */
	void setRealMappingRels(List<RelationContext> realMappingRels);

	/**
	 * Returns the mapping relations.
	 * 
	 * @return the mapping relations
	 */
	List<RelationContext> getMappingRelations();

	/**
	 * Sets the source 2 target rules.
	 * 
	 * @param source2targetRules
	 */
	void setSource2TargetRules(List<Rule> source2targetRules);

	/**
	 * Sets the source to provenance rules.
	 * 
	 * @param source2provRules
	 */
	void setSource2ProvRules(List<Rule> source2provRules);

	/**
	 * Sets the provenance to target rules.
	 * 
	 * @param prov2targetMappings
	 */
	void setProv2TargetMappings(List<Mapping> prov2targetMappings);

	/**
	 * Returns the provenance relation associated with {@code mappingId}.
	 * 
	 * @param mappingId
	 * @return the provenance relation associated with {@code mappingId}
	 * @throws MappingNotFoundException 
	 */
	RelationContext getProvenanceRelationForMapping(String mappingId) throws MappingNotFoundException;

	/**
	 * Sets the outer union relations.
	 * 
	 * @param outerUnionRels
	 */
	void setOuterUnionRels(List<RelationContext> outerUnionRels);

	/**
	 * Returns the rej relations.
	 * 
	 * @param system
	 * @return the rej relations
	 */
	List<RelationContext> getRej(OrchestraSystem system);

	/**
	 * Returns the provenance to target rules.
	 * 
	 * @param builtInSchemas
	 * @return the provenance to target rules
	 */
	Collection<Rule> getProv2TargetRules(
			Map<String, Schema> builtInSchemas);

	/**
	 * Sets the simulated outer join relations.
	 * 
	 * @param outerJoinRels
	 */
	void setSimulatedOuterJoinRels(List<RelationContext> outerJoinRels);

	/**
	 * DOCUMENT ME
	 * 
	 * @param joinRels
	 */
	void setRealOuterJoinRels(List<RelationContext> joinRels);

	/**
	 * DOCUMENT ME
	 * 
	 * @param joinRels
	 */
	void setInnerJoinRels(List<RelationContext> joinRels);

}
