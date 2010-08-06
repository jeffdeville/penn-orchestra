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

import java.util.List;
import java.util.Map;

import org.w3c.dom.Document;

import edu.upenn.cis.orchestra.mappings.Rule;

/**
 * Only those {@code TranslationState} methods needed for delta rule generation.
 * @author John Frommeyer
 *
 */
public interface ITranslationRules {

	/**
	 * Returns the edbs.
	 * 
	 * @param builtInSchemas
	 * @return the edbs
	 */
	public List<RelationContext> getEdbs(Map<String, Schema> builtInSchemas);

	/**
	 * Returns the idbs.
	 * 
	 * @param builtInSchemas
	 * @return the idbs
	 */
	public List<RelationContext> getIdbs(Map<String, Schema> builtInSchemas);

	/**
	 * Returns the rejection relations.
	 * @param system 
	 * @return the rejection relations
	 * 
	 */
	List<RelationContext> getRej(OrchestraSystem system);
	
	/**
	 * Returns the outer union relations.
	 * 
	 * @return the outer union relations
	 */
	public List<RelationContext> getOuterUnionRelations();

	/**
	 * Returns the real mapping relations.
	 * 
	 * @return the real mapping relations
	 */
	public List<RelationContext> getRealMappingRelations();

	/**
	 * Returns the local to peer rules.
	 * 
	 * @return the local to peer rules
	 */
	public List<Rule> getLocal2PeerRules();

	/**
	 * Returns the source to provenance rules.
	 * 
	 * @return the source to provenance rules
	 */
	public List<Rule> getSource2ProvRules();

	/**
	 * Returns the source to target rules.
	 * 
	 * @return the source to target rules
	 */
	public List<Rule> getSource2TargetRules();

	/**
	 * Return the original mappings.
	 * 
	 * @return the original mappings
	 */
	public List<Mapping> getOriginalMappings();

	/**
	 * Returns the provenance to target mappings.
	 * 
	 * @return the provenance to target mappings
	 */
	public List<Mapping> getProv2TargetMappings();
	
	/**
	 * Returns an XML {@code Document} which represents this {@code ITranslationRules}.
	 * @param system
	 * @param builtInSchemas
	 * 
	 * @return an XML {@code Document} which represents this {@code ITranslationRules}
	 */
	Document serialize(OrchestraSystem system, Map<String, Schema> builtInSchemas);

	/**
	 * Returns the inner join relations.
	 * 
	 * @return the inner join relations
	 */
	List<RelationContext> getInnerJoinRelations();

	/**
	 * Returns the real outer join relations.
	 * 
	 * @return  the real outer join relations
	 */
	List<RelationContext> getRealOuterJoinRelations();

	/**
	 * Returns the simulated outer join relations.
	 * 
	 * @return the simulated outer join relations
	 */
	List<RelationContext> getSimulatedOuterJoinRelations();

	

}
