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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.w3c.dom.Document;

import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.RelationContext;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.exceptions.IncompatibleTypesException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnsupportedTypeException;
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.provenance.ProvenanceRelation;
import edu.upenn.cis.orchestra.provenance.ProvenanceRelation.ProvRelType;
import edu.upenn.cis.orchestra.util.XMLParseException;

/**
 * A {@code ITranslationRuleGen} using outer joins to store provenance
 * relations.
 * 
 * @author John Frommeyer
 * 
 */
public class OuterJoinTranslationRuleGen extends TranslationRuleGen {

	/**
	 * Returns a new {@code OuterJoinTranslationRuleGen}.
	 * 
	 * @param systemMappingsDoc
	 * @param userRelationsDoc
	 * @param builtInSchemasDoc
	 * @param system
	 * @throws XMLParseException
	 * 
	 * @see {@link edu.upenn.cis.orchestra.exchange.TranslationRuleGen#newInstance(Document, Document, Document, OrchestraSystem)
	 *      newInstance}
	 */
	OuterJoinTranslationRuleGen(Document systemMappingsDoc,
			Document userRelationsDoc, Document builtInSchemasDoc,
			OrchestraSystem system) throws XMLParseException {
		super(systemMappingsDoc, userRelationsDoc, builtInSchemasDoc, system);
	}

	/**
	 * Returns a new {@code OuterJoinTranslationRuleGen}.
	 * 
	 * @param systemMappings
	 * @param userRelations
	 * @param builtInSchemas
	 * @param isBidirectional
	 * @see {@link edu.upenn.cis.orchestra.exchange.TranslationRuleGen#newInstance(List, List, Map, boolean)
	 *      newInstance}
	 */
	OuterJoinTranslationRuleGen(
			List<Mapping> systemMappings,
			List<RelationContext> userRelations,
			@SuppressWarnings("hiding") final Map<String, Schema> builtInSchemas,
			boolean isBidirectional) {
		super(systemMappings, userRelations, builtInSchemas, isBidirectional);
	}

	/**
	 * {@inheritDoc}
	 * 
	 */
	@Override
	protected void computeProvRules(List<Rule> sourceToProvRules,
			List<Mapping> provToTargetMappings)
			throws IncompatibleTypesException, UnsupportedTypeException {
		// Create join relations but keep originals as well
		
		List<RelationContext> provRels = state.getMappingRelations();
		List<RelationContext> newRels = new ArrayList<RelationContext>();
		List<Rule> ojMappings = new ArrayList<Rule>();
		int k = 3; // join every 2 mappings
		int j;
		for (j = 0; j + k - 1 < provRels.size(); j = j + k) {
			List<RelationContext> rels = new ArrayList<RelationContext>();
			List<ProvenanceRelation> prels = new ArrayList<ProvenanceRelation>();
			boolean allRealMappings = true;
			for (int l = j; l < j + k; l++) {
				rels.add(provRels.get(l));
				prels.add((ProvenanceRelation) provRels.get(l).getRelation());
				if (prels.get(prels.size() - 1).getMappings().get(0)
						.isFakeMapping()) {
					allRealMappings = false;
				}
			}

			if (allRealMappings) {
				ProvenanceRelation newRel = ProvenanceRelation
						.createJoinProvRelSchema(prels,
								ProvRelType.SIMULATED_LEFT_OUTER_JOIN);
				RelationContext newRelCtx = new RelationContext(newRel,
						provRels.get(j).getSchema(), provRels.get(j).getPeer(),
						true);
				newRels.add(newRelCtx);
				ojMappings.addAll(newRel.outerJoinMappingsForMaintenance(builtInSchemas));
			}
			for (int l = 0; l < k; l++) {
				prels.get(l).getSplitMappings(rels.get(l).getPeer(),
						rels.get(l).getSchema(), sourceToProvRules,
						provToTargetMappings, builtInSchemas);
			}
		}

		if (provRels.size() % k != 0) {
			for (int l = j; l < provRels.size(); l++) {
				RelationContext relCtx = provRels.get(l);
				ProvenanceRelation rel = (ProvenanceRelation) relCtx
						.getRelation();

				rel
						.getSplitMappings(relCtx.getPeer(), relCtx.getSchema(),
								sourceToProvRules, provToTargetMappings,
								builtInSchemas);
			}
		}
		state.setSimulatedOuterJoinRels(newRels);

	}
}
