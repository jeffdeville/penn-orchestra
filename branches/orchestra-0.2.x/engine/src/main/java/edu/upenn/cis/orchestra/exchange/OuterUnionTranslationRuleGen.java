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
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.provenance.ProvenanceRelation;
import edu.upenn.cis.orchestra.provenance.ProvenanceRelation.ProvRelType;
import edu.upenn.cis.orchestra.util.XMLParseException;

/**
 * A {@code ITranslationRuleGen} using outer unions to store provenance
 * relations.
 * 
 * @author John Frommeyer
 * 
 */
public class OuterUnionTranslationRuleGen extends TranslationRuleGen {
	/**
	 * Returns a new {@code OuterUnionTranslationRuleGen}.
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
	OuterUnionTranslationRuleGen(Document systemMappingsDoc,
			Document userRelationsDoc, Document builtInSchemasDoc,
			OrchestraSystem system) throws XMLParseException {
		super(systemMappingsDoc, userRelationsDoc, builtInSchemasDoc, system);
	}

	/**
	 * Returns a new {@code OuterUnionTranslationRuleGen}.
	 * 
	 * @param systemMappings
	 * @param userRelations
	 * @param builtInSchemas
	 * @param isBidirectional
	 * @see {@link edu.upenn.cis.orchestra.exchange.TranslationRuleGen#newInstance(List, List, Map, boolean)
	 *      newInstance}
	 */
	OuterUnionTranslationRuleGen(List<Mapping> systemMappings,
			List<RelationContext> userRelations,
			@SuppressWarnings("hiding") final Map<String, Schema> builtInSchemas, boolean isBidirectional) {
		super(systemMappings, userRelations, builtInSchemas, isBidirectional);
	}

	/**
	 * {@inheritDoc} 
	 */
	@Override
	protected void computeProvRules(List<Rule> sourceToProvRules,
			List<Mapping> provToTargetMappings) throws Exception {
		List<RelationContext> provRels = state.getMappingRelations();
		List<RelationContext> newRels = new ArrayList<RelationContext>();
		int k = 2; // union every 2 mappings
		int j;
		for (j = 0; j + k - 1 < provRels.size(); j = j + k) {
			List<RelationContext> rels = new ArrayList<RelationContext>();
			List<ProvenanceRelation> prels = new ArrayList<ProvenanceRelation>();
			for (int l = j; l < j + k; l++) {
				rels.add(provRels.get(l));
				prels.add((ProvenanceRelation) provRels.get(l).getRelation());
			}
			ProvenanceRelation newRel = ProvenanceRelation
					.createUnionProvRelSchema(prels, ProvRelType.OUTER_UNION);
			RelationContext newRelCtx = new RelationContext(newRel, rels.get(0)
					.getSchema(), rels.get(0).getPeer(), true);
			newRels.add(newRelCtx);
			newRel.getSplitMappings(newRelCtx.getPeer(), newRelCtx.getSchema(),
					sourceToProvRules, provToTargetMappings, builtInSchemas);
		}
		if (provRels.size() % k != 0) {
			for (int l = j; l < provRels.size(); l++) {
				RelationContext relCtx = provRels.get(l);
				ProvenanceRelation rel = (ProvenanceRelation) relCtx
						.getRelation();
				newRels.add(relCtx);
				rel.getSplitMappings(relCtx.getPeer(), relCtx.getSchema(),
						sourceToProvRules, provToTargetMappings, builtInSchemas);
			}
		}
		state.setMappingRels(newRels);		
	}
}
