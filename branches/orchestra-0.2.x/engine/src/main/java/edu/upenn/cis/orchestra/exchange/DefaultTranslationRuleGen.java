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

import java.util.List;
import java.util.Map;

import org.w3c.dom.Document;

import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.RelationContext;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.exceptions.IncompatibleTypesException;
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.provenance.ProvenanceRelation;
import edu.upenn.cis.orchestra.util.XMLParseException;

/**
 * A {@code ITranslationRuleGen} not using outer unions or outer joins to store
 * provenance relations.
 * 
 * @author John Frommeyer
 * 
 */
public class DefaultTranslationRuleGen extends TranslationRuleGen {

	/**
	 * Returns a new {@code DefaultTranslationRuleGen}.
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
	DefaultTranslationRuleGen(Document systemMappingsDoc,
			Document userRelationsDoc, Document builtInSchemasDoc,
			OrchestraSystem system) throws XMLParseException {
		super(systemMappingsDoc, userRelationsDoc, builtInSchemasDoc, system);
	}

	/**
	 * Returns a new {@code DefaultTranslationRuleGen}.
	 * 
	 * @param systemMappings
	 * @param userRelations
	 * @param builtInSchemas
	 * @param isBidirectional
	 * @see {@link edu.upenn.cis.orchestra.exchange.TranslationRuleGen#newInstance(List, List, Map, boolean)
	 *      newInstance}
	 */
	DefaultTranslationRuleGen(
			List<Mapping> systemMappings,
			List<RelationContext> userRelations,
			@SuppressWarnings("hiding") final Map<String, Schema> builtInSchemas,
			boolean isBidirectional) {
		super(systemMappings, userRelations, builtInSchemas, isBidirectional);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void computeProvRules(List<Rule> sourceToProvRules,
			List<Mapping> provToTargetMappings)
			throws IncompatibleTypesException {
		for (RelationContext newRelCtx : state.getMappingRelations()) {
			ProvenanceRelation newRel = (ProvenanceRelation) newRelCtx
					.getRelation();

			newRel.getSplitMappings(newRelCtx.getPeer(), newRelCtx.getSchema(),
					sourceToProvRules, provToTargetMappings, builtInSchemas);
		}

	}

}
