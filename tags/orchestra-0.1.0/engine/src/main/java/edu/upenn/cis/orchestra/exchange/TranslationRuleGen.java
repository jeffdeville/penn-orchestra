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

import static edu.upenn.cis.orchestra.util.DeserializationUtils.deserializeRelationContexts;
import static edu.upenn.cis.orchestra.util.DeserializationUtils.deserializeVerboseMappings;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import org.w3c.dom.Document;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.Debug;
import edu.upenn.cis.orchestra.OrchestraUtil;
import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datalog.atom.AtomArgument;
import edu.upenn.cis.orchestra.datalog.atom.AtomVariable;
import edu.upenn.cis.orchestra.datamodel.ITranslationState;
import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationContext;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.TranslationState;
import edu.upenn.cis.orchestra.datamodel.exceptions.IncompatibleKeysException;
import edu.upenn.cis.orchestra.datamodel.exceptions.IncompatibleTypesException;
import edu.upenn.cis.orchestra.exceptions.ConfigurationException;
import edu.upenn.cis.orchestra.mappings.MappingsCompositionMgt;
import edu.upenn.cis.orchestra.mappings.MappingsIOMgt;
import edu.upenn.cis.orchestra.mappings.MappingsInversionMgt;
import edu.upenn.cis.orchestra.mappings.MappingsTranslationMgt;
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.provenance.ProvenanceRelation;
import edu.upenn.cis.orchestra.util.XMLParseException;

/**
 * This class computes the initial translation rules for update exchange which
 * can then be used to compute the delta rules for incremental updates.
 * 
 * @author gkarvoun, John Frommeyer
 * 
 */
public abstract class TranslationRuleGen implements ITranslationRuleGen {

	private final List<Mapping> systemMappings;
	private final List<RelationContext> userRelations;
	
	/** The built-in function schema map for the system under consideration. */
	protected final Map<String, Schema> builtInSchemas;
	
	/** The translation rules which are being generated. */
	protected ITranslationState state;
	
	/** Indicates that the system under consideration contains bidirectional mappings. */
	protected final boolean bidirectional;

	/**
	 * Returns this rule generator.
	 * 
	 * @param systemMappings the mappings from an Orchestra schema file. For
	 *            example, from {@code
	 *            OrchestraSystem.getAllSystemMappings(boolean)}
	 * @param userRelations the relation contexts from an Orchestra schema file.
	 *            For example, from {@code
	 *            OrchestraSystem.getAllUserRelations()}
	 * @param builtInSchemas
	 * @param isBidirectional
	 */
	TranslationRuleGen(
			@SuppressWarnings("hiding") final List<Mapping> systemMappings,
			@SuppressWarnings("hiding") final List<RelationContext> userRelations,
			@SuppressWarnings("hiding") final Map<String, Schema> builtInSchemas,
			boolean isBidirectional) {
		this.systemMappings = systemMappings;
		this.userRelations = userRelations;
		this.builtInSchemas = builtInSchemas;
		bidirectional = isBidirectional;
	}

	/**
	 * Returns this rule generator.
	 * 
	 * @param systemMappingsDoc
	 * @param userRelationsDoc
	 * @param builtInSchemasDoc
	 * @param system
	 * @throws XMLParseException
	 */
	TranslationRuleGen(Document systemMappingsDoc, Document userRelationsDoc,
			Document builtInSchemasDoc, OrchestraSystem system)
			throws XMLParseException {
		this.systemMappings = deserializeVerboseMappings(systemMappingsDoc
				.getDocumentElement(), system);
		this.userRelations = deserializeRelationContexts(userRelationsDoc
				.getDocumentElement(), system);
		this.builtInSchemas = OrchestraSystem
				.deserializeBuiltInFunctions(builtInSchemasDoc);
		bidirectional = system.isBidirectional();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.upenn.cis.orchestra.exchange.ITranslationRuleGen#computeTranslationRules
	 * ()
	 */

	public List<Rule> computeTranslationRules() throws Exception {

		try {
			List<Mapping> mappings = OrchestraUtil.newArrayList(systemMappings);
			for (Mapping m : mappings) {
				m.renameExistentialVars();
			}
			List<RelationContext> relations = OrchestraUtil
					.newArrayList(userRelations);

			if (Config.getEdbbits())
				MappingsTranslationMgt.addEdbBitsToMappings(mappings);

			state = new TranslationState(mappings, relations);

			Debug.println("Mappings: " + mappings.size());

			Calendar before = Calendar.getInstance();

			List<Rule> source2targetRules;
			List<Rule> local2peerRules = MappingsIOMgt.inOutTranslationL(
					builtInSchemas, relations);
			state.setLocal2PeerRules(local2peerRules);
			mappings.addAll(state.getLocal2PeerRules());

			// Make variables in each mapping different
			int i = 0;
			for (Mapping m : mappings) {
				m.renameVariables("_M" + i);
				i++;
			}
			state.setMappings(mappings);
			List<Mapping> skolMappings;
			List<RelationContext> allMappingRels;
			List<RelationContext> realMappingRels = new ArrayList<RelationContext>();

			skolMappings = MappingsInversionMgt.skolemizeMappings(mappings);
			MappingsCompositionMgt
					.composeMappings(skolMappings, builtInSchemas);

			allMappingRels = computeProvenanceRelations(skolMappings);

			// Need to change this - put it inside Provenance Relations
			// mappings = expandBidirectionalMappings(mappings);

			state.setMappingRels(allMappingRels);
			for (RelationContext relctx : allMappingRels) {
				ProvenanceRelation prvrel = (ProvenanceRelation) relctx
						.getRelation();

				if (!prvrel.getType().equals(
						ProvenanceRelation.ProvRelType.SINGLE)
						|| !prvrel.getMappings().get(0).isFakeMapping()) {
					realMappingRels.add(relctx);
				}
			}
			state.setRealMappingRels(realMappingRels);

			List<Rule> source2provRules = new ArrayList<Rule>();
			List<Mapping> prov2targetMappings = new ArrayList<Mapping>();

			computeProvRules(source2provRules, prov2targetMappings);

			source2targetRules = MappingsInversionMgt.splitMappingsHeads(
					skolMappings, builtInSchemas);
			source2targetRules = MappingsIOMgt.inOutTranslationR(
					source2targetRules, true);

			state.setSource2TargetRules(source2targetRules);
			state.setSource2ProvRules(source2provRules);
			state.setProv2TargetMappings(prov2targetMappings);

			Calendar after = Calendar.getInstance();
			long time = after.getTimeInMillis() - before.getTimeInMillis();
			Debug.println("TOTAL RULE MANIPULATION TIME: " + time + "msec");
			return source2targetRules;
		} catch (Exception e) {
			e.printStackTrace();
			throw (e);
		}
	}

	/**
	 * Computes the rules and mappings with Provenance endpoints.
	 * 
	 * @param sourceToProvRules
	 * @param provToTargetMappings
	 * @throws IncompatibleTypesException
	 * @throws IncompatibleTypesException
	 */
	protected abstract void computeProvRules(List<Rule> sourceToProvRules,
			List<Mapping> provToTargetMappings) throws Exception;

	/**
	 * Returns a list containing {@code mapping} and its inverse if {@code
	 * mapping } is bidirectional.
	 * 
	 * @param mapping
	 * @return a list containing {@code mapping} and its inverse if {@code
	 *         mapping } is bidirectional
	 */
	public static List<Mapping> expandBidirectionalMapping(Mapping mapping) {
		List<Mapping> newMappings = new ArrayList<Mapping>();

		newMappings.add(mapping);
		if (mapping.isBidirectional()) {
			Mapping back = new Mapping(mapping.getId() + "-INV", mapping
					.getDescription()
					+ " bidirectional inverse", mapping.isMaterialized(),
					false, mapping.getTrustRank(), mapping.copyBody(), mapping
							.copyMappingHead());
			back = MappingsInversionMgt.skolemizeMapping(back);
			back.setProvenanceRelation(mapping.getProvenanceRelation());
			back.setDerivedFrom(mapping.getId());
			newMappings.add(back);
		}
		return newMappings;
	}

	static List<RelationContext> computeProvenanceRelations(
			List<Mapping> mappings) throws IncompatibleTypesException,
			IncompatibleKeysException {

		List<RelationContext> mappingRels = new ArrayList<RelationContext>();

		for (int i = 0; i < mappings.size(); i++) {
			final Mapping mapping = mappings.get(i);

			final List<AtomVariable> allVars = mapping.getAllBodyVariables();
			final List<AtomArgument> allVarsCast = new ArrayList<AtomArgument>(
					allVars.size());
			for (final AtomVariable var : allVars)
				allVarsCast.add(var);

			if (mapping instanceof Rule) {
				mapping.setDescription("+");
			} else {
				mapping.setDescription(mapping.getId());
			}

			Relation rel = null;
			try {
				rel = CreateProvenanceStorage.computeProvenanceRelation(
						mapping, i);
			} catch (IncompatibleTypesException e) {
				Debug.println("Creation of provenance relation for mapping:\n"
						+ mapping.toString()
						+ "\nfailed due to type error in the mapping");
				throw (e);
			} catch (IncompatibleKeysException ke) {
				Debug.println("Creation of provenance relation for mapping:\n"
						+ mapping.toString()
						+ "\nfailed due to mismatch bw keys in the mapping");
				throw (ke);
			}

			Atom pickOne = mapping.getMappingHead().get(0);

			RelationContext relCtx = new RelationContext(rel, pickOne
					.getSchema(), pickOne.getPeer(), true);

			mapping.setProvenanceRelation(relCtx);
			mappingRels.add(relCtx);

		}

		return mappingRels;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.upenn.cis.orchestra.exchange.ITranslationRuleGen#getState()
	 */
	public ITranslationState getState() {
		return state;
	}

	/**
	 * Returns a new {@code ITranslationRuleGen}.
	 * 
	 * @param systemMappings
	 * @param userRelations
	 * @param builtInSchemas
	 * @param isBidirectional
	 * 
	 * @param bidirectional
	 * 
	 * @return a new {@code ITranslationGen}
	 * @throws ConfigurationException for an illegal combination of options
	 */
	public static ITranslationRuleGen newInstance(List<Mapping> systemMappings,
			List<RelationContext> userRelations,
			Map<String, Schema> builtInSchemas, boolean isBidirectional) {
		boolean outerUnion = Config.getOuterUnion();
		boolean outerJoin = Config.getOuterJoin();

		checkOptions(isBidirectional, outerUnion, outerJoin);
		if (outerUnion) {
			return new OuterUnionTranslationRuleGen(systemMappings,
					userRelations, builtInSchemas, isBidirectional);
		} else if (outerJoin) {
			return new OuterJoinTranslationRuleGen(systemMappings,
					userRelations, builtInSchemas, isBidirectional);
		} else {
			return new DefaultTranslationRuleGen(systemMappings, userRelations,
					builtInSchemas, isBidirectional);
		}
	}

	/**
	 * Returns a new {@code ITranslationRuleGen}.
	 * 
	 * @param systemMappingsDoc
	 * @param userRelationsDoc
	 * @param builtInSchemasDoc
	 * @param system
	 * 
	 * @return a new {@code ITranslationGen}
	 * @throws XMLParseException
	 * @throws ConfigurationException for an illegal combination of options
	 */
	public static ITranslationRuleGen newInstance(Document systemMappingsDoc,
			Document userRelationsDoc, Document builtInSchemasDoc,
			OrchestraSystem system) throws XMLParseException {
		boolean outerUnion = Config.getOuterUnion();
		boolean outerJoin = Config.getOuterJoin();
		List<Mapping> systemMappings = deserializeVerboseMappings(
				systemMappingsDoc.getDocumentElement(), system);
		List<RelationContext> userRelations = deserializeRelationContexts(
				userRelationsDoc.getDocumentElement(), system);
		Map<String, Schema> builtInSchemas = OrchestraSystem
				.deserializeBuiltInFunctions(builtInSchemasDoc);
		boolean isBidirectional = system.isBidirectional();

		checkOptions(isBidirectional, outerUnion, outerJoin);
		if (outerUnion) {
			return new OuterUnionTranslationRuleGen(systemMappings,
					userRelations, builtInSchemas, isBidirectional);
		} else if (outerJoin) {
			return new OuterJoinTranslationRuleGen(systemMappings,
					userRelations, builtInSchemas, isBidirectional);
		} else {
			return new DefaultTranslationRuleGen(systemMappings, userRelations,
					builtInSchemas, isBidirectional);
		}
	}

	/**
	 * We cannot have both outerUnion and outerJoin selected. If there are
	 * bidirectional mappings then, neither outerUnion or outerJoin may be true.
	 * 
	 * @param bidirectional
	 * @param outerUnion
	 * @param outerJoin
	 */
	private static void checkOptions(boolean bidirectional, boolean outerUnion,
			boolean outerJoin) {
		if (outerUnion && outerJoin) {
			throw new ConfigurationException(
					"At most one of the following Config options are allowed to be true: 'outerunion' ["
							+ outerUnion
							+ "], 'outerjoin' ["
							+ outerJoin
							+ "].");
		} else if ((outerUnion && bidirectional)
				|| (outerJoin && bidirectional)) {
			String badOption = (outerUnion) ? "'outerunion'" : "'outerjoin'";
			throw new ConfigurationException(
					"The option "
							+ badOption
							+ " cannot be true since this Orchestra system contains bidirectional mappings");
		}
	}
}
