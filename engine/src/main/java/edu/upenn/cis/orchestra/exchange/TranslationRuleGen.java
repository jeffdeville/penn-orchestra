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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.w3c.dom.Document;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.Debug;
import edu.upenn.cis.orchestra.OrchestraUtil;
import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datalog.atom.AtomAnnotation;
import edu.upenn.cis.orchestra.datalog.atom.AtomAnnotationFactory;
import edu.upenn.cis.orchestra.datalog.atom.AtomArgument;
import edu.upenn.cis.orchestra.datalog.atom.AtomConst;
import edu.upenn.cis.orchestra.datalog.atom.AtomVariable;
import edu.upenn.cis.orchestra.datamodel.ITranslationState;
import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationContext;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.TranslationState;
import edu.upenn.cis.orchestra.datamodel.TrustConditions;
import edu.upenn.cis.orchestra.datamodel.AbstractRelation.BadColumnName;
import edu.upenn.cis.orchestra.datamodel.exceptions.IncompatibleKeysException;
import edu.upenn.cis.orchestra.datamodel.exceptions.IncompatibleTypesException;
import edu.upenn.cis.orchestra.datamodel.exceptions.RelationNotFoundException;
import edu.upenn.cis.orchestra.datamodel.exceptions.RelationUpdateException;
import edu.upenn.cis.orchestra.exceptions.ConfigurationException;
import edu.upenn.cis.orchestra.exchange.sql.SqlEngine;
import edu.upenn.cis.orchestra.mappings.MappingTopologyTest;
import edu.upenn.cis.orchestra.mappings.MappingsCompositionMgt;
import edu.upenn.cis.orchestra.mappings.MappingsIOMgt;
import edu.upenn.cis.orchestra.mappings.MappingsInversionMgt;
import edu.upenn.cis.orchestra.mappings.MappingsTranslationMgt;
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.provenance.ProvenanceGeneration;
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
	 * Take the set of mappings, make sure Skolemization and renaming is applied,
	 * create provenance relations, and create mappings through the provenance
	 * relations.  Also creates the in/out relations.
	 * 
	 * @see
	 * edu.upenn.cis.orchestra.exchange.ITranslationRuleGen#computeTranslationRules
	 * ()
	 * 
	 * @param peers Set of peers
	 * @param trustMapping Map from peer name -> schema name -> TrustConditions
	 */
	public List<Rule> computeTranslationRules(Collection<Peer> peers,
			Map<String, Map<String, TrustConditions>> trustMapping) throws Exception {

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

			/*
			 * Add simple mappings from base relations to peer relations
			 * (of the form R_L to R)
			 */
			final List<Rule> local2peerRules = MappingsIOMgt.inOutTranslationL(
					builtInSchemas, relations);

			state.setLocal2PeerRules(local2peerRules);
			
			mappings.addAll(state.getLocal2PeerRules());

			/*
			 * Regularize the mappings with Skolems, unique names, etc.
			 */
			
			// Make variables in each mapping different
			int i = 0;
			Set<String> seen = new HashSet<String>();
			for (Mapping m : mappings) {
				//m.renameVariables("_M" + i);
				
				m.uniquifyVariables(seen, "_M" + i);
				i++;
			}
			state.setMappings(mappings);
			List<Mapping> skolMappings;
			List<RelationContext> allMappingRels;
			List<RelationContext> realMappingRels = new ArrayList<RelationContext>();

			skolMappings = MappingsInversionMgt.skolemizeMappings(mappings);
			
			/*
			 * Eliminate non-materialized mapping relations through composition
			 */
			
			// TODO: add any trust conditions here relating to the eliminated relation!!!
			MappingsCompositionMgt
					.composeMappings(skolMappings, builtInSchemas);

			/*
			 * Create provenance tables
			 */
			allMappingRels = computeProvenanceRelations(peers, skolMappings);
			
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

			/**
			 * Add the sets of rules to/from the provenance tables
			 */
			computeProvRules(source2provRules, prov2targetMappings);
			
			// Re-propagate the labeled nulls here, to the new head atom
			Set<Relation> rels = new HashSet<Relation>();
			for (Mapping r : source2provRules) {
				MappingTopologyTest.propagateLabeledNulls(r);
				
				for (Atom a : r.getMappingHead()) {
					Relation rel = a.getRelation();
					rels.add(rel);
				}
			}
			for (Relation rel : rels)
				try {
					SqlEngine.addLabeledNulls(rel);
				} catch (BadColumnName e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}

			/*
			 * Break mappings into multiple rules
			 */
			List<Rule> source2targetRules = MappingsInversionMgt.splitMappingsHeads(
					skolMappings, builtInSchemas);
			
			/*
			 * Add the rejection rule modifications
			 */
			source2targetRules = MappingsIOMgt.inOutTranslationR(
					source2targetRules, true);

			// Add the base trust values
			if (Config.addTrustAnnotations()) {
				for (Rule r : local2peerRules) {
					setTrustAnnotationsFor(peers, trustMapping, r, true);
				}
			}
			
		if (Config.addTrustAnnotations()) {
			for (Rule r : source2provRules) {
				setTrustAnnotationsFor(peers, trustMapping, r, false);
			}
			for (Mapping m : prov2targetMappings)
				setTrustAnnotationsFor(peers, trustMapping, m, false);
		}
		
			// TODO: see if we can move this to the creation of skolMappings, which should
			// then make the composition logic work.
			if (Config.addTrustAnnotations()) {
				for (Rule r : source2targetRules) {
					setTrustAnnotationsFor(peers, trustMapping, r, false);
				}
			}
			

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

	/**
	 * For each mapping, create a provenance relation
	 * 
	 * @param mappings
	 * @return
	 * @throws IncompatibleTypesException
	 * @throws IncompatibleKeysException
	 */
	static List<RelationContext> computeProvenanceRelations(
			Collection<Peer> peers, List<Mapping> mappings) throws IncompatibleTypesException,
			IncompatibleKeysException {

		List<RelationContext> mappingRels = new ArrayList<RelationContext>();

		for (int i = 0; i < mappings.size(); i++) {
			final Mapping mapping = mappings.get(i);

			/*
			final List<AtomVariable> allVars = mapping.getAllBodyVariables();
			final List<AtomArgument> allVarsCast = new ArrayList<AtomArgument>(
					allVars.size());
			for (final AtomVariable var : allVars)
				allVarsCast.add(var);*/

			if (mapping instanceof Rule) {
				mapping.setDescription("+");
			} else {
				mapping.setDescription(mapping.getId());
			}

			Relation rel = null;
			try {
				rel = ProvenanceGeneration.computeProvenanceRelation(
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
			
			for (Atom other : mapping.getMappingHead()) {
				if (other.getSchema() != pickOne.getSchema() || other.getPeer() != pickOne.getPeer())
					throw new RuntimeException("Illegal to have two peers or schemas as target of a single mapping");
			}
			
			for (Peer p : peers) {
				AtomAnnotation ann = AtomAnnotationFactory.createPeerTrustAnnotation(p.getId(), p.getId());
				
				try {
					rel.addCol(ann.getLabel(), ann.getDataType());
				} catch (BadColumnName e) {
					throw new RuntimeException(e.getMessage());
				}

			}
			RelationContext relCtx = new RelationContext(rel, pickOne
					.getSchema(), pickOne.getPeer(), true);

			mapping.setProvenanceRelation(relCtx);
			mappingRels.add(relCtx);

			Debug.println("Added provenance relation " + rel.getName() + rel.getFieldsInList());
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

	/**
	 * Create trust attributes for each of the peers, for a specific
	 * mapping rule
	 * 
	 * @return
	 */
//	public void setTrustAnnotationsFor(Collection<Peer> peers, 
//			Map<String, Map<String, TrustConditions>> tcs, Rule r, boolean isBase) throws RelationNotFoundException, BadColumnName {
//		setTrustAnnotationsFor(peers, tcs, /*r.getHead(),*/ r, isBase);
//	}
	
	private boolean isLocal(Relation r) {
		return r.getName().endsWith(Relation.LOCAL) || r.getName().endsWith(Relation.REJECT);
	}

	/**
	 * Create trust attributes for each of the peers, for a specific
	 * mapping
	 * 
	 * @return
	 */
	public void setTrustAnnotationsFor(Collection<Peer> peers, 
			Map<String, Map<String, TrustConditions>> tcs, Mapping m, boolean isBase) throws RelationNotFoundException, BadColumnName {

		// Choose the first item with a real schema as the "representative" head for
		// purposes of determining a trust condition
		Atom head = null;
		for (Atom h : m.getMappingHead())
			if (head == null || head.getSchema() == null)
				head = h;
		
//			setTrustAnnotationsFor(peers, tcs, h, m, isBase);
		// Don't double-annotate the rule
//		if (head.isAnnotated())
//			return;
		
		//System.out.println("Adding annotations to " + m.toString());
		
		// See if we have any IDB atoms, from which we can get trust attributes (otherwise we get the default trust value)
		boolean haveTrustValues = false;
		for (Atom a : m.getBody())
			haveTrustValues = haveTrustValues | !isLocal(a.getRelation());
		
		for (Peer p : peers) {
			AtomAnnotation ann = AtomAnnotationFactory.createPeerTrustAnnotation(p.getId(),
					Mapping.getFreshAutogenAnnotationName());

			// We want to find this peer
			// And the trust condition for this target schema

			TrustConditions tc;
			// If trustlevel [ mapping ] then set to this
			if (tcs.get(p.getId()) != null && (tc = tcs.get(p.getId()).get(head.getSchema())) != null) {
	
				// Get the trust conditions for this particular relation
				int relID = head.getRelation().getRelationID();
				
//				AtomVariable output = new AtomVariable(Mapping.getFreshAutogenVariableName());
//				Atom at = ann.setTrustCondition(getMappingDb().getBuiltInSchemas(), output, 
//						tc.getConditions().get(relID));
				AtomConst output = ann.setTrustPriority(tc.getConditions().get(relID));
				
				output.setType(ann.getDataType());
				
//				r.addToBody(at);
				for (Atom h : m.getMappingHead())
					h.addArgument(output, false, true, ann.getLabel(), ann.getDataType());
				
				// elseif leaf then set to default value
			} else if (isBase || !haveTrustValues) {
				AtomConst c = new AtomConst(ann.getDefaultTrustValue());
				c.setType(ann.getDataType());
				
				for (Atom h : m.getMappingHead())
					h.addArgument(c, false, true, ann.getLabel(), ann.getDataType());
				
				// else set computation based on the annotations of the subgoals
			} else {
				// Add an annotation attribute to the atoms
				AtomVariable output = new AtomVariable(Mapping.getFreshAutogenAnnotationName());
				List<AtomArgument> newVariables = new ArrayList<AtomArgument>(); 
				for (Atom a : m.getBody()) {
					// Skip negated atoms (_R table) and any built-in predicates 
					if (!a.isNeg() && a.getSchema() != null && !builtInSchemas.containsKey(a.getSchema().getSchemaId())) {
						AtomVariable in = new AtomVariable(Mapping.getFreshAutogenAnnotationName());
						newVariables.add(in);
						a.addArgument(in, false, true, ann.getLabel(), ann.getDataType());
					}
				}
				// If we have multiple atoms, we need to compute a semiring product
				if (newVariables.size() > 1) {
					Atom at = ann.setTrustDerivation(builtInSchemas, output, 
							newVariables);
					
					m.addToBody(at);
					for (Atom h : m.getMappingHead())
						h.addArgument(output, false, true, ann.getLabel(), ann.getDataType());
					
				// else just carry forward the annotation
				} else {
					for (Atom h : m.getMappingHead())
						h.addArgument(newVariables.get(0), false, true, ann.getLabel(), ann.getDataType());
				}
			}
		}
		Debug.println("Annotated mapping " + m.toString());
	}
	
//	private void setTrustAnnotationsFor(Collection<Peer> peers, 
//			Map<String, Map<String, TrustConditions>> tcs, Atom head, Mapping m, boolean isBase) throws RelationNotFoundException, BadColumnName {
//		
//		// Don't double-annotate the rule
//		if (head.isAnnotated())
//			return;
//		
//		//System.out.println("Adding annotations to " + m.toString());
//		
//		for (Peer p : peers) {
//			AtomAnnotation ann = AtomAnnotationFactory.createPeerTrustAnnotation(p.getId(),
//					Mapping.getFreshAutogenAnnotationName());
//
//			// We want to find this peer
//			// And the trust condition for this target schema
//
//			TrustConditions tc;
//			// If trustlevel [ mapping ] then set to this
//			if (tcs.get(p.getId()) != null && (tc = tcs.get(p.getId()).get(head.getSchema())) != null) {
//				// Get the trust conditions for this particular relation
//				int relID = head.getRelation().getRelationID();
//				
////				AtomVariable output = new AtomVariable(Mapping.getFreshAutogenVariableName());
////				Atom at = ann.setTrustCondition(getMappingDb().getBuiltInSchemas(), output, 
////						tc.getConditions().get(relID));
//				AtomConst output = ann.setTrustPriority(tc.getConditions().get(relID));
//				
//				output.setType(ann.getDataType());
//				
////				r.addToBody(at);
//				head.addArgument(output, false, true, ann.getLabel(), ann.getDataType());
//				
//				// elseif leaf then set to default value
//			} else if (isBase) {
//				AtomConst c = new AtomConst(ann.getDefaultTrustValue());
//				c.setType(ann.getDataType());
//				
//				head.addArgument(c, false, true, ann.getLabel(), ann.getDataType());
//				
//				// else set computation based on the annotations of the subgoals
//			} else {
//				// Add an annotation attribute to the atoms
//				AtomVariable output = new AtomVariable(Mapping.getFreshAutogenAnnotationName());
//				List<AtomArgument> newVariables = new ArrayList<AtomArgument>(); 
//				for (Atom a : m.getBody()) {
//					// Skip negated atoms (_R table) and any built-in predicates 
//					if (!a.isNeg() && a.getSchema() != null && !builtInSchemas.containsKey(a.getSchema().getSchemaId())) {
//						AtomVariable in = new AtomVariable(Mapping.getFreshAutogenAnnotationName());
//						newVariables.add(in);
//						a.addArgument(in, false, true, ann.getLabel(), ann.getDataType());
//					}
//				}
//				// If we have multiple atoms, we need to compute a semiring product
//				if (newVariables.size() > 1) {
//					Atom at = ann.setTrustDerivation(builtInSchemas, output, 
//							newVariables);
//					
//					m.addToBody(at);
//					head.addArgument(output, false, true, ann.getLabel(), ann.getDataType());
//					
//				// else just carry forward the annotation
//				} else
//					head.addArgument(newVariables.get(0), false, true, ann.getLabel(), ann.getDataType());
//			}
//		}
//		Debug.println("Annotated mapping " + m.toString());
//	}
}
