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

package edu.upenn.cis.orchestra.localupdates.apply.sql;

import static edu.upenn.cis.orchestra.OrchestraUtil.newHashMap;
import static edu.upenn.cis.orchestra.OrchestraUtil.newHashSet;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datamodel.ITranslationRules;
import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationContext;
import edu.upenn.cis.orchestra.datamodel.RelationField;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.localupdates.apply.exceptions.DerivabilityCheckException;
import edu.upenn.cis.orchestra.provenance.ProvenanceRelation;
import edu.upenn.cis.orchestra.provenance.ProvenanceRelationColumn;
import edu.upenn.cis.orchestra.sql.ISqlSelect;

/**
 * Given a {@code ITranslationRules} and a {@code Tuple} and access to the
 * Provenance tables determines if the tuple is derivable from the rules.
 * 
 * @author John Frommeyer
 * 
 */
public class DerivabilityCheck implements IDerivabilityCheck {
	private final List<RelationContext> provenanceRelations;
	private final Map<Relation, Set<ProvenanceRelation>> relationToProvenanceRelations = newHashMap();
	private final CountSqlSelect countSqlSelect = new CountSqlSelect();

	// prov rel --> (base rel column name -> prov rel column name)
	private final Map<ProvenanceRelation, Map<String, String>> provenanceRelationToFieldMap = newHashMap();

	private final static Logger logger = LoggerFactory
			.getLogger(DerivabilityCheck.class);

	/**
	 * Creates a {@code IDerivabilityCheck} which will use {@code rules}.
	 * 
	 * @param rules
	 */
	public DerivabilityCheck(ITranslationRules rules) {

		this.provenanceRelations = rules.getRealMappingRelations();

		for (RelationContext context : provenanceRelations) {
			ProvenanceRelation provRelation = (ProvenanceRelation) context
					.getRelation();

			// Map source column names to prov. rel. column names
			List<ProvenanceRelationColumn> columns = provRelation.getColumns();
			Map<String, String> tupleFieldToProvField = newHashMap();
			for (ProvenanceRelationColumn column : columns) {
				List<List<RelationField>> sourceColumnsList = column
						.getSourceColumns();
				for (List<RelationField> sourceColumns : sourceColumnsList) {
					for (RelationField sourceColumn : sourceColumns) {
						tupleFieldToProvField.put(sourceColumn.getName(),
								column.getColumn().getName());
					}
				}
			}
			provenanceRelationToFieldMap.put(provRelation,
					tupleFieldToProvField);
			logger
					.debug("Adding {} -> {}", provRelation,
							tupleFieldToProvField);

			// Map base rel to associated prov rels.
			List<Mapping> candidateMappings = provRelation.getMappings();
			for (Mapping mapping : candidateMappings) {
				// We only care about non-fake mappings.
				if (!mapping.isFakeMapping()) {
					List<Atom> head = mapping.getMappingHead();
					for (Atom headAtom : head) {
						Relation headRelation = headAtom.getRelation();
						Set<ProvenanceRelation> provRelSet = relationToProvenanceRelations
								.get(headRelation);
						if (provRelSet == null) {
							provRelSet = newHashSet();
							relationToProvenanceRelations.put(headRelation,
									provRelSet);
						}
						provRelSet.add(provRelation);
						logger.debug("Adding {} -> {}", headRelation,
								provRelation);

					}
				}
			}
		}
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see edu.upenn.cis.orchestra.localupdates.apply.sql.IDerivabilityCheck#isDerivable(edu.upenn.cis.orchestra.datamodel.Tuple)
	 */
	@Override
	public boolean isDerivable(Tuple tuple, Connection connection)
			throws DerivabilityCheckException {
		boolean derivable = false;
		try {
			Relation tupleRelation = tuple.getSchema();
			Set<ProvenanceRelation> relevantProvRels = relationToProvenanceRelations
					.get(tupleRelation);
			if (relevantProvRels != null) {
				Iterator<ProvenanceRelation> iter = relevantProvRels.iterator();
				while (!derivable && iter.hasNext()) {
					ProvenanceRelation provRel = iter.next();
					ISqlSelect count = countSqlSelect.getSqlSelect(provRel
							.getFullQualifiedDbId(), tuple,
							provenanceRelationToFieldMap.get(provRel));
					Statement countStatement = connection.createStatement();
					ResultSet resultSet = countStatement.executeQuery(count
							.toString());
					resultSet.next();
					int countResult = resultSet.getInt(1);
					derivable = (countResult > 0);
				}
			}

			return derivable;
		} catch (Exception e) {
			throw new DerivabilityCheckException(tuple, e);
		}
	}
}
