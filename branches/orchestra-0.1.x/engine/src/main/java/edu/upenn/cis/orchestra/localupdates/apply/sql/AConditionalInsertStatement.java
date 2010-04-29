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

import static edu.upenn.cis.orchestra.OrchestraUtil.newArrayList;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationField;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.exchange.sql.SqlEngine;
import edu.upenn.cis.orchestra.sql.ISqlConstant;
import edu.upenn.cis.orchestra.sql.ISqlExpression;
import edu.upenn.cis.orchestra.sql.ISqlFactory;
import edu.upenn.cis.orchestra.sql.ISqlInsert;
import edu.upenn.cis.orchestra.sql.SqlFactories;
import edu.upenn.cis.orchestra.sql.ISqlConstant.Type;
import edu.upenn.cis.orchestra.sql.ISqlExpression.Code;

/**
 * Represents a conditional SQL insert. If {@code shouldInsert(tuple) == true}
 * the insert will be applied, otherwise not.
 * 
 * @author John Frommeyer
 * 
 */
abstract class AConditionalInsertStatement {
	private final PreparedStatement insStatement;
	protected final ISqlFactory sqlFactory = SqlFactories.getSqlFactory();
	protected final Connection connection;
	private final ISqlExpression valuesTemplate = sqlFactory
			.newExpression(Code.COMMA);
	private final List<ISqlConstant> targetColumns = newArrayList();
	private boolean populated = false;

	abstract protected boolean shouldInsert(Tuple tuple) throws Exception;

	private static final Logger logger = LoggerFactory
			.getLogger(AConditionalInsertStatement.class);
	private final String insTableName;

	/**
	 * 
	 * Creates a conditional insert statement into {@code
	 * baseRelation.getFullQualifiedDbId() + relationType + deltaType}.
	 * 
	 * @param baseRelation the base relation
	 * @param relationType either {@code Relation.LOCAL} or {@code Relation.REJECT}
	 * @param deltaType either {@code Relation.INSERT} or {@code Relation.DELETE}
	 * @param connection
	 * @throws SQLException
	 */
	AConditionalInsertStatement(Relation baseRelation, String relationType,
			String deltaType,
			@SuppressWarnings("hiding") final Connection connection)
			throws SQLException {
		setupSqlTemplates(baseRelation);
		insTableName = baseRelation.getFullQualifiedDbId() + relationType
				+ deltaType;
		ISqlInsert insert = sqlFactory.newInsert(insTableName);
		insert.addTargetColumns(targetColumns);
		insert.addValueSpec(valuesTemplate);
		logger.debug("Insert: {}", insert);
		this.connection = connection;
		insStatement = connection.prepareStatement(insert.toString());
	}

	final void handleTuple(Tuple tuple) throws Exception {
		if (shouldInsert(tuple)) {
			populateStatement(tuple, insStatement);
			insStatement.addBatch();
			populated = true;
		}
	}

	final int apply() throws SQLException {
		try {
			int result;
			if (populated) {
				int[] results = insStatement.executeBatch();
				result = resultSum(results);
			} else {
				result = 0;
			}
			return result;
		} finally {
			insStatement.close();
		}
	}

	private void setupSqlTemplates(Relation relation) {
		List<RelationField> fields = relation.getFields();
		for (RelationField field : fields) {
			valuesTemplate.addOperand(sqlFactory.newConstant("?",
					Type.PREPARED_STATEMENT_PARAMETER));
			targetColumns.add(sqlFactory.newConstant(field.getName(),
					ISqlConstant.Type.COLUMNNAME));
			if (field.getType().isLabeledNullable()) {
				valuesTemplate.addOperand(sqlFactory.newConstant("?",
						Type.PREPARED_STATEMENT_PARAMETER));
				targetColumns.add(sqlFactory.newConstant(field.getName()
						+ RelationField.LABELED_NULL_EXT,
						ISqlConstant.Type.COLUMNNAME));
			}
		}

	}

	protected static void populateStatement(Tuple tuple,
			PreparedStatement statement) throws SQLException {
		int parameterIndex = 1;
		int nfields = tuple.getNumCols();
		for (int i = 0; i < nfields; i++) {
			boolean sawLabeledNullable = populateStatement(tuple, i,
					parameterIndex, statement);
			parameterIndex = sawLabeledNullable ? (parameterIndex + 2)
					: (parameterIndex + 1);

		}
	}

	/**
	 * 
	 * Returns {@code true} if the type of the field {@code i} of {@code tuple}
	 * is labeled nullable, and {@code false} otherwise.
	 * <p>
	 * Populates the parameter at position {@code parameterIndex} (and possibly
	 * {@code parameterIndex + 1}) of the supplied {@code PreparedStatement}s
	 * with the value from field {@code i} of {@code tuple} in a labeled
	 * null-aware way.
	 * <p>
	 * Assumes that the column names in {@code statements} are ordered so that
	 * if a field is labeled nullable, the labeled null column name immediately
	 * follows the base column name.
	 * 
	 * @param tuple the tuple being used to populate the statements
	 * @param fieldIndex the current tuple field index being handled
	 * @param parameterIndex the current database column being handled
	 * @param statement
	 * @return {@code true} if the type of the field {@code i} of {@code tuple}
	 *         is labeled nullable, and {@code false} otherwise
	 * @throws SQLException
	 */
	private static boolean populateStatement(Tuple tuple, int fieldIndex,
			int parameterIndex, PreparedStatement statement)
			throws SQLException {
		Relation relation = tuple.getSchema();
		boolean labeledNullSeenThisTime = false;

		int type = relation.getColType(fieldIndex).getSqlTypeCode();
		if (tuple.isLabeledNullable(fieldIndex)) {
			labeledNullSeenThisTime = true;
			if (tuple.isLabeledNull(fieldIndex)) {
				int lnValue = tuple.getLabeledNull(fieldIndex);
				statement.setNull(parameterIndex, type);
				statement.setInt(parameterIndex + 1, lnValue);
			} else {
				Object object = tuple.get(fieldIndex);
				statement.setObject(parameterIndex, object, type);
				statement.setInt(parameterIndex + 1,
						SqlEngine.LABELED_NULL_NONVALUE);
			}
		} else {
			Object object = tuple.get(fieldIndex);
			statement.setObject(parameterIndex, object, type);

		}
		return labeledNullSeenThisTime;
	}

	@Override
	public String toString() {
		return "Insertion into " + insTableName;
	}

	private int resultSum(int[] result) {
		int count = 0;
		for (int i : result) {
			count += i;
		}
		return count;
	}
}
