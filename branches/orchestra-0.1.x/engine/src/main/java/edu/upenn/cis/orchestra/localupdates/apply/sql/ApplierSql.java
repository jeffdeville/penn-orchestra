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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationField;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.Update;
import edu.upenn.cis.orchestra.exchange.sql.SqlEngine;
import edu.upenn.cis.orchestra.localupdates.ILocalUpdates;
import edu.upenn.cis.orchestra.localupdates.apply.IApplier;
import edu.upenn.cis.orchestra.localupdates.apply.exceptions.UpdatesNotAppliedException;
import edu.upenn.cis.orchestra.sql.ISqlConstant;
import edu.upenn.cis.orchestra.sql.ISqlExp;
import edu.upenn.cis.orchestra.sql.ISqlExpression;
import edu.upenn.cis.orchestra.sql.ISqlFactory;
import edu.upenn.cis.orchestra.sql.ISqlInsert;
import edu.upenn.cis.orchestra.sql.ISqlSelect;
import edu.upenn.cis.orchestra.sql.SqlFactories;
import edu.upenn.cis.orchestra.sql.ISqlConstant.Type;
import edu.upenn.cis.orchestra.sql.ISqlExpression.Code;

/**
 * Uses SQL to apply a {@code ILocalUpdates} to the local database. The basic
 * rules followed are:
 * <p>
 * <dl>
 * <dt>If the update was an insertion:</dt>
 * <dd>
 * <ol>
 * <li>Add {@code tuple} to Rel_L_INS table.</li>
 * <li>If {@code tuple} exists in the Rel_R table, then add it to Rel_R_DEL.</li>
 * </ol>
 * </dd>
 * <dt>If the update was a deletion:</dt>
 * <dd>
 * <ol>
 * <li>Add {@code tuple} to Rel_R_INS table.</li>
 * <li>If {@code tuple} exists in the Rel_L table, then add it to Rel_L_DEL.</li>
 * </ol>
 * </dd>
 * </dl>
 * 
 * @author John Frommeyer
 * 
 */
public class ApplierSql implements IApplier<Connection> {

	private static final Logger logger = LoggerFactory
			.getLogger(ApplierSql.class);

	/**
	 * {@inheritDoc}
	 * 
	 * @see edu.upenn.cis.orchestra.applyupdates.IApplyUpdates#applyUpdates(edu.upenn
	 *      .cis.orchestra.extractupdates.ILocalUpdates)
	 */
	@Override
	public void applyUpdates(ILocalUpdates updates, Connection connection)
			throws UpdatesNotAppliedException {

		try {
			Peer peer = updates.getPeer();
			Collection<Schema> schemas = peer.getSchemas();
			for (Schema schema : schemas) {
				Collection<Relation> relations = schema.getRelations();
				for (Relation relation : relations) {

					if (!relation.isInternalRelation()) {
						List<Update> updateList = updates.getLocalUpdates(
								schema, relation);

						if (!updateList.isEmpty()) {
							ApplierStatements statementsForInsertions = ApplierStatements
									.createStatementsForInsertions(connection,
											relation);
							ApplierStatements statementsForDeletions = ApplierStatements
									.createStatementsForDeletions(connection,
											relation);
							for (Update update : updateList) {
								if (update.isInsertion()) {
									Tuple insertedTuple = update.getNewVal();
									handleTuple(insertedTuple,
											statementsForInsertions);
								} else if (update.isDeletion()) {
									Tuple deletedTuple = update.getOldVal();
									handleTuple(deletedTuple,
											statementsForDeletions);
								} else {
									throw new IllegalStateException(
											"Cannot process update: "
													+ update.toString()
													+ ". Updates should be handled as deletion/insertion pairs.");
								}

							}
						}
					}
				}
			}
		} catch (SQLException e) {
			throw new UpdatesNotAppliedException(e);
		}

	}

	private void handleTuple(Tuple tuple, ApplierStatements statements)
			throws SQLException {

		PreparedStatement insStatement = statements.getInsInsertStatement();
		PreparedStatement countStatement = statements.getCountStatement();
		populateStatements(tuple, insStatement, countStatement);

		insStatement.executeUpdate();
		ResultSet foundInDelTableResult = countStatement.executeQuery();

		foundInDelTableResult.next();
		int count = foundInDelTableResult.getInt(1);

		if (count > 0) {
			PreparedStatement delInsertStatement = statements
					.getDelInsertStatement();
			populateStatements(tuple, delInsertStatement);

			delInsertStatement.executeUpdate();
		}
	}

	private void populateStatements(Tuple tuple,
			PreparedStatement insInsertStatement,
			PreparedStatement selectStatement) throws SQLException {
		populateStatements(tuple, new PreparedStatement[] { insInsertStatement,
				selectStatement });
	}

	private void populateStatements(Tuple tuple, PreparedStatement statement)
			throws SQLException {
		populateStatements(tuple, new PreparedStatement[] { statement });
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
	 * @param statements the statements to be populated
	 * @param fieldIndex the current tuple field index being handled
	 * @param parameterIndex the current database column being handled
	 * @return {@code true} if the type of the field {@code i} of {@code tuple}
	 *         is labeled nullable, and {@code false} otherwise
	 * @throws SQLException
	 */
	private boolean populateStatements(Tuple tuple,
			PreparedStatement[] statements, int fieldIndex, int parameterIndex)
			throws SQLException {
		Relation relation = tuple.getSchema();
		boolean labeledNullSeenThisTime = false;

		int type = relation.getColType(fieldIndex).getSqlTypeCode();
		if (tuple.isLabeledNullable(fieldIndex)) {
			labeledNullSeenThisTime = true;
			if (tuple.isLabeledNull(fieldIndex)) {
				int lnValue = tuple.getLabeledNull(fieldIndex);
				for (PreparedStatement statement : statements) {
					statement.setNull(parameterIndex, type);
					statement.setInt(parameterIndex + 1, lnValue);
				}
			} else {
				Object object = tuple.get(fieldIndex);
				for (PreparedStatement statement : statements) {
					statement.setObject(parameterIndex, object, type);
					statement.setInt(parameterIndex + 1,
							SqlEngine.LABELED_NULL_NONVALUE);
				}
			}
		} else {
			Object object = tuple.get(fieldIndex);
			for (PreparedStatement statement : statements) {
				statement.setObject(parameterIndex, object, type);
			}
		}
		return labeledNullSeenThisTime;
	}

	private void populateStatements(Tuple tuple, PreparedStatement[] statements)
			throws SQLException {
		int parameterIndex = 1;
		int nfields = tuple.getNumCols();
		for (int i = 0; i < nfields; i++) {
			boolean sawLabeledNullable = populateStatements(tuple, statements,
					i, parameterIndex);
			parameterIndex = sawLabeledNullable ? (parameterIndex + 2)
					: (parameterIndex + 1);

		}
	}
}

/**
 * 
 * Takes care of creating the {@code PreparedStatment}s needed by {@code
 * ApplierSql}. The returned statements are written in such a way that if the
 * parameter in position i corresponds to a labeled nullable field, then the
 * associated labeled null column parameter is at position i + 1.
 * 
 * @author John Frommeyer
 * 
 */
class ApplierStatements {
	private final ISqlFactory sqlFactory = SqlFactories.getSqlFactory();
	private final ISqlSelect selectTemplate = sqlFactory.newSelect();
	private final ISqlExpression valuesTemplate = sqlFactory
			.newExpression(Code.COMMA);
	private final List<ISqlConstant> targetColumns = newArrayList();
	private final Connection connection;
	private final String insTableName;
	private final String delTableName;
	private final String countTableName;
	private static final Logger logger = LoggerFactory
			.getLogger(ApplierStatements.class);
	private PreparedStatement insInsertStatement;
	private PreparedStatement countStatement;
	private PreparedStatement delInsertStatement;

	static ApplierStatements createStatementsForInsertions(
			Connection connection, Relation relation) {
		// For an insertion into relation Rel, we update Rel_L_INS and possibly
		// Rel_R_DEL.
		return new ApplierStatements(connection, relation, Relation.LOCAL,
				Relation.REJECT);
	}

	static ApplierStatements createStatementsForDeletions(
			Connection connection, Relation relation) {
		// For an deletion from relation Rel, we update Rel_R_INS and possibly
		// Rel_L_DEL.
		return new ApplierStatements(connection, relation, Relation.REJECT,
				Relation.LOCAL);
	}

	private ApplierStatements(
			@SuppressWarnings("hiding") final Connection connection,
			Relation relation, String insTable, String delTable) {
		this.connection = connection;
		String fqName = relation.getFullQualifiedDbId();
		insTableName = fqName + insTable + Relation.INSERT;
		delTableName = fqName + delTable + Relation.DELETE;
		countTableName = fqName + delTable;
		setupSqlTemplates(relation);

	}

	PreparedStatement getInsInsertStatement() throws SQLException {
		if (insInsertStatement == null) {
			insInsertStatement = getInsInsert();
		}
		return insInsertStatement;
	}

	PreparedStatement getCountStatement() throws SQLException {
		if (countStatement == null) {
			countStatement = getCount();
		}
		return countStatement;
	}

	PreparedStatement getDelInsertStatement() throws SQLException {
		if (delInsertStatement == null) {
			delInsertStatement = getDelInsert();
		}
		return delInsertStatement;
	}

	private void setupSqlTemplates(Relation relation) {

		selectTemplate.addSelectClause(Collections.singletonList(sqlFactory
				.newSelectItem().setExpression(
						sqlFactory.newExpression(Code.COUNT))));

		ISqlExp whereClause = null;
		List<RelationField> fields = relation.getFields();
		for (RelationField field : fields) {
			ISqlExp condition = sqlFactory.newExpression(Code.EQ, sqlFactory
					.newConstant(field.getName(), Type.COLUMNNAME), sqlFactory
					.newConstant("?", Type.PREPARED_STATEMENT_PARAMETER));
			whereClause = (whereClause == null) ? condition : sqlFactory
					.newExpression(Code.AND, whereClause, condition);
			valuesTemplate.addOperand(sqlFactory.newConstant("?",
					Type.PREPARED_STATEMENT_PARAMETER));
			targetColumns.add(sqlFactory.newConstant(field.getName(),
					ISqlConstant.Type.COLUMNNAME));
			if (field.getType().isLabeledNullable()) {
				ISqlExp lnCondition = sqlFactory.newExpression(Code.EQ,
						sqlFactory.newConstant(field.getName()
								+ RelationField.LABELED_NULL_EXT,
								Type.COLUMNNAME), sqlFactory.newConstant("?",
								Type.PREPARED_STATEMENT_PARAMETER));
				whereClause = sqlFactory.newExpression(Code.AND, whereClause,
						lnCondition);
				valuesTemplate.addOperand(sqlFactory.newConstant("?",
						Type.PREPARED_STATEMENT_PARAMETER));
				targetColumns.add(sqlFactory.newConstant(field.getName()
						+ RelationField.LABELED_NULL_EXT,
						ISqlConstant.Type.COLUMNNAME));
			}
		}

		selectTemplate.addWhere(whereClause);
	}

	private PreparedStatement getInsInsert() throws SQLException {
		ISqlInsert insInsert = sqlFactory.newInsert(insTableName);
		insInsert.addTargetColumns(targetColumns);
		insInsert.addValueSpec(valuesTemplate);

		logger.debug("INS insert: {}", insInsert);
		PreparedStatement statement = connection.prepareStatement(insInsert
				.toString());
		return statement;
	}

	private PreparedStatement getCount() throws SQLException {
		selectTemplate.addFromClause(Collections.singletonList(sqlFactory
				.newFromItem(countTableName)));
		logger.debug("Select: {}", selectTemplate);
		PreparedStatement statement = connection
				.prepareStatement(selectTemplate.toString());
		return statement;
	}

	private PreparedStatement getDelInsert() throws SQLException {
		ISqlInsert delInsert = sqlFactory.newInsert(delTableName);
		delInsert.addTargetColumns(targetColumns);
		delInsert.addValueSpec(valuesTemplate);
		logger.debug("DEL insert: {}", delInsert);
		PreparedStatement statement = connection.prepareStatement(delInsert
				.toString());
		return statement;
	}

}
