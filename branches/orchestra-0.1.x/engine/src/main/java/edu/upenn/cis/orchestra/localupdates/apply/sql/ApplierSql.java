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
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.Update;
import edu.upenn.cis.orchestra.localupdates.ILocalUpdates;
import edu.upenn.cis.orchestra.localupdates.apply.IApplier;
import edu.upenn.cis.orchestra.localupdates.apply.exceptions.UpdatesNotAppliedException;
import edu.upenn.cis.orchestra.sql.ISqlExpression;
import edu.upenn.cis.orchestra.sql.ISqlFactory;
import edu.upenn.cis.orchestra.sql.ISqlInsert;
import edu.upenn.cis.orchestra.sql.ISqlSelect;
import edu.upenn.cis.orchestra.sql.SqlFactories;
import edu.upenn.cis.orchestra.sql.ISqlConstant.Type;
import edu.upenn.cis.orchestra.sql.ISqlExpression.Code;

/**
 * Uses SQL to apply a {@code ILocalUpdates} to the local database.
 * 
 * @author John Frommeyer
 * 
 */
public class ApplierSql implements IApplier<Connection> {

	private static final Logger logger = LoggerFactory
			.getLogger(ApplierSql.class);
	private static final ISqlFactory sqlFactory = SqlFactories.getSqlFactory();

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.upenn.cis.orchestra.applyupdates.IApplyUpdates#applyUpdates(edu.upenn
	 * .cis.orchestra.extractupdates.ILocalUpdates)
	 */
	@Override
	public void applyUpdates(ILocalUpdates updates, Connection connection)
			throws UpdatesNotAppliedException {

		try {
			Peer peer = updates.getPeer();
			ISqlSelect selectTemplate = sqlFactory.newSelect();
			ISqlExpression valuesTemplate = sqlFactory
					.newExpression(Code.COMMA);
			Collection<Schema> schemas = peer.getSchemas();
			for (Schema schema : schemas) {
				Collection<Relation> relations = schema.getRelations();
				for (Relation relation : relations) {

					if (!relation.isInternalRelation()) {
						List<Update> updateList = updates.getLocalUpdates(
								schema, relation);
						if (!updateList.isEmpty()) {
							setupSqlTemplates(relation, selectTemplate,
									valuesTemplate);
						}
						for (Update update : updateList) {

							Tuple tuple = null;
							String insTable = null;
							String delTable = null;
							if (update.isInsertion()) {
								tuple = update.getNewVal();
								insTable = Relation.LOCAL;
								delTable = Relation.REJECT;
							} else if (update.isDeletion()) {
								tuple = update.getOldVal();
								insTable = Relation.REJECT;
								delTable = Relation.LOCAL;
							} else {
								throw new IllegalStateException(
										"Cannot process update: "
												+ update.toString()
												+ ". Updates should be handled as deletion/insertion pairs.");
							}
							handleTuple(connection, tuple, selectTemplate,
									valuesTemplate, insTable, delTable);
						}
					}
				}
			}
		} catch (SQLException e) {
			throw new UpdatesNotAppliedException(e);
		}

	}

	private void setupSqlTemplates(Relation relation,
			ISqlSelect selectTemplate, ISqlExpression valuesTemplate) {

		selectTemplate.addSelectClause(Collections.singletonList(sqlFactory
				.newSelectItem("COUNT(*)")));
		int ncol = relation.getNumCols();
		ISqlExpression whereClause = sqlFactory.newExpression(Code.AND);
		for (int i = 0; i < ncol; i++) {
			whereClause.addOperand(sqlFactory.newExpression(Code.EQ,
					sqlFactory.newConstant(relation.getColName(i),
							Type.COLUMNNAME), sqlFactory.newConstant("?",
							Type.PREPARED_STATEMENT_PARAMETER)));
			valuesTemplate.addOperand(sqlFactory.newConstant("?",
					Type.PREPARED_STATEMENT_PARAMETER));

		}
		selectTemplate.addWhere(whereClause);
	}

	/**
	 * 
	 * Handles the update of {@code tuple} in its relation Rel. This means:
	 * <dl>
	 * <dt>If the update was an insertion:</dt>
	 * <dd>
	 * <ol>
	 * <li>Add {@code tuple} to Rel_L_INS table.</li>
	 * <li>If {@code tuple} exists in the Rel_R table, then add it to Rel_R_DEL.
	 * </li>
	 * </ol>
	 * </dd>
	 * <dt>If the update was a deletion:</dt>
	 * <dd>
	 * <ol>
	 * <li>Add {@code tuple} to Rel_R_INS table.</li>
	 * <li>If {@code tuple} exists in the Rel_L table, then add it to Rel_L_DEL.
	 * </li>
	 * </ol>
	 * </dd>
	 * </dl>
	 * 
	 * @param connection
	 * @param tuple
	 * @param selectTemplate
	 * @param valuesTemplate
	 * @param insTable
	 * @param delTable
	 * @throws SQLException
	 * 
	 * 
	 */
	private void handleTuple(Connection connection, Tuple tuple,
			ISqlSelect selectTemplate, ISqlExpression valuesTemplate,
			String insTable, String delTable) throws SQLException {
		Relation relation = tuple.getSchema();
		String fqName = relation.getFullQualifiedDbId();
		selectTemplate.addFromClause(Collections.singletonList(sqlFactory
				.newFromItem(fqName + delTable)));

		ISqlInsert insInsert = sqlFactory.newSqlInsert(fqName + insTable
				+ Relation.INSERT);
		insInsert.addValueSpec(valuesTemplate);

		logger.debug("INS insert: {}", insInsert);
		logger.debug("Select: {}", selectTemplate);

		PreparedStatement insInsertStatement = connection
				.prepareStatement(insInsert.toString());
		PreparedStatement selectStatement = connection
				.prepareStatement(selectTemplate.toString());
		int ncol = tuple.getNumCols();
		for (int i = 0; i < ncol; i++) {
			insInsertStatement.setObject(i + 1, tuple.get(i), relation
					.getColType(i).getSqlTypeCode());
			selectStatement.setObject(i + 1, tuple.get(i), relation.getColType(
					i).getSqlTypeCode());
		}
		insInsertStatement.executeUpdate();
		ResultSet foundInDelTableResult = selectStatement.executeQuery();
		foundInDelTableResult.next();
		int count = foundInDelTableResult.getInt(1);
		if (count > 0) {
			ISqlInsert delInsert = sqlFactory.newSqlInsert(fqName + delTable
					+ Relation.DELETE);
			delInsert.addValueSpec(valuesTemplate);
			logger.debug("DEL insert: {}", delInsert);
			PreparedStatement delInsertStatement = connection
					.prepareStatement(delInsert.toString());
			for (int i = 0; i < ncol; i++) {
				delInsertStatement.setObject(i + 1, tuple.get(i), relation
						.getColType(i).getSqlTypeCode());
			}
			delInsertStatement.executeUpdate();
		}

	}

}
