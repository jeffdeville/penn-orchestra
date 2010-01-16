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
package edu.upenn.cis.orchestra.localupdates.extract.db2;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import edu.upenn.cis.orchestra.OrchestraUtil;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationField;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.Update;
import edu.upenn.cis.orchestra.datamodel.AbstractRelation.NameNotFound;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.localupdates.ILocalUpdates;
import edu.upenn.cis.orchestra.localupdates.LocalUpdates;
import edu.upenn.cis.orchestra.localupdates.extract.IExtractor;
import edu.upenn.cis.orchestra.localupdates.extract.exceptions.DBConnectionError;
import edu.upenn.cis.orchestra.localupdates.extract.exceptions.RDBMSExtractError;
import edu.upenn.cis.orchestra.localupdates.extract.exceptions.SchemaIncoherentWithDBError;
import edu.upenn.cis.orchestra.sql.ISqlFactory;
import edu.upenn.cis.orchestra.sql.ISqlSelect;
import edu.upenn.cis.orchestra.sql.SqlFactories;
import edu.upenn.cis.orchestra.sql.ISqlConstant.Type;

/**
 * An implementation of {@code IExtractor} for DB2 relying on SQL replication.
 * 
 * @author John Frommeyer
 * 
 */
public class ExtractorDB2 implements IExtractor<Connection> {

	private final ISqlFactory sqlFactory = SqlFactories.getSqlFactory();
	private final static String OPERATION_COL = "IBMSNAP_OPERATION";
	private final static String COMMITSEQ_COL = "IBMSNAP_COMMITSEQ";
	private final static String INTENTSEQ_COL = "IBMSNAP_INTENTSEQ";
	private final static String CCD_SUFFIX = "_CCD";

	private static enum Operation {
		INSERTION, DELETION, UPDATE;
		static Operation fromString(String op) {
			if ("I".equals(op)) {
				return INSERTION;
			} else if ("D".equals(op)) {
				return DELETION;
			} else if ("U".equals(op)) {
				return UPDATE;
			} else {
				throw new IllegalArgumentException("Unrecognized operation ["
						+ op + "]. Should be one of 'I', 'D', or 'U'.");
			}
		}
	}

	private void processRelation(Relation relation, Schema schema,
			Connection connection, LocalUpdates.Builder builder)
			throws SQLException, RDBMSExtractError {

		String ccdName = "CCDTEST." + relation.getDbRelName() + CCD_SUFFIX;

		ISqlSelect select = sqlFactory.newSelect(
				sqlFactory.newSqlSelectItem("*"),
				sqlFactory.newFromItem(ccdName)).addOrderBy(
				OrchestraUtil.newArrayList(sqlFactory
						.newSqlOrderByItem(sqlFactory.newConstant(
								COMMITSEQ_COL, Type.COLUMNNAME)), sqlFactory
						.newSqlOrderByItem(sqlFactory.newConstant(
								INTENTSEQ_COL, Type.COLUMNNAME))));

		Statement statement = null;
		try {
			statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_UPDATABLE);
			ResultSet updateSet = statement.executeQuery(select.toString()
					+ " FOR UPDATE");
			while (updateSet.next()) {
				Operation op = Operation.fromString(updateSet
						.getString(OPERATION_COL));
				extractUpdate(schema, relation, updateSet, op, builder);
				updateSet.deleteRow();
			}
		} finally {
			if (statement != null) {
				statement.close();
			}
		}
	}

	private void extractUpdate(Schema schema, Relation relation,
			ResultSet updateSet, Operation op, LocalUpdates.Builder builder)
			throws SQLException, RDBMSExtractError {

		Tuple tuple = new Tuple(relation);
		for (RelationField field : relation.getFields()) {
			String fieldName = field.getName();
			try {
				tuple.set(fieldName, updateSet.getObject(fieldName));
			} catch (NameNotFound e) {
				throw new RDBMSExtractError(e);
			} catch (ValueMismatchException e) {
				throw new RDBMSExtractError(e);
			}
		}

		Update update;
		switch (op) {
		case INSERTION:
			update = new Update(null, tuple);
			break;
		case DELETION:
			update = new Update(tuple, null);
			break;
		default:
			throw new IllegalArgumentException(
					"Updates are not yet supported. SQL replication should be set to convert updates into delete/insert pairs.");
		}

		builder.addUpdate(schema, relation, update);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.upenn.cis.orchestra.extractupdates.IExtractUpdates#extractTransactions
	 * ()
	 */
	@Override
	public ILocalUpdates extractTransactions(Peer peer, Connection connection)
			throws SchemaIncoherentWithDBError, DBConnectionError,
			RDBMSExtractError {
		LocalUpdates.Builder builder = new LocalUpdates.Builder(peer);

		for (Schema s : peer.getSchemas()) {
			for (Relation relation : s.getRelations()) {
				if (!relation.isInternalRelation()) {
					try {
						processRelation(relation, s, connection, builder);
					} catch (SQLException e) {
						throw new RDBMSExtractError(e);
					}
				}
			}
		}

		return builder.buildLocalUpdates();
	}

}
