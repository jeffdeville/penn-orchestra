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
package edu.upenn.cis.orchestra.localupdates.extract.sql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import edu.upenn.cis.orchestra.localupdates.LocalUpdates.Builder;
import edu.upenn.cis.orchestra.localupdates.extract.IExtractor;
import edu.upenn.cis.orchestra.localupdates.extract.exceptions.DBConnectionError;
import edu.upenn.cis.orchestra.localupdates.extract.exceptions.RDBMSExtractError;
import edu.upenn.cis.orchestra.localupdates.extract.exceptions.SchemaIncoherentWithDBError;
import edu.upenn.cis.orchestra.sql.ISqlFactory;
import edu.upenn.cis.orchestra.sql.ISqlSelect;
import edu.upenn.cis.orchestra.sql.SqlFactories;
import edu.upenn.cis.orchestra.sql.ISqlExpression.Code;

/**
 * This will eventually be a pure SQL implementation of update extraction.
 * 
 * @author John Frommeyer
 * 
 */
public class ExtractorDefault implements IExtractor<Connection> {

	private static enum Operation {
		INSERTION, DELETION, UPDATE;
	}

	private final ISqlFactory sqlFactory = SqlFactories.getSqlFactory();
	private final Logger logger = LoggerFactory.getLogger(getClass());


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
		// Not yet implemented.
		LocalUpdates.Builder builder = new LocalUpdates.Builder(peer);
		for (Schema s : peer.getSchemas()) {
			for (Relation relation : s.getRelations()) {
				if (!relation.isInternalRelation()) {
					try {
						processRelation(relation, Operation.DELETION, s, connection, builder);
						processRelation(relation, Operation.INSERTION, s, connection, builder);
					} catch (SQLException e) {
						throw new RDBMSExtractError(e);
					}
				}
			}
		}
		return builder.buildLocalUpdates();
	}

	
	private void processRelation(Relation relation, Operation op, Schema s,
			Connection connection, Builder builder) throws SQLException,
			RDBMSExtractError {
		ISqlSelect diff = operationToSql(relation, op);
		logger.debug("Sql for {}: {}", op, diff);
		Statement statement = null;
		try {
			statement = connection.createStatement();
			ResultSet updateSet = statement.executeQuery(diff
					.toString());
			while (updateSet.next()) {
				extractUpdate(s, relation, updateSet, op,
						builder);
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

	private ISqlSelect operationToSql(Relation relation, Operation op) {
		final String prevName = relation.getDbSchema() + "."
				+ relation.getDbRelName() + "_PREV";
		final String baseName = relation.getFullQualifiedDbId();
		String firstName, secondName;
		switch (op) {
		case INSERTION:
			firstName = baseName;
			secondName = prevName;
			break;
		case DELETION:
			firstName = prevName;
			secondName = baseName;
			break;
		default:
			throw new IllegalArgumentException(
					"Updates are not yet supported. Updates should be represented as delete/insert pairs.");
		}

		ISqlSelect firstMinusSecond = sqlFactory.newSelect(sqlFactory
				.newSelectItem("*"), sqlFactory.newFromItem(firstName));

		ISqlSelect minusSecond = sqlFactory.newSelect(sqlFactory
				.newSelectItem("*"), sqlFactory.newFromItem(secondName));
		firstMinusSecond.addSet(sqlFactory.newExpression(Code.EXCEPT,
				minusSecond));
		return firstMinusSecond;
	}

}
