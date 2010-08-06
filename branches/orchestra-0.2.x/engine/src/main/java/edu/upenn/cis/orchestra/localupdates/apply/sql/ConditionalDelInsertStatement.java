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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.sql.ISqlSelect;

/**
 * If a tuple is found in the {@code relationType} version of {@code
 * baseRelation}, then it is inserted into the {@code Relation.DELETE} version.
 * 
 * @author John Frommeyer
 * 
 */
public class ConditionalDelInsertStatement extends AConditionalInsertStatement {
	private final CountSqlSelect sqlSelect = new CountSqlSelect();
	private final String countTableName;

	/**
	 * Creates a {@code AConditionalInsertStatement} which will insert a tuple
	 * into {@code baseRelation + relationType + Relation.DELETE} if it is in
	 * {@code baseRelation + relationType}.
	 * 
	 * @param baseRelation
	 * @param relationType either {@code Relation.LOCAL} or {@code
	 *            Relation.REJECT}
	 * @param connection
	 * @throws SQLException
	 */
	ConditionalDelInsertStatement(Relation baseRelation, String relationType,
			@SuppressWarnings("hiding") final Connection connection)
			throws SQLException {
		super(baseRelation, relationType, Relation.DELETE, connection);
		countTableName = baseRelation.getFullQualifiedDbId() + relationType;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @throws SQLException
	 * 
	 * @see edu.upenn.cis.orchestra.localupdates.apply.sql.AConditionalInsertStatement#shouldInsert(edu.upenn.cis.orchestra.datamodel.Tuple)
	 */
	@Override
	protected boolean shouldInsert(Tuple tuple) throws Exception {
		Statement countStatement = connection.createStatement();
		ISqlSelect countSelect = sqlSelect.getSqlSelect(countTableName, tuple);
		ResultSet foundInTableResult = countStatement.executeQuery(countSelect
				.toString());
		foundInTableResult.next();
		int count = foundInTableResult.getInt(1);
		return count > 0;
	}

}
