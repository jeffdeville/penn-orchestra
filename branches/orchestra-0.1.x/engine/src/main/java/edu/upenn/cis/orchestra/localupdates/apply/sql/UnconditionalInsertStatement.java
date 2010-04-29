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
import java.sql.SQLException;

import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.Tuple;

/**
 * Represents a conditional SQL insert where the condition is always true. The
 * insert will always be applied.
 * 
 * @author John Frommeyer
 * 
 */
public class UnconditionalInsertStatement extends AConditionalInsertStatement {

	/**
	 * Creates a insert statement into {@code
	 * baseRelation.getFullQualifiedDbId() + relationType + deltaType} which
	 * will always be applied.
	 * 
	 * @param baseRelation
	 * @param relationType
	 * @param deltaType
	 * @param connection
	 * @throws SQLException
	 */
	public UnconditionalInsertStatement(Relation baseRelation,
			String relationType, String deltaType,
			@SuppressWarnings("hiding") final Connection connection)
			throws SQLException {
		super(baseRelation, relationType, deltaType, connection);
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see edu.upenn.cis.orchestra.localupdates.apply.sql.AConditionalInsertStatement#shouldInsert(edu.upenn.cis.orchestra.datamodel.Tuple)
	 */
	@Override
	protected boolean shouldInsert(Tuple tuple) throws SQLException {
		return true;
	}

}
