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
 * If a tuple is found to be derivable, then it is inserted into the {@code
 * Relation.REJECT}, {@code Relation.INSERT} version of {@code baseRelation}.
 * 
 * @author John Frommeyer
 * 
 */
public class InsertIfDerivableStatement extends AConditionalInsertStatement {

	private final IDerivabilityCheck derivabilityChecker;

	/**
	 * Creates a {@code AConditionalInsertStatement} which will insert a tuple
	 * into {@code baseRelation + RELATION.REJECT + Relation.INSERT} if {@code
	 * derivabilityChecker} finds that it is derivable.
	 * 
	 * @param derivabilityChecker
	 * @param baseRelation
	 * @param connection
	 * @throws SQLException
	 */
	public InsertIfDerivableStatement(
			@SuppressWarnings("hiding") final IDerivabilityCheck derivabilityChecker,
			Relation baseRelation,
			@SuppressWarnings("hiding") final Connection connection)
			throws SQLException {
		super(baseRelation, Relation.REJECT, Relation.INSERT, connection);
		this.derivabilityChecker = derivabilityChecker;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see edu.upenn.cis.orchestra.localupdates.apply.sql.AConditionalInsertStatement#shouldInsert(edu.upenn.cis.orchestra.datamodel.Tuple)
	 */
	@Override
	protected boolean shouldInsert(Tuple tuple) throws Exception {
		return derivabilityChecker.isDerivable(tuple, connection);
	}

}
