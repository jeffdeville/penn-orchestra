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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.Update;

/**
 * Insertion statements required by {@code ApplierSql}.
 * 
 * In this implementation, the basic rules followed are:
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
 * <li>If {@code tuple} is derivable, then add it to Rel_R_INS table.</li>
 * <li>If {@code tuple} exists in the Rel_L table, then add it to Rel_L_DEL.</li>
 * </ol>
 * </dd>
 * </dl>
 * 
 * @author John Frommeyer
 * 
 */
public class ApplierStatements implements IApplierStatements {

	private final AConditionalInsertStatement rDelIfRejectedForInsertions;
	private final AConditionalInsertStatement lInsForInsertions;
	private final AConditionalInsertStatement lDelIfLocalForDeletions;
	private final AConditionalInsertStatement rInsIfDerivableForDeletions;

	private static final Logger logger = LoggerFactory
			.getLogger(ApplierStatements.class);

	ApplierStatements(IDerivabilityCheck derivabilityChecker,
			Relation relation, Connection connection) throws SQLException {
		// Any insertion into relation triggers an insert into relation_L_INS
		lInsForInsertions = new UnconditionalInsertStatement(relation,
				Relation.LOCAL, Relation.INSERT, connection);
		// If a tuple inserted into relation exists in relation_R, then it
		// triggers an insert into relation_R_DEL
		rDelIfRejectedForInsertions = new ConditionalDelInsertStatement(
				relation, Relation.REJECT, connection);
		// If a deleted tuple from relation is derivable, then it triggers an
		// insert into relation_R_INS
		rInsIfDerivableForDeletions = new InsertIfDerivableStatement(
				derivabilityChecker, relation, connection);
		// If a deleted tuple from relation exists in relation_L, then it
		// triggers an insert into relation_L_DEL
		lDelIfLocalForDeletions = new ConditionalDelInsertStatement(relation,
				Relation.LOCAL, connection);
	}

	private void addDeletion(Tuple tuple) throws Exception {
		rInsIfDerivableForDeletions.handleTuple(tuple);
		lDelIfLocalForDeletions.handleTuple(tuple);
	}

	private void addInsertion(Tuple tuple) throws Exception {
		lInsForInsertions.handleTuple(tuple);
		rDelIfRejectedForInsertions.handleTuple(tuple);
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @throws SQLException
	 * 
	 * @see edu.upenn.cis.orchestra.localupdates.apply.sql.IApplierStatements#apply()
	 */
	@Override
	public int apply() throws SQLException {
		int result = rInsIfDerivableForDeletions.apply();
		logger.debug("{} resulted in {} updates.", rInsIfDerivableForDeletions,
				Integer.valueOf((result)));
		int total = result;

		result = lDelIfLocalForDeletions.apply();
		logger.debug("{} resulted in {} updates.", lDelIfLocalForDeletions,
				Integer.valueOf((result)));
		total += result;

		result = lInsForInsertions.apply();
		logger.debug("{} resulted in {} updates.", lInsForInsertions, Integer
				.valueOf((result)));
		total += result;

		result = rDelIfRejectedForInsertions.apply();
		logger.debug("{} resulted in {} updates.", rDelIfRejectedForInsertions,
				Integer.valueOf((result)));
		total += result;

		return total;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see edu.upenn.cis.orchestra.localupdates.apply.sql.IApplierStatements#add(edu.upenn.cis.orchestra.datamodel.Update)
	 */
	@Override
	public void add(Update update) throws Exception {
		if (update.isInsertion()) {
			Tuple insertedTuple = update.getNewVal();
			addInsertion(insertedTuple);
		} else if (update.isDeletion()) {
			Tuple deletedTuple = update.getOldVal();
			addDeletion(deletedTuple);
		} else {
			throw new IllegalStateException(
					"Cannot process update: "
							+ update.toString()
							+ ". Updates should be handled as deletion/insertion pairs.");
		}

	}

}
