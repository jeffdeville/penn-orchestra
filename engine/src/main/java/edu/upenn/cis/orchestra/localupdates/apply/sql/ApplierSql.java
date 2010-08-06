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
import java.util.Collection;
import java.util.List;

import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.Update;
import edu.upenn.cis.orchestra.localupdates.ILocalUpdates;
import edu.upenn.cis.orchestra.localupdates.apply.IApplier;
import edu.upenn.cis.orchestra.localupdates.apply.exceptions.UpdatesNotAppliedException;

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
 * <li>If {@code tuple} is derivable, then add it to Rel_R_INS table.</li>
 * <li>If {@code tuple} exists in the Rel_L table, then add it to Rel_L_DEL.</li>
 * </ol>
 * </dd>
 * </dl>
 * 
 * @author John Frommeyer
 * 
 */
public class ApplierSql implements IApplier<Connection> {

	private final IDerivabilityCheck derivabilityChecker;

	/**
	 * A {@code IApplier<Connection>} which will use {@code checker} when
	 * necessary.
	 * 
	 */
	ApplierSql(IDerivabilityCheck checker) {
		derivabilityChecker = checker;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see edu.upenn.cis.orchestra.applyupdates.IApplyUpdates#applyUpdates(edu.upenn
	 *      .cis.orchestra.extractupdates.ILocalUpdates, T)
	 */
	@Override
	public int applyUpdates(ILocalUpdates updates, Connection connection)
			throws UpdatesNotAppliedException {
		int count = 0;
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
							IApplierStatements statements = new ApplierStatements(
									derivabilityChecker, relation, connection);
							for (Update update : updateList) {
								statements.add(update);
							}
							count = statements.apply();
						}
					}
				}
			}
		} catch (Exception e) {
			throw new UpdatesNotAppliedException(e);
		}
		return count;
	}
}
