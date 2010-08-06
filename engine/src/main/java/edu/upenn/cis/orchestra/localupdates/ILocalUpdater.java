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
package edu.upenn.cis.orchestra.localupdates;

import java.util.List;

import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.dbms.IDb;
import edu.upenn.cis.orchestra.localupdates.exceptions.LocalUpdatesException;
import edu.upenn.cis.orchestra.localupdates.extract.exceptions.DBConnectionError;

/**
 * Implementations of this interface are intended to extract and apply any new
 * local updates to the peer.
 * 
 * @author John Frommeyer
 * 
 */
public interface ILocalUpdater {

	/**
	 * Extracts and applies any new local updates to {@code peer}.
	 * 
	 * @param peer
	 * @throws DBConnectionError
	 * @throws LocalUpdatesException
	 */
	public void extractAndApplyLocalUpdates(Peer peer)
			throws DBConnectionError, LocalUpdatesException;
	
	/**
	 * Called once during Migration so that any required setup can be done.
	 * 
	 * @param db the database where the setup needs to be done.
	 * @param relations the relations for which setup needs to be done.
	 * 
	 */
	public void prepare(IDb db, List<? extends Relation> relations);
	
	/**
	 * This method will be called after update exchange or reconciliation.
	 * 
	 * @param db
	 * @param relations
	 */
	public void postReconcileHook(IDb db, List<? extends Relation> relations);
}
