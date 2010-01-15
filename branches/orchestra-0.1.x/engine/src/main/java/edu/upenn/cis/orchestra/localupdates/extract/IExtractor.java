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
package edu.upenn.cis.orchestra.localupdates.extract;

import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.localupdates.ILocalUpdates;
import edu.upenn.cis.orchestra.localupdates.extract.exceptions.DBConnectionError;
import edu.upenn.cis.orchestra.localupdates.extract.exceptions.RDBMSExtractError;
import edu.upenn.cis.orchestra.localupdates.extract.exceptions.SchemaIncoherentWithDBError;

/**
 * {@code IExtractor} is the interface that will be implemented for each RDBMS
 * system supported by Orchestra. It has been developed to extract updates from
 * a database and send these updates to the Orchestra system.
 * 
 *
 * @author Olivier Biton, John Frommeyer
 * @param <T> Intended to represent a connection class for dealing with the underlying database.
 *
 */
public interface IExtractor<T> {

	/**
	 * Extract all transactions (and updates) in the
	 * database.
	 * @param peer
	 * @param connection 
	 * @return the local updates for this {@code IExtractor}
	 * @throws SchemaIncoherentWithDBError If a relation or column declared in
	 *             the schema can't be find in the database updates
	 * @throws DBConnectionError If the system was not able to connect to the DB
	 * @throws RDBMSExtractError If the RDBMS returned an exception while
	 *             extracting updates
	 */
	public ILocalUpdates extractTransactions(Peer peer, T connection)
			throws SchemaIncoherentWithDBError, DBConnectionError,
			RDBMSExtractError;

}
