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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.localupdates.ILocalUpdates;
import edu.upenn.cis.orchestra.localupdates.LocalUpdates;
import edu.upenn.cis.orchestra.localupdates.extract.IExtractor;
import edu.upenn.cis.orchestra.localupdates.extract.exceptions.DBConnectionError;
import edu.upenn.cis.orchestra.localupdates.extract.exceptions.RDBMSExtractError;
import edu.upenn.cis.orchestra.localupdates.extract.exceptions.SchemaIncoherentWithDBError;

/**
 * This will eventually be a pure SQL implementation of update extraction.
 * 
 * @author John Frommeyer
 * 
 */
public class ExtractorDefault implements IExtractor<Connection> {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	/**
	 * Creates the default SQL "diff" extractor.
	 * 
	 * @param peer
	 */
	public ExtractorDefault() {
		logger.debug("Using default update extractor. Not yet implemented.");
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
		// Not yet implemented.
		LocalUpdates.Builder builder = new LocalUpdates.Builder(peer);
		return builder.buildLocalUpdates();
	}

}
