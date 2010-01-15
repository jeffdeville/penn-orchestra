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
package edu.upenn.cis.orchestra.exchange;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.RelationContext;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.iterators.ResultSetIterator;
import edu.upenn.cis.orchestra.dbms.IDb;
import edu.upenn.cis.orchestra.mappings.Rule;

/**
 * Basic interface to a mapping engine.
 * 
 * @author zives, gkarvoun
 *
 */
public interface IEngine {
	public void migrate () throws Exception;
	public void createBaseSchemaRelations() throws Exception;
	public void dropAllTables() throws Exception;
	public void clearAllTables() throws Exception;
	public void copyBaseTables() throws Exception;
	public void compareBaseTablesWithCopies() throws Exception;
	public void subtractLInsDel() throws Exception;	
	
	public List<Rule> computeTranslationRules() throws Exception;
	public void computeDeltaRules();
	
	public List<Rule> getTranslationRules();
	//public DeltaRules getDeltaRules();

	/**
	 * Fetches any updates from the peers using the sourceDb
	 * 
	 * @param sourceDb Database to which updates were applied
	 * @param d
	 * @param dao
	 * @param system
	 * @throws Exception
	 * @deprecated
	 */
	public void importUpdates(IDb sourceDb) throws Exception;
	
	
	/**
	 * Fetches updates from the specified directory
	 * @param specificPeer A specific peer to import (null = all)
	 * @param dir Source directory
	 * @param succeeded RETURNS successful imports
	 * @param failed RETURNS failed imports
	 */
	public void importUpdates(Peer specificPeer, String dir, ArrayList<String> succeeded,
			ArrayList<String> failed) throws IOException, SQLException;
	
	/**
	 * Map a set of updates.
	 * 
	 * @param dr Delta rules used to do the mapping
	 */
	public long mapUpdates(int lastRec, int recno, Peer reconciler, boolean insFirst) throws Exception;
	
	/**
	 * Shut down an Orchestra mapping environment, leaving auxiliary structures
	 *
	 */
	public void close() throws Exception;
	
	/**
	 * Clear an Orchestra mapping environment, resetting everything
	 * @param d
	 * @param dao
	 * @param system
	 * @throws Exception
	 */
	public void reset() throws Exception;
	
	public void softReset() throws Exception;
	
	public void clean() throws Exception;
	
	public CreateProvenanceStorage getProvenancePrepInfo();
	
	public List<RelationContext> getMappingRelations();
	
	public void serialize(Document doc, Element el);
	
	public IDb getMappingDb();
//	public IDb getUpdateDb();

	/** Evaluate a query and return results with iterator */
	public ResultSetIterator<Tuple> evalQueryRule(Rule r) throws Exception;
	
	/** Evaluate an update (insertion or deletion) and return count of tuples updated */
	public int evalUpdateRule(Rule rule) throws Exception;
}
