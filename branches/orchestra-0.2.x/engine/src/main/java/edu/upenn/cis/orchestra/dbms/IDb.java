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
package edu.upenn.cis.orchestra.dbms;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.iterators.ResultSetIterator;
import edu.upenn.cis.orchestra.exchange.RuleQuery;
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.reconciliation.Db;

public interface IDb {
	
	public String getServer();
	
	public String getUsername();
	
	public String getPassword();
	
	/** Establish connection to database engine */
	public boolean connect() throws Exception;
	
	public boolean isConnected();
	
	/** Commit any pending transactions */
	public void commit() throws Exception;
	
	public void runstats();
	
	public void finalize() throws Exception;

	/** Disconnect from DB engine */
	public void disconnect() throws Exception;

	/** Evaluate an update string; returns number of changes */
	public int evaluateUpdate(String str) throws Exception;

	/** Add to a batch of queries */
	public void addToBatch(String str) throws Exception;
	
	/** Evaluate a query batch */
	public int evaluateBatch() throws Exception;

	/** Evaluate a single query */
	public boolean evaluate(String str) throws Exception;

	/** Re-initialize performance counters */
	public void resetCounters();
	
	/** Create a query object */
	public RuleQuery generateQuery();
	
	/** Import data from the source into this database */
	public boolean importData(IDb source, List<String> baseTables) throws Exception;
	
	/** Import data from the source into this database */
	public int importRelation(IDb source, Relation baseTable, boolean replace,
			Relation logicalTable, Schema s, Peer p, Db pubDb) throws Exception;

	/** Import the transactions from the DBMS */
	public int fetchDbTransactions(Peer p, Db store) throws Exception;

	/** Prepare an input source for reading */
	public boolean initImportSource(Relation baseTable) throws Exception;

	/** Read the next tuple into an arraylist, advance cursor */
	public boolean importNextTupleToArrayList(Relation baseTable, ArrayList<Object> data) 
	throws Exception;
	
	public long logTime();
	public long emptyTime();
	
	public List<Schema> getSchemas();

	public void serialize(Document doc, Element db);
	
	/** Evaluate a query and return results with iterator */
	public ResultSetIterator<Tuple> evalQueryRule(Rule r) throws Exception;

	/** Evaluate a query and return results with iterator */
	public List<ResultSetIterator<Tuple>> evalRuleSet(List<Rule> r, String semiringName, boolean provenanceQuery) throws Exception;
	
	/** Evaluate an update (insertion or deletion) and return count of tuples updated */
	public int evalUpdateRule(Rule rule) throws SQLException;

	public void updateTableStatistics(String tableName, boolean detailed) throws SQLException;
	
	public abstract void registerBuiltInSchema(Schema s);
	
	public abstract Map<String,Schema> getBuiltInSchemas();
	
	public abstract boolean isBuiltInAtom(Atom a);
}
