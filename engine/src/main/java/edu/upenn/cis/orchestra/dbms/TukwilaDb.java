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

import java.io.IOException;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.db.tukwila.backend.InvokeXQuery;
import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.Debug;
import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.exceptions.RelationNotFoundException;
import edu.upenn.cis.orchestra.datamodel.iterators.ResultSetIterator;
import edu.upenn.cis.orchestra.exchange.sql.SqlEngine;
import edu.upenn.cis.orchestra.exchange.RuleQuery;
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.reconciliation.Db;

public class TukwilaDb implements IDb {
	protected List<String> _tables;
	protected List<Schema> _schemas;
	protected Map<String,Schema> _builtins;
	protected String _host;
	protected int _port;

	public String getServer() {
		return _host + ":" + _port;
	}
	
	public String getUsername() {
		return null;
	}
	
	public String getPassword() {
		return null;
	}
	
	public TukwilaDb(String host, int port, List<String> tables, List<Schema> schemas) {
		_tables = tables;
		_schemas = schemas;
		_host = host;
		_port = port;
		_builtins = new HashMap<String,Schema>();
	}

	public TukwilaDb(List<String> tables, List<Schema> schemas) {
		_tables = tables;
		_schemas = schemas;
		_host = Config.getProperty("tukwilahost");
		_port = Config.getInteger("tukwilaport");
		_builtins = new HashMap<String,Schema>();
	}

    public void setAllTables(List<String> tables){
    	_tables = tables;
    }

	public void addToBatch(String str) throws Exception {
		// TODO Auto-generated method stub

	}

	public void commit() throws Exception {
		// TODO Auto-generated method stub

	}
	
	public void runstats(){
		// TODO Auto-generated method stub
	}

	public void finalize() throws Exception {
		// TODO Auto-generated method stub
	}

	public boolean connect() throws Exception {
		// TODO Auto-generated method stub
		return false;
	}
	
	public boolean isConnected() {
		return false;
	}

	public void disconnect() throws Exception {
		// TODO Auto-generated method stub

	}

	public boolean evaluate(String str) throws Exception {
		// TODO Auto-generated method stub
		return true;
	}
	
	public int evaluateBatch() throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	public int evaluateUpdate(String str) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	public RuleQuery generateQuery() {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean importData(IDb source, List<String> baseTables)
			throws Exception {
		// TODO Auto-generated method stub
		return false;
	}

	public int importRelation(IDb source, Relation baseTable, boolean replaceAll,
			Relation logicalTable, Schema s, Peer p, Db pubDb) throws Exception {
		return 0;
	}
	
	public long logTime() {
		// TODO Auto-generated method stub
		return 0;
	}
	
	public long emptyTime() {
		// TODO Auto-generated method stub
		return 0;
	}

	public void resetCounters() {
		// TODO Auto-generated method stub

	}

	public List<Schema> getSchemas() {
		// TODO Auto-generated method stub
		return null;
	}

	public int execute(String plan, boolean apply) throws IOException {
		StringWriter writer = new StringWriter();
		Debug.println("Executing query plan...");
//		Debug.println(plan);
//		if (false) {
		if (apply) {
			int ret = InvokeXQuery.sendXMLPlanFile(_host, _port, plan, writer);
//			Debug.println("RESULT:");
//			Debug.println(writer.getBuffer().toString());
			Debug.println("> Time taken: " + ret + " msec");
			return ret;
		} else {
			return 0;
		}
	}
	
	public void serialize(Document doc, Element db) {
		db.setAttribute("type", "tukwila");
		db.setAttribute("host", _host);
		db.setAttribute("port", Integer.toString(_port));
	}
	
	public static TukwilaDb deserialize(OrchestraSystem catalog, Element db) {
		String host = db.getAttribute("host");
		int port = Integer.parseInt(db.getAttribute("port"));
		List<String> tables = SqlEngine.getNamesOfAllTables(catalog, false, false, true);
		List<Schema> schemas = catalog.getAllSchemas();
		return new TukwilaDb(host, port, tables, schemas);
	}

	public ResultSetIterator<Tuple> evalQueryRule(Rule r) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public List<ResultSetIterator<Tuple>> evalRuleSet(List<Rule> r, String semiringName, boolean provenanceQuery) {
		// TODO Auto-generated method stub
		return null;
	}

	public int evalUpdateRule(Rule rule) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}
	public boolean initImportSource(Relation baseTable) throws Exception {
		return false;
	}

	public boolean importNextTupleToArrayList(Relation baseTable, ArrayList<Object> data) 
	throws Exception {
		return false;
	}

	public int fetchDbTransactions(Peer p, Db store) throws Exception
	{
		return 0;
	}
	
	public void registerBuiltInSchema(Schema s) {
		_builtins.put(s.getSchemaId(), s);
	}
	
	public Schema getBuiltInSchema(String s) {
		return _builtins.get(s);
	}
	
	public Map<String,Schema> getBuiltInSchemas() {
		return _builtins;
	}

	public boolean isBuiltInAtom(Atom a) {
		if (!_builtins.containsKey(a.getSchema().getSchemaId()))
			return false;
		else
			try {
				_builtins.get(a.getSchema().getSchemaId()).getRelation(a.getRelation().getName());
				return true;
			} catch (RelationNotFoundException rne) {
				return false;
			}
	}
	
	public void updateTableStatistics(String tableName, boolean detailed) throws SQLException{
		
	}
}

