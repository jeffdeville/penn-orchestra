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
package edu.upenn.cis.orchestra.exchange.flatfile;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datalog.atom.Atom.AtomType;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationField;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateRelationIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnsupportedTypeException;
import edu.upenn.cis.orchestra.datamodel.iterators.ResultSetIterator;
import edu.upenn.cis.orchestra.dbms.IDb;
import edu.upenn.cis.orchestra.dbms.SqlDb;
import edu.upenn.cis.orchestra.exchange.RuleQuery;
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.reconciliation.Db;

/**
 * File system as a "database", i.e., a source of tables that can be
 * imported into SQL.
 * 
 * TODO: make it parse the rows from a file and return as tuples
 * 
 * @author zives
 *
 */
public class FileDb implements IDb {
	String m_prefix;
	Relation _relationSchema;
	char _delimiter;
	List<String> _queryBatch;
	BufferedReader _inputReader;
	String _ext;
	String _path = "";

	/**
	 * Instantiate a FileDb object to import a delimited text file
	 * using a specified schema
	 * 
	 * @param prefix
	 * @param relSchema
	 * @param delimiter
	 */
	public FileDb(String dir, String prefix, String ext, Relation relSchema, char delimiter) {
		m_prefix = prefix;
		_path = dir;
		_relationSchema = relSchema;
		_delimiter = delimiter;
		_queryBatch = new ArrayList<String>();
		_ext = ext;
	}
	
	public FileDb(String prefix, String ext) {
		m_prefix = prefix;
		_ext = ext;
		_queryBatch = new ArrayList<String>();
	}

	public String getServer() {
		return null;
	}
	
	public String getUsername() {
		return null;
	}
	
	public String getPassword() {
		return null;
	}
	
	public String getSQLImportFromFile(AtomType typ) throws java.sql.SQLException {
		String ret = m_prefix;
		
		if (typ == AtomType.DEL) {
			ret = ret + ".delete";
		} else if (typ == AtomType.INS) {
			ret = ret + ".insert";
		} else {
			throw new java.sql.SQLException("Unable to generate import command");
		}
		
		return ret;
	}
	
	public String checkImportCommand(String tableName, AtomType typ) throws java.sql.SQLException {
		return("select count(*) as cnt from " +  tableName + "_" + typ);
	}

	public String getSQLImportCommandOld(String relName, 
			List<String> attribs, AtomType typ) throws java.sql.SQLException {
		String fPath = "tests";
		String fPrefix = m_prefix;
		String ret = "IMPORT FROM ";

		String rName = SqlDb.getTableName(relName);
		
		ret = ret + "\"" + fPath + "\\" + fPrefix + "." + rName;
		if (typ == AtomType.DEL) {
			ret = ret + "_DEL";
		} else if (typ == AtomType.INS) {
			ret = ret + "_INS";
		} else {
			throw new java.sql.SQLException("Unable to generate import command");
		}
		ret = ret + "\"";
		
		String range = "";
		String attribList = "";
		int i = 0;
		for (String a: attribs) {
			if (i++ > 0) {
				range = range + ", ";
				attribList = attribList + ", ";
			}
			range = range + Integer.toString(i);
			attribList = attribList + a;
		}
		
		ret = ret + " OF DEL MODIFIED BY COLDEL| METHOD P (" + range;
		
		ret = ret + ") MESSAGES NUL";
		ret = ret + " INSERT INTO " + /*SqlDb.DBUSER + "." +*/ relName;
		if (typ == AtomType.DEL) {
			ret = ret + "_DEL";
		} else if (typ == AtomType.INS) {
			ret = ret + "_INS";
		}
		ret = ret + "(" + attribList + ")";
		
		return ret;
	}

	public String getSQLImportCommandHsqlDb(String relName, 
			List<String> attribs, List<String> types,
			AtomType typ) throws java.sql.SQLException {
//		String fPath = Config.getString("workdir");//"tests";
		String fPrefix = m_prefix;
		
		//fPath = "C:" + fPath.replace("/", "\\");

		String rName = relName;//SqlDb.getTableName(relName);
		if (typ == AtomType.DEL) {
			rName = rName + "_DEL";
		} else if (typ == AtomType.INS) {
			rName = rName + "_INS";
		}
		
		String attribList = "";
		String ln_attribList = "";
		String ln_list = "";
		String attrNames = "";
		int i = 0;
		for (String a: attribs) {
			if (i > 0) {
				attribList = attribList + ", ";
				ln_attribList = ln_attribList + ", ";
				ln_list = ln_list + ", ";
				attrNames = attrNames + ", ";
			}
			attribList = attribList + a + " " + types.get(i++);
			attrNames = attrNames + a;
			ln_attribList = ln_attribList + a + RelationField.LABELED_NULL_EXT + " INTEGER DEFAULT 1";
			ln_list = ln_list + "1";
		}
		String defn = "(" + attribList + ")";
		
		String ret = "DROP TABLE " + rName + "_TXT IF EXISTS\nCREATE TEXT TABLE ";
		ret = ret + rName + "_TXT" + defn + "\nDROP TABLE " + rName + " IF EXISTS\n";
		ret = ret + "CREATE TABLE " + rName + "(" + attribList + "," + ln_attribList + ")";
		
		ret = ret + "\nSET TABLE " + rName + "_TXT" + " SOURCE " + 
			"\"" + /*fPath + "/" +*/ fPrefix + "." + SqlDb.getTableName(rName);
//		"\"" + /*fPath + "/" +*/ fPrefix + "-1i1d" + "." + SqlDb.getTableName(rName);
		/*if (typ == AtomType.DEL) {
			ret = ret + "_DEL";
		} else if (typ == AtomType.INS) {
			ret = ret + "_INS";
		} else {
			throw new java.sql.SQLException("Unable to generate import command");
		}*/
		ret = ret + ";fs=|\"";
		
		
//		ret = ret + " OF DEL MODIFIED BY COLDEL| METHOD P (" + range;
		
//		ret = ret + ") MESSAGES NUL";
//		ret = ret + " INSERT INTO " + /*SqlDb.DBUSER + "." +*/ relName;
//		if (typ == AtomType.DEL) {
//			ret = ret + "_DEL";
//		} else if (typ == AtomType.INS) {
//			ret = ret + "_INS";
//		}
		
		ret = ret + "\nINSERT INTO " + rName + " SELECT " + attrNames + ", " + ln_list + " FROM " + rName + "_TXT";
		return ret;
	}

	public void commit() throws Exception {
	}

	public void runstats(){
		// TODO Auto-generated method stub
	}

	public void finalize() throws Exception {
	}

	/** Opens the input file (assumed to have a .table extension) */
	public boolean connect() throws Exception {
		_inputReader = new BufferedReader(new FileReader(_path + java.io.File.separator 
				+ m_prefix + "." + _ext));//".table"));

		return (_inputReader != null);
	}

	/** If necessary, close the input file */
	public void disconnect() throws Exception {
		if (_inputReader != null)
			_inputReader.close();
	}

	// Can do a No-op update, but all other updates are disallowed
	public int evaluateUpdate(String str) throws Exception {
		if (str.length() == 0)
			return 0;
		else
			throw new RuntimeException("Unable to update file!");
	}
	
	public void addToBatch(String s) throws Exception {
		_queryBatch.add(s);	
	}
	
	public int evaluateBatch() throws Exception {
		int ret = 0;
		for (String q : _queryBatch)
			ret += (evaluate(q) ? 1 : 0);
		
		return ret;
	}

	// TODO: should take empty query string and return true
	public boolean evaluate(String str) throws Exception {
		throw new RuntimeException("Unable to update file!");	
	}
	
	// TODO: should take identity rule and return the set of lines as tuples
	public ResultSetIterator<Tuple> evaluateRule(Rule r, boolean physicalLevel) {
		throw new RuntimeException("Unable to update file!");	
	}

	public RuleQuery generateQuery() {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * Import from another DB: should always fail for FileDB
	 */
	public boolean importData(IDb source, List<String> baseTables) {
		return false;
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
	}
	
	public List<Schema> getSchemas() {
		List<Schema> schemas = new ArrayList<Schema>();
		
		if (_relationSchema != null) {
			Schema s = new Schema("File" + _relationSchema.getName(), "Import for file " + m_prefix);
			try {
				s.addRelation(_relationSchema);
			} catch (DuplicateRelationIdException de) {
				de.printStackTrace();
			}
			schemas.add(s);
		}
		
		return schemas;
	}

	public void serialize(Document doc, Element db) {
		assert(false);	// unimplemented
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
	
	public boolean isConnected() {
		return false;
	}
	
	public int importRelation(IDb source, Relation baseTable, boolean replaceAll,
			Relation logicalTable, Schema s, Peer p, Db pubDb) throws Exception
	{
		return 0;
	}
	
	/**
	 * Fetches a tuple from the file, according to the Relation schema,
	 * and puts it into the ArrayList data.  The routine will initialize
	 * data with the appropriate number of elements if it is empty.
	 * 
	 * @param baseTable
	 * @param data
	 * @return
	 * @throws UnsupportedTypeException 
	 * @throws IOException 
	 */
	public boolean importNextTupleToArrayList(Relation baseTable, ArrayList<Object> data) 
	throws UnsupportedTypeException, IOException {
		String inLine = _inputReader.readLine();
		if (data.size() == 0) {
			data.ensureCapacity(baseTable.getNumCols());
		}
		int pos = 0;
		for (int i = 0; i < baseTable.getNumCols(); i++) {
			String curItem;
			// quit
			if (pos == -1) {
				return false;
			}
			
			int pos2 = inLine.indexOf(_delimiter, pos);
			if (pos2 < 0)
				curItem = inLine.substring(pos);
			else
				curItem = inLine.substring(pos, pos2);
			if (pos2 < pos)
				curItem = inLine.substring(pos);
			else if (pos2 == pos)
				curItem = "";
			else
				pos = pos2+1;
			
			switch (baseTable.getColType(i).getSqlTypeCode()) {
			case java.sql.Types.BOOLEAN:
				data.set(i, Boolean.valueOf(curItem));
				break;
			case java.sql.Types.INTEGER:
				data.set(i, Integer.valueOf(curItem));
				break;
			case java.sql.Types.CHAR:
			case java.sql.Types.VARCHAR:
			case java.sql.Types.CLOB:
				data.set(i, curItem);
				break;
			case java.sql.Types.FLOAT:
				data.set(i, Float.valueOf(curItem));
				break;
			case java.sql.Types.DOUBLE:
				data.set(i, Double.valueOf(curItem));
				break;
			case java.sql.Types.DATE:
				data.set(i, java.sql.Date.valueOf(curItem));
				break;
			default:
				throw new UnsupportedTypeException(baseTable.getColType(i).getSQLTypeName()); 
			}
		}
		
		return true;
	}
	
	public boolean initImportSource(Relation baseTable) throws Exception {
		connect();
		return true;
	}

	/**
	 * Sets the appropriate parameters in a prepared statement to match a row
	 * form the import table
	 * 
	 * @param baseTable
	 * @param statement
	 * @return True if a row was read, false otherwise
	 * 
	 * @throws UnsupportedTypeException
	 * @throws IOException
	 * @throws SQLException
	 */
	public boolean importNextTupleToPS(Relation baseTable, PreparedStatement statement) 
	throws UnsupportedTypeException, IOException, SQLException {
		String inLine = _inputReader.readLine();
		
		if (inLine == null)
			return false;
		
		int pos = 0;
		for (int i = 0; i < baseTable.getNumCols(); i++) {
			String curItem;
			// quit
			if (pos == -1) {
				return false;
			}
			
			int pos2 = inLine.indexOf(_delimiter, pos);
			if (pos2 < 0)
				curItem = inLine.substring(pos);
			else
				curItem = inLine.substring(pos, pos2);
			if (pos2 < pos)
				curItem = inLine.substring(pos);
			else if (pos2 == pos)
				curItem = "";
			else
				pos = pos2+1;
			
			switch (baseTable.getColType(i).getSqlTypeCode()) {
			case java.sql.Types.BOOLEAN:
				statement.setBoolean(i+1, Boolean.valueOf(curItem));
				break;
			case java.sql.Types.INTEGER:
				statement.setInt(i+1, Integer.valueOf(curItem));
				break;
			case java.sql.Types.CHAR:
			case java.sql.Types.VARCHAR:
				statement.setString(i+1, curItem);
				break;
			case java.sql.Types.CLOB:
				statement.setString(i+1, curItem);
				break;
			case java.sql.Types.FLOAT:
				statement.setFloat(i+1, Float.valueOf(curItem));
				break;
			case java.sql.Types.DOUBLE:
				statement.setDouble(1, Double.valueOf(curItem));
				break;
			case java.sql.Types.DATE:
				statement.setDate(i+1, java.sql.Date.valueOf(curItem));
				break;
			default:
				throw new UnsupportedTypeException(baseTable.getColType(i).getSQLTypeName()); 
			}
		}
		
		return true;
	}

	public int fetchDbTransactions(Peer p, Db store) throws Exception
	{
		return 0;
	}
	
	public void registerBuiltInSchema(Schema s) {
		
	}
	
	
	// Currently return empty
	public Map<String,Schema> getBuiltInSchemas() {
		return new HashMap<String,Schema>();
	}

	public boolean isBuiltInAtom(Atom a) {
		return false;
	}
	
	public void updateTableStatistics(String tableName, boolean detailed) throws SQLException{
		
	}

}
