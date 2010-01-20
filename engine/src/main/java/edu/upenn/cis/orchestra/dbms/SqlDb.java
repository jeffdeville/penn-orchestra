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

import static edu.upenn.cis.orchestra.OrchestraUtil.newArrayList;

import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ProxySelector;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Vector;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.Debug;
import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datalog.atom.AtomArgument;
import edu.upenn.cis.orchestra.datalog.atom.AtomConst;
import edu.upenn.cis.orchestra.datalog.atom.AtomSkolem;
import edu.upenn.cis.orchestra.datalog.atom.AtomVariable;
import edu.upenn.cis.orchestra.datalog.atom.Atom.AtomType;
import edu.upenn.cis.orchestra.datamodel.AbstractRelation;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationContext;
import edu.upenn.cis.orchestra.datamodel.RelationField;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.StringType;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.datamodel.Type;
import edu.upenn.cis.orchestra.datamodel.Update;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateRelationIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.RelationNotFoundException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnsupportedTypeException;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.datamodel.iterators.IteratorException;
import edu.upenn.cis.orchestra.datamodel.iterators.ResultIterator;
import edu.upenn.cis.orchestra.datamodel.iterators.ResultSetIterator;
import edu.upenn.cis.orchestra.dbms.sql.generation.ISqlStatementGen;
import edu.upenn.cis.orchestra.dbms.sql.generation.SqlRuleQuery;
import edu.upenn.cis.orchestra.dbms.sql.generation.SqlStatementGenFactory;
import edu.upenn.cis.orchestra.exchange.RuleQuery;
import edu.upenn.cis.orchestra.exchange.flatfile.FileDb;
import edu.upenn.cis.orchestra.exchange.sql.SqlEngine;
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.provenance.ProvenanceNode;
import edu.upenn.cis.orchestra.provenance.ProvenanceRelation;
import edu.upenn.cis.orchestra.reconciliation.ClientCentricDb;
import edu.upenn.cis.orchestra.reconciliation.Db;
import edu.upenn.cis.orchestra.reconciliation.DbException;
import edu.upenn.cis.orchestra.sql.ISqlColumnDef;
import edu.upenn.cis.orchestra.sql.ISqlExpression;
import edu.upenn.cis.orchestra.sql.ISqlFactory;
import edu.upenn.cis.orchestra.sql.ISqlInsert;
import edu.upenn.cis.orchestra.sql.ISqlSelect;
import edu.upenn.cis.orchestra.sql.ISqlSelectItem;
import edu.upenn.cis.orchestra.sql.ISqlStatement;
import edu.upenn.cis.orchestra.sql.SqlFactories;
import edu.upenn.cis.orchestra.sql.ISqlExpression.Code;

/**
 * SQL interface class to DB2 or other databases. Refactored from other code and
 * modularized.
 * 
 * @author zives, gkarvoun
 * 
 */
public class SqlDb implements IDb {
	private static final Method resultSetIsClosed;
	static {
		Method temp;
		try {
			Class<ResultSet> c = ResultSet.class;
			temp = c.getDeclaredMethod("isClosed");
		} catch (NoSuchMethodException nsme) {
			temp = null;
		}
		resultSetIsClosed = temp;
	}

	private Log _log = LogFactory.getLog(getClass());

	/**
	 * Queueing and SQL application class for updates
	 * 
	 * @author zives, gkarvoun
	 * 
	 */
	public class BatchInsert {
		Map<Relation, List<Update>> _tList;
		public static final int MAX_QUEUE = 1000;

		BatchInsert() {
			_tList = new HashMap<Relation, List<Update>>();
		}

		/**
		 * Adds an update to the queue associated with the specified relation.
		 * If the queue size exceeds MAX_QUEUE, we will apply the updates in the
		 * queue.
		 */
		public void add(Relation r, Update u) throws SQLException,
		UnsupportedTypeException {
			List<Update> ul = _tList.get(r);

			if (ul == null) {
				_tList.put(r, new ArrayList<Update>());
				ul = _tList.get(r);
			}

			ul.add(u);

			// If we've exceeded the queue size, apply
			// all of the updates and reset
			if (ul.size() > MAX_QUEUE) {
				apply(r);

				ul.clear();
			}
		}

		/**
		 * Apply all of the pending updates
		 * 
		 * @return Count of updates applied
		 * @throws SQLException
		 */
		public int applyAll() throws SQLException, UnsupportedTypeException {
			int count = 0;
			for (Relation r : _tList.keySet()) {
				count += apply(r);
			}
			return count;
		}

		/**
		 * Takes the tuple and populates the prepared statement using it.
		 * Considers labeled nulls.
		 * 
		 * @param t
		 * @param ps
		 */
		private void populateTuple(Tuple t, PreparedStatement ps)
		throws SQLException, UnsupportedTypeException {
			int firstBase = 1; // First column inx of base attributes
			int firstLN = t.getNumCols() + 1; // First column inx of labeled
			// null attrs
			for (int i = 0; i < t.getNumCols(); i++) {
				if (t.isLabeledNull(i)) {
					// Null out the non-LN attribute
					ps.setNull(firstBase + i, t.getSchema().getColType(i)
							.getSqlTypeCode());

					// Put the null code into the LN attribute
					ps.setInt(firstLN + i, t.getLabeledNull(i));
				} else {
					// Not a labeled null
					ps.setInt(firstLN + i, 1);

					setPreparedStatementData(ps, i, t.getSchema().getColType(i)
							.getSqlTypeCode(), t.get(i));
				}
			}
		}

		/**
		 * Takes the queued-up list of updated related to the Relation and
		 * applies them as SQL insertions.
		 * 
		 * @param r
		 * @return
		 * @throws SQLException
		 */
		public int apply(Relation r) throws SQLException,
		UnsupportedTypeException {

			List<Update> ul = _tList.get(r);

			String prepIns = "INSERT INTO " + r.getFullQualifiedDbId()
			+ Relation.LOCAL + "_INS (";

			boolean first = true;
			for (int i = 0; i < r.getNumCols(); i++) {
				prepIns += ((first) ? "" : ",") + r.getColName(i);
				first = false;
			}
			if(r.hasLabeledNulls()){
				for (int i = 0; i < r.getNumCols(); i++) {
					if (!Config.useCompactNulls() || r.isNullable(i))
						prepIns += "," + r.getColName(i) + RelationField.LABELED_NULL_EXT;
				}
			}
			prepIns += ") VALUES (";
			first = true;
			for (int i = 0; i < r.getNumCols() * 2; i++) {
				prepIns += ((first) ? "" : ",") + "?";
				first = false;
			}
			prepIns += ")";

			System.out.println(prepIns);

			PreparedStatement ins = null, del = null;
			try {
				ins = _con.prepareStatement(prepIns);

				/*
				 * String prepDel = "DELETE FROM " + r.getFullQualifiedDbId() + "
				 * WHERE ";
				 * 
				 * first = true; for (int i = 0; i < r.getNumCols() * 2; i++) {
				 * prepDel += ((first) ? "" : " AND ") + r.getColName(i) + " =
				 * ?"; first = false; }
				 */
				String prepDel = "INSERT INTO " + r.getFullQualifiedDbId()
				+ Relation.LOCAL + "_DEL (";

				first = true;
				for (int i = 0; i < r.getNumCols(); i++) {
					prepDel += ((first) ? "" : ",") + r.getColName(i);
					first = false;
				}
				if(r.hasLabeledNulls()){
					for (int i = 0; i < r.getNumCols(); i++) {
						if (!Config.useCompactNulls() || r.isNullable(i))
							prepDel += "," + r.getColName(i) + RelationField.LABELED_NULL_EXT;
					}
				}
				prepDel += ") VALUES (";
				first = true;
				for (int i = 0; i < r.getNumCols() * 2; i++) {
					prepDel += ((first) ? "" : ",") + "?";
					first = false;
				}
				prepDel += ")";

				System.out.println(prepDel);
				del = _con.prepareStatement(prepDel);

				// Translate the updates, one at a time, filling in _LN
				// values as necessary

				for (Update u : ul) {
					if (u.isDeletion()) {
						// Add the parameters
						populateTuple(u.getOldVal(), del);

						del.addBatch();
					} else if (u.isInsertion()) {
						// Add the parameters
						populateTuple(u.getNewVal(), ins);

						ins.addBatch();
					}
				}
				int[] count1 = del.executeBatch();

				int count = 0;
				for (int i = 0; i < count1.length; i++)
					count += count1[i];
				System.out.println("Applied " + count + " deletions to "
						+ r.getName());

				count1 = ins.executeBatch();

				count = 0;
				for (int i = 0; i < count1.length; i++)
					count += count1[i];
				System.out.println("Applied " + count + " insertions to "
						+ r.getName());

				return 0;
			} finally {
				if (null != ins) {
					ins.close();
				}
				if (null != del) {
					del.close();
				}
			}
		}
	}

	private class StreamReaderThread extends Thread {
		StringBuffer mOut;
		InputStreamReader mIn;

		public StreamReaderThread(InputStream in, StringBuffer out) {
			mOut = out;
			mIn = new InputStreamReader(in);
		}

		public void run() {
			int ch;
			try {
				while (-1 != (ch = mIn.read()))
					mOut.append((char) ch);
			} catch (Exception e) {
				mOut.append("\nRead error:" + e.getMessage());
			}
		}
	}

	public static final int TRANSACTION_CUTOFF = 1000000;
	public static final int QUERY_CUTOFF = 500;
	//	public static final int QUERY_CUTOFF = Config.getQueryCutoff();;
	//	public static final int TRANSACTION_CUTOFF = 50000;
	//	public static final int QUERY_CUTOFF = 20;
	public static final int MAX_INDEX_COLS = 64;

	// public static /*final*/ String DRIVER2 = "com.ibm.db2.jcc.DB2Driver";
	// public static /*final*/ String DRIVER = "COM.ibm.db2.jdbc.app.DB2Driver";

	// public static /*final*/ String DRIVER = "com.ibm.db2.jcc.DB2Driver";
	// public static /*final*/ String DRIVER2 =
	// "COM.ibm.db2.jdbc.app.DB2Driver";
	public static/* final */String DRIVER = Config.getJDBCDriver();

	protected Connection _con;
	protected Statement _stmt;
	private HashMap<Statement, ResultSet> _statementResults;

	protected Runtime _rt = Runtime.getRuntime();

	protected DataSource _ds;
	protected boolean isMigrated = false;

	public static int numUnionedQueries = 1;
	public static int preparedStmtCnt = 0;
	public static int numPrepareCalls = 0;
	protected int totalTupleCnt = 0;
	protected int transactionCnt = 0;
	public int queryCnt = 0;
	protected int totalQueryCnt = 0;
	// protected String slowestQuery;
	protected int slowestQuery;
	protected long slowestQueryTime = 0;
	public long time4CommitLogging = 0;
	public long time4EmptyChecking = 0;

	private OrchestraSystem _system;
	protected List<String> _allTables;
	protected List<Schema> _schemas;

	protected String _server;
	protected String _username;
	protected String _password;

	protected Map<String, Schema> _builtins;

	protected boolean didUpdate = false;

	protected ISqlStatementGen _sqlString = null;
	
	private ISqlFactory _sqlFactory = SqlFactories.getSqlFactory();

	public SqlDb(String server, String dbuser, String pwd,
			List<String> allTables, List<Schema> schemas, OrchestraSystem system, Map<String, Schema> builtInSchemas) {
		_allTables = allTables;
		_schemas = schemas;
		_server = server;
		_username = dbuser;
		_password = pwd;

		_builtins = builtInSchemas;
		_sqlString = SqlStatementGenFactory.createStatementGenerator();
		_system = system;
	}

	public SqlDb(List<String> allTables, List<Schema> schemas, OrchestraSystem system) {
		_allTables = allTables;
		_schemas = schemas;
		_server = Config.getSQLServer();
		_username = Config.getUser();
		_password = Config.getPassword();
		_builtins = new HashMap<String, Schema>();
		_system = system;
	}

	public ISqlStatementGen getSqlTranslator() {
		return _sqlString;
	}

	public void setReadOnlyTransactions(Connection con) throws SQLException {
		if (Config.isDB2())
			con
			.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);

	}

	public String getServer() {
		return _server;
	}

	public String getUsername() {
		return _username;
	}

	public String getPassword() {
		return _password;
	}

	public void initKeepStmt() {
		// numPrepareCalls = 0;
		try {
			if (Config.getAutocommit() == false) {
				_con.setAutoCommit(false);
			} else {
				_con.setAutoCommit(true);
			}

			activateRuleBasedOptimizer();
			// _stmt = _con.createStatement();
			// Debug.println("db2cmd /c /w /i db2 connect to " + DBNAME + " USER
			// " + DBUSER + " USING " + DBPWD);
			// _rt.exec("db2cmd /c /w /i db2 connect to " + DBNAME + " USER " +
			// DBUSER + " USING " + DBPWD);
		} catch (java.lang.Exception ex) {
			ex.printStackTrace();
		}
		SingleConnectionDataSource ds = new SingleConnectionDataSource();
		ds.setDriverClassName(SqlDb.DRIVER);
		ds.setUrl(_server);
		ds.setUsername(_username);
		ds.setPassword(_password);

		_ds = ds;
		_statementResults = new HashMap<Statement, ResultSet>();
	}

	public void init() {
		numPrepareCalls = 0;
		if (Config.getApply()) {
			try {
				if (Config.getAutocommit() == false) {
					_con.setAutoCommit(false);
				} else {
					_con.setAutoCommit(true);
				}

				setReadOnlyTransactions(_con);

				_stmt = _con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
						ResultSet.CONCUR_READ_ONLY);
				// _stmt = _con.createStatement(ResultSet.TYPE_FORWARD_ONLY,
				// ResultSet.CONCUR_READ_ONLY);

				activateRuleBasedOptimizer();

				// Debug.println("db2cmd /c /w /i db2 connect to " + DBNAME + "
				// USER " + DBUSER + " USING " + DBPWD);
				// _rt.exec("db2cmd /c /w /i db2 connect to " + DBNAME + " USER
				// " + DBUSER + " USING " + DBPWD);
			} catch (java.lang.Exception ex) {
				ex.printStackTrace();
			}
			SingleConnectionDataSource ds = new SingleConnectionDataSource();
			ds.setDriverClassName(Config.getJDBCDriver());
			ds.setUrl(_server);
			ds.setUsername(_username);
			ds.setPassword(_password);

			_ds = ds;
			_statementResults = new HashMap<Statement, ResultSet>();
			try {
				setReadOnlyTransactions(ds.getConnection());

			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public void initDS() {
		try {
			SingleConnectionDataSource ds = new SingleConnectionDataSource();
			ds.setDriverClassName(Config.getJDBCDriver());
			ds.setUrl(_server);
			ds.setUsername(_username);
			ds.setPassword(_password);

			_ds = ds;
			_statementResults = new HashMap<Statement, ResultSet>();

			setReadOnlyTransactions(ds.getConnection());

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void closeDS() {
		try {
			_ds.getConnection().close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public boolean isConnected() {
		return _con != null;
	}

	public void commit() throws SQLException {
		if (Config.getAutocommit() == false) {
			runStatsOnAllTables(_system);
			_con.commit();
			_ds.getConnection().commit();
			turnOffLoggingAndResetStats();
		}
	}

	public void finalize() throws SQLException {
		if (Config.getAutocommit() == false) {
			_con.commit();
			_ds.getConnection().commit();
		}
	}

	protected Statement getStatement() {
		return _stmt;
	}

	protected Statement getNewStatement() throws SQLException {
		for (Statement s : _statementResults.keySet()) {
			if (_statementResults.get(s) == null) {
				return s;
			}
			// Perform this method dynamically, since it doesn't exist in
			// Java 1.5. Once everybody is using Java 1.6, this will no longer
			// be necessary.
			if (resultSetIsClosed != null) {
				try {
					if ((Boolean) resultSetIsClosed.invoke(_statementResults
							.get(s))) {
						return s;
					}
				} catch (IllegalArgumentException e) {
					throw new RuntimeException(e);
				} catch (IllegalAccessException e) {
					throw new RuntimeException(e);
				} catch (InvocationTargetException e) {
					throw new RuntimeException(e);
				}
			}
			try {
				if (_statementResults.get(s).isAfterLast()) {
					return s;
				}
			} catch (SQLException e) {
				// Probably because result set is closed
			}

		}

		// Statement n = _con.createStatement(ResultSet.TYPE_FORWARD_ONLY,
		// ResultSet.CONCUR_READ_ONLY);
		Statement n = _con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
				ResultSet.CONCUR_READ_ONLY);
		_statementResults.put(n, null);
		return n;
	}

	protected void bindStatement(Statement s, ResultSet r) {
		_statementResults.put(s, r);
	}

	public PreparedStatement createPrepared(String stmt) throws SQLException {
		return _con.prepareStatement(stmt);
	}

	public RuleQuery generateQuery() {
		return new SqlRuleQuery(this);
	}

	public void resetCounters() {
		Debug.println("TOTAL QUERY COUNT: " + totalQueryCnt);
		Debug.println("TOTAL PREPARED STMT COUNT: " + preparedStmtCnt);
		Debug.println("PREPARE CALLS: " + numPrepareCalls);
		Debug.println("SLOWEST QUERY: " + slowestQuery);
		Debug.println("SLOWEST QUERY TIME: " + slowestQueryTime + " msec");
		System.out.println("TOTAL TUPLE COUNT: " + totalTupleCnt);
		Debug.println("LOGGING TIME: " + time4CommitLogging + " msec");
		totalQueryCnt = 0;
		slowestQueryTime = 0;
		// slowestQuery = null;
		slowestQuery = 0;
		transactionCnt = 0;
		totalTupleCnt = 0;
		System.out.println("TRANSACTION CNT BEFORE RESET WAS: " + transactionCnt);
		System.out.println("QUERY CNT BEFORE RESET WAS: " + queryCnt);
		transactionCnt = 0;
		queryCnt = 0;
		time4CommitLogging = 0;
		time4EmptyChecking = 0;
		preparedStmtCnt = 0;
	}

	public void disconnect() {
		if (Config.getApply() && _con != null) {
			try {
				_stmt.close();
				if (!Config.getAutocommit())
					_con.commit();
				_con.close();
				closeDS();
				_con = null;
				// _rt.exec("db2cmd /c /w /i db2 disconnect all");
			} catch (java.lang.Exception ex) {
				ex.printStackTrace();
			}
		}
	}

	public void disconnectKeepStmt() {
		if (Config.getApply()) {
			try {
				// _stmt.close();
				_con.commit();
				_con.close();
				// _rt.exec("db2cmd /c /w /i db2 disconnect all");
			} catch (java.lang.Exception ex) {
				ex.printStackTrace();
			}
		}
	}

	public boolean connect() throws ClassNotFoundException, SQLException {
		if (Config.getApply()) {
			// Load the JDBC-ODBC bridge
			ProxySelector.setDefault(null);
			Class.forName(Config.getJDBCDriver());
			_con = DriverManager.getConnection(_server, _username, _password);

			setReadOnlyTransactions(_con);

			init();
			return (_con != null);
		} else {
			return true;
		}
	}

	public void runStatsOnTables(List<String> tables, boolean detailed)
	{
		try{
			for(String table : tables){
				updateTableStatistics(table, detailed);
			}
		}catch(SQLException e){
			e.printStackTrace();
		}
	}

	public void runStatsOnAllTables(OrchestraSystem catalog)
	{
		//		The first misses on _L_*, _R_* ...
				List<String> allTables = SqlEngine.getNamesOfAllTablesFromDeltas(catalog, true, true, true);
//		runStatsOnTables(allTables, true);
		runStatsOnTables(allTables, false);

//		List<String> baseAndMappingTables = SqlEngine.getNamesOfAllTablesFromDeltas(catalog, true, true, false);
//		runStatsOnTables(baseAndMappingTables, false);
//		List<String> baseAndASRTables = SqlEngine.getNamesOfAllTablesFromDeltas(catalog, true, false, true);
//		runStatsOnTables(baseAndASRTables, true);
	}

	public void turnOffLoggingAndResetStats()
	throws SQLException {
		List<String> statements = new ArrayList<String>();
		List<String> tables = SqlEngine.getNamesOfAllTablesFromDeltas(_system, true, true, true);

		for (String tab : tables) {
			statements.addAll(getSqlTranslator().turnOffLoggingAndResetStats(
					tab));
		}
		try {
			if (Config.getApply()) {
				for (String s : statements)
					_stmt.execute(s);
			}
		} catch (SQLException E) {
			Debug.println("activateNotLoggedInitDB2 caught Exception - but should be ok anyway");
		}

	}

	public void runstats(List<String> tables) {
		if (Config.getRunStatistics()) {
			try {
				Calendar before = Calendar.getInstance();
				System.out.println("Refresh statistics between datalog programs");
				runStatsOnTables(tables, false);
				if(!Config.getAutocommit()){ 
					_con.commit();
					turnOffLoggingAndResetStats();
				}
				Calendar after = Calendar.getInstance();
				time4CommitLogging += after.getTimeInMillis() - before.getTimeInMillis();
			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}

	public void runstats() {
		if (Config.getRunStatistics()) {
			try {
				Calendar before = Calendar.getInstance();
				System.out.println("Refresh statistics between datalog programs");
				runStatsOnAllTables(_system);
				if(!Config.getAutocommit()){ 
					_con.commit();
					turnOffLoggingAndResetStats();
				}
				Calendar after = Calendar.getInstance();
				time4CommitLogging += after.getTimeInMillis() - before.getTimeInMillis();
			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}

	protected boolean commitIfNecessary() throws java.sql.SQLException {
		//		if (!Config.getAutocommit() && Config.getRunStatistics()) {
		if (!Config.getAutocommit()) {
			if ((queryCnt > Config.getQueryCutoff())
					|| (transactionCnt > Config.getTransactionCutoff())) {
				if((queryCnt > Config.getQueryCutoff()))
					System.out.println("QUERY CUTOFF (" + Config.getQueryCutoff() + ") REACHED");
				else
					System.out.println("TRANSACTION CUTOFF (" + Config.getTransactionCutoff() + ") REACHED");
				Calendar before = Calendar.getInstance();
				runStatsOnAllTables(_system);
				_con.commit();
				turnOffLoggingAndResetStats();
				Calendar after = Calendar.getInstance();
				time4CommitLogging += after.getTimeInMillis()
				- before.getTimeInMillis();
				queryCnt = 0;
				transactionCnt = 0;
				return true;
			}
		}
		return false;
	}

	protected void checkIfSlowest(long time, int queryCnt) {
		if (slowestQueryTime < time) {
			slowestQueryTime = time;
			slowestQuery = queryCnt;
		}
	}

	protected void updateCounters(int num) {
		transactionCnt += num;
		totalTupleCnt += num;
		queryCnt++;
		totalQueryCnt++;

		Debug.println("LAST QUERY COUNT: " + num);
		Debug.println("QUERY COUNT: " + queryCnt);
		Debug.println("TUPLE COUNT: " + transactionCnt);
	}

	public long logTime() {
		return time4CommitLogging;
	}

	public long emptyTime() {
		return time4EmptyChecking;
	}

	// public PreparedStatement prepare(String q) {
	// try {
	// preparedStmtCnt++;
	// return _con.prepareStatement(q);
	// } catch (java.sql.SQLException s) {
	// s.printStackTrace();
	// throw new RuntimeException("Terminating");
	// }
	// }

	/**
	 * Evaluate a prepared statement, keeping track of how many operations were
	 * run, tuples were produced, etc.
	 * 
	 * @param stmt
	 * @return
	 */
	public int evaluatePrepared(PreparedStatement stmt, int curIterCnt,
			List<Integer> params) {
		try {
			Calendar before = Calendar.getInstance();

			if (Config.getStratified()) {
				if (params != null) {
					for (int i = 0; i < params.size(); i++)
						stmt.setInt(i + 1, curIterCnt + params.get(i));
					// stmt.setString(i+1, Integer.toString(params.get(i)));
				}

				//				Debug.println("QUERY WITH PARAMS: " + stmt);
			}

			int num = stmt.executeUpdate();

			Calendar after = Calendar.getInstance();
			long time = after.getTimeInMillis() - before.getTimeInMillis();

			Debug.println("QUERY EXECUTION TIME: " + time + " msec");

			checkIfSlowest(time, totalQueryCnt);

			updateCounters(num);

			commitIfNecessary();
			return num;

		} catch (java.sql.SQLException s) {
			s.printStackTrace();
			throw new RuntimeException(s);
		}
	}

	/**
	 * Evaluate a conventional update statement, waiting until it completes.
	 * Recycles the same statement.
	 * 
	 * @param str
	 * @return
	 */
	public int evaluateUpdate(String str) throws SQLException {
		checkConnected();

		Calendar before = Calendar.getInstance();
		int num;
		try {
			_log.info(str);
			num = _stmt.executeUpdate(str);
		} catch (SQLException ex) {
			System.err.println(str);
			throw ex;
		}

		Calendar after = Calendar.getInstance();
		long time = after.getTimeInMillis() - before.getTimeInMillis();

		Debug.println("QUERY EXECUTION TIME: " + time + " msec");

		checkIfSlowest(time, totalQueryCnt);

		updateCounters(num);

		commitIfNecessary();

		return num;
	}

	public void addToBatch(String str) throws Exception {
		// We need to reset the batch if we have evaluated
		// and then are adding more
		if (didUpdate) {
			_stmt.clearBatch();
			didUpdate = false;
		}

		_stmt.addBatch(str);
	}

	public int evaluateBatch() throws Exception {
		Calendar before = Calendar.getInstance();
		int num = 0;
		int[] numbers = _stmt.executeBatch();

		// for (int i : numbers)
		// num += numbers[i];

		for (int i = 0; i < numbers.length; i++) {
			num += numbers[i];
		}

		Calendar after = Calendar.getInstance();
		long time = after.getTimeInMillis() - before.getTimeInMillis();

		Debug.println("QUERY EXECUTION TIME: " + time + " msec");

		checkIfSlowest(time, totalQueryCnt);

		updateCounters(num);

		commitIfNecessary();

		didUpdate = true;
		return num;
	}

	public boolean evaluate(String str) {
		boolean ret = false;
		try {
			Debug.println(str);
			ret = _stmt.execute(str);
		} catch (java.sql.SQLException s) {
			System.err.println(str);
			s.printStackTrace();
			return false;
		}
		return ret;
	}

	public ResultSet evaluateQuery(String str) throws SQLException {
		checkConnected();

		synchronized (_statementResults) {
			Debug.println(str);
			try {
				Statement s = getNewStatement();
				if(Config.isMYSQL())
					s.setFetchSize(Integer.MIN_VALUE);
				else
					s.setFetchSize(1);
				ResultSet r = s.executeQuery(str);
				bindStatement(s, r);
				return r;
			} catch (SQLException sqle) {
				System.err.println("Error executing " + str);
				sqle.printStackTrace();
				throw sqle;
			}
		}
	}

	public boolean evaluateFromShell(String str, File dir, boolean createOrLoad) {
		try {
			String curDir = System.getProperty("user.dir");

			System.out.println(curDir);
			Process proc;

			if (Config.isDB2()) {
				// Process proc = Runtime.getRuntime().exec("db2cmd /c /w /i db2
				// -lcommands"+str+" -zimportout-" + str +" -wtf " + str);
				System.out.println("db2cmd /c /w /i db2 -tf " + str);
				proc = Runtime.getRuntime().exec(
						"db2cmd /c /w /i db2 -tf " + str, null, dir);
			} else { // if (Config.isOracle()){
				if (createOrLoad) {
					// System.out.println("sqlldr " + Config.USER + "/" +
					// Config.PASSWORD + "@" + Config.DBNAME + " CONTROL=" +
					// str);
					// proc = Runtime.getRuntime().exec("sqlldr " + Config.USER
					// + "/" + Config.PASSWORD + "@" + Config.DBNAME + "
					// CONTROL=" + str, null, dir);
					System.out.println(Config.getCygwinHome() + "/bin/bash "
							+ str);
					proc = Runtime.getRuntime().exec(
							Config.getCygwinHome() + "/bin/bash " + str, null,
							dir);

				} else { // sqlldr $DBUSR/$DBPWD@$DBINST CONTROL=catalog.ctl
					// LOG=log/catalog.log
					System.out.println("sqlplus " + _username + "/" + _password
							+ "@" + _server + " @" + str);
					proc = Runtime.getRuntime().exec(
							"sqlplus " + _username + "/" + _password + "@"
							+ _server + " @" + str, null, dir);
				}
			}

			// System.out.println("db2cmd /c /w /i db2 -lcommands"+str+"
			// -zimportout-" + str +" -wtf " + str);

			/*
			 * // Process proc = Runtime.getRuntime().exec("db2cmd /c /w /i db2
			 * -ctf " + str); Process proc = Runtime.getRuntime().exec("db2cmd
			 * /c /w /i db2 -lcommands"+str+" -zimportout-" + str +" -wtcf " +
			 * str); // p.waitFor(); // Thread.sleep(10000); // Consume buffers -
			 * otherwise process blocks!
			 *  // InputStream stderr = proc.getErrorStream(); InputStream
			 * stdout = proc.getInputStream(); // InputStreamReader isr = new
			 * InputStreamReader(stderr); InputStreamReader isr = new
			 * InputStreamReader(stdout); BufferedReader br = new
			 * BufferedReader(isr); String line = null; // System.out.println("<ERROR>");
			 * while ( (line = br.readLine()) != null) Debug.println(line); //
			 * System.out.println("</ERROR>"); int exitVal = proc.waitFor(); //
			 * System.out.println("Process exitValue: " + exitVal);
			 */

			int result;
			// prepare buffers for process output and error streams
			StringBuffer err = new StringBuffer();
			StringBuffer out = new StringBuffer();

			// create thread for reading inputStream (process' stdout)
			StreamReaderThread outThread = new StreamReaderThread(proc
					.getInputStream(), out);
			// create thread for reading errorStream (process' stderr)
			StreamReaderThread errThread = new StreamReaderThread(proc
					.getErrorStream(), err);
			// start both threads
			outThread.start();
			errThread.start();
			// wait for process to end
			result = proc.waitFor();
			// finish reading whatever's left in the buffers
			outThread.join();
			errThread.join();

			if (result != 0) {
				System.out.println("Process returned non-zero value:" + result);
				System.out.println("Process output:\n" + out.toString());
				System.out.println("Process error:\n" + err.toString());
			} else {
				System.out.println("Process executed successfully");
				System.out.println("Process output:\n" + out.toString());
				System.out.println("Process error:\n" + err.toString());
			}

			return true;

		} catch (java.io.IOException s) {
			s.printStackTrace();
			return false;
			// } catch(java.lang.InterruptedException s){
			// s.printStackTrace();
			// return false;
		} catch (java.lang.Exception s) {
			s.printStackTrace();
			return false;
		}

	}

	public DataSource getDataSource() {
		return _ds;
	}

	public List<String> getAttributes(String rel) {
		List<String> ret = new ArrayList<String>();
		for (Schema sc : _schemas) {
			AbstractRelation r;
			try {
				r = sc.getRelation(getTableName(rel));
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			}

			for (RelationField f : r.getFields()) {
				ret.add(f.getName());
			}
		}

		return ret;
	}

	public List<String> getTypes(String rel) {
		List<String> ret = new ArrayList<String>();
		for (Schema sc : _schemas) {
			AbstractRelation r;
			try {
				r = sc.getRelation(getTableName(rel));
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			}
			for (RelationField f : r.getFields()) {
				ret.add(f.getSQLTypeName());
			}
		}

		return ret;
	}

	public static String getTableName(String relName) {
		String rName = relName.toUpperCase();

		// if (!Config.isDB2())
		// return relName;

		String namespace = Config.getProperty("sqlschema").toUpperCase() + ".";

		// Remove the namespace qualification from the table name (if it exists)
		if (rName.indexOf(namespace) >= 0) {
			rName = rName.substring(0, rName.indexOf(namespace))
			+ rName.substring(rName.indexOf(namespace)
					+ namespace.length());
		}
		return rName;
	}

	public String updateStatCommand(String table) {
		return "RUNSTATS ON TABLE " + /* DBUSER + "." + */table
		+ " ON ALL COLUMNS ALLOW WRITE ACCESS";
	}

	public boolean importData(IDb source, List<String> baseTables)
	throws java.sql.SQLException {
		if (source instanceof FileDb) {
			FileDb fdb = (FileDb) source;
			List<String> statements = getSqlTranslator().importData(fdb,
					baseTables, this);

			if (Config.getApply()) {

				Calendar before = Calendar.getInstance();

				for (String s : statements) {
					// Debug.println("EVALUATE : " + s);
					if (Config.isDB2() || Config.isOracle()) {
						File dir = new File(Config.getProperty("workdir"));
						evaluateFromShell(s, dir, true);
					} else {
						Debug.println(s);
						evaluate(s);
					}
				}
				Calendar after = Calendar.getInstance();
				long time = (after.getTimeInMillis() - before.getTimeInMillis());
				System.out.println("EXP: IMPORT DATA TIME: " + time + " msec");

				for (String s : baseTables) {
					if (s.endsWith("_L") || (false && s.endsWith("_R"))) {
						ResultSet rs = evaluateQuery("select * from " + s
								+ "_DEL");

						int count = 0;
						while (rs.next())
							count++;
						rs.close();
						System.out.println(count + " tuples imported into " + s
								+ "_DEL");
						rs = evaluateQuery("select * from " + s + "_INS");
						count = 0;
						while (rs.next())
							count++;
						System.out.println(count + " tuples imported into " + s
								+ "_INS");
						rs.close();
					}
					// statements.add(updateStatCommand(s + "_INS"));
				}

				// initDS();
				// // try{
				// // connect();
				// //// init();
				// initKeepStmt();
				// // activateNotLoggedInitDB2(_con, _allTables);
				// commit();
				// }catch(Exception e){
				// e.printStackTrace();
				// }
				/*
				 * for(String table : baseTables){ List<String> check = new
				 * ArrayList<String>(); //
				 * evaluateQuery(fdb.checkImportCommand(table, AtomType.DEL));
				 * ResultSet res = evaluateQuery(fdb.checkImportCommand(table,
				 * AtomType.INS));
				 * 
				 * res.next(); int num = res.getInt(1); res.getMetaData();
				 * 
				 * evaluateQuery(fdb.checkImportCommand(table, AtomType.INS));
				 * 
				 * if(num != 0){ System.out.println("IMPORT SUCCESSFUL"); //
				 * return true; }else{ System.out.println("IMPORT FAILED!"); //
				 * return false; } }
				 */

			}

			return true;
		}
		return false;
	}

	public boolean initImportSource(Relation baseTable) throws Exception {
		// TODO: create a statement just for importing this table
		return false;
	}

	public boolean importNextTupleToArrayList(Relation baseTable,
			ArrayList<Object> data) throws Exception {
		// TODO: read a row from the tuple into the arraylist
		return false;
	}

	public static void setPreparedStatementData(PreparedStatement ps, int pos,
			int typ, Object o) throws SQLException, UnsupportedTypeException {
		switch (typ) {
		case java.sql.Types.BOOLEAN:
			ps.setBoolean(pos + 1, (Boolean) o);
			break;
		case java.sql.Types.INTEGER:
			ps.setInt(pos + 1, (Integer) o);
			break;
		case java.sql.Types.CHAR:
		case java.sql.Types.VARCHAR:
			ps.setString(pos + 1, (String) o);
			break;
		case java.sql.Types.CLOB:
			ps.setString(pos + 1, (String) o);
			break;
		case java.sql.Types.FLOAT:
			ps.setFloat(pos + 1, (Float) o);
			break;
		case java.sql.Types.DOUBLE:
			ps.setDouble(pos + 1, (Double) o);
			break;
		case java.sql.Types.DATE:
			ps.setDate(pos + 1, (java.sql.Date) o);
			break;
		default:
			throw new UnsupportedTypeException("JDBC OptimizerType Code " + typ);
		}

	}

	public void updateTableStatistics(String tableName, boolean detailed) throws SQLException{
		// Run statistics
		String statement = getSqlTranslator().runStats(tableName, AtomType.NONE, detailed);

		if (statement != null) {
			//			CallableStatement statS = _con.prepareCall(statement);

			if (Config.getApply()){
				_stmt.execute(statement);
				//				statS.executeUpdate();
			}
			Debug.println(statement);
			System.out.println(statement);
			//			statS.close();
		}
	}

	public int importRelation(IDb source, Relation baseTable,
			boolean replaceAll, Relation logicalTable, Schema s, Peer p,
			Db pubDb) throws Exception {

		int imported = 0;

		System.out.println("Importing relation " + baseTable.getName());

		if (_con == null)
			connect();

		source.initImportSource(baseTable);

		// Clear the old contents, if desired
		if (replaceAll) {
			if (Config.getApply()) {
				Statement delS = _con.createStatement();
				delS.execute("DELETE FROM " + baseTable.getFullQualifiedDbId());
			} else
				Debug
				.println("DELETE FROM "
						+ baseTable.getFullQualifiedDbId());
		}

		// Go through the Relation and make a PreparedStatement
		// with the appropriate types
		StringBuffer insStatement = new StringBuffer("INSERT INTO "
				+ baseTable.getFullQualifiedDbId() + "_INS " + "(");

		boolean first = true;
		for (int i = 0; i < baseTable.getNumCols(); i++) {
			insStatement.append(((first) ? "" : ",") + baseTable.getColName(i));
			first = false;
		}

		insStatement.append(")\n VALUES (");
		first = true;
		for (int i = 0; i < baseTable.getNumCols(); i++) {
			insStatement.append(((first) ? "?" : ",?"));
			first = false;
		}
		insStatement.append(")");

		PreparedStatement ps = null;
		try {
			ps = _con.prepareStatement(insStatement.toString());

			if (!Config.getApply())
				Debug.println(insStatement.toString());

			if (source instanceof FileDb) {
				while (((FileDb) source).importNextTupleToPS(baseTable, ps)) {
					ps.addBatch();
				}
			} else {
				ArrayList<Object> al = new ArrayList<Object>();
				while (source.importNextTupleToArrayList(baseTable, al)) {
					for (int i = 0; i < baseTable.getNumCols(); i++) {
						setPreparedStatementData(ps, i, baseTable.getColType(i)
								.getSqlTypeCode(), al.get(i));
					}
					ps.addBatch();
				}

			}
			if (Config.getApply()) {
				int[] counts = ps.executeBatch();

				for (int i = 0; i < counts.length; i++)
					imported += counts[i];
			}
		} finally {
			if (null != ps) {
				ps.close();
			}
		}

		//		updateTableStatistics(baseTable.getFullQualifiedDbId());

		int count = this.convert(logicalTable, new RelationContext(baseTable,
				s, p, false), AtomType.INS, pubDb);
		pubDb.publish();
		_system.translate();
		System.out.println("Converted " + count + " tuples from import of "
				+ imported + " into " + logicalTable.getName() + " ("
				+ logicalTable.getRelationID() + ")");

		// Now clear out the _INS table, since we've already copied from it and
		// will
		// import back!
		if (Config.getApply()) {
			Statement delS = _con.createStatement();
			delS.execute("DELETE FROM " + baseTable.getFullQualifiedDbId()
					+ "_INS");
		}

		return imported;
	}

	private void dropExistingData(Relation r) throws SQLException {
		Statement st = _con.createStatement();
		if (Config.getApply())
			st.execute("DELETE FROM " + r.getFullQualifiedDbId());
		else
			Debug.println("DELETE FROM " + r.getFullQualifiedDbId());
	}

	private void dropExistingData(String r) throws SQLException {
		Statement st = _con.createStatement();
		if (Config.getApply())
			st.execute("DELETE FROM " + r);
		else
			Debug.println("DELETE FROM " + r);
	}

	/**
	 * Moves data from two internal tables
	 * 
	 * @param r
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public void moveExistingData(Relation r, String sourceSuffix,
			String targetSuffix) throws ClassNotFoundException, SQLException {
		if (_con == null)
			connect();

		Statement st = _con.createStatement();
		if (Config.getApply())
			st
			.execute("DELETE FROM " + r.getFullQualifiedDbId()
					+ targetSuffix);
		else
			Debug.println("DELETE FROM " + r.getFullQualifiedDbId()
					+ targetSuffix);

		// Go through the Relation and make a PreparedStatement
		// with the appropriate types
		StringBuffer insStatement = new StringBuffer("INSERT INTO "
				+ r.getFullQualifiedDbId() + targetSuffix + " (");

		boolean first = true;
		for (int i = 0; i < r.getNumCols(); i++) {
			insStatement.append(((first) ? "" : ",") + r.getColName(i));
			first = false;
		}

		insStatement.append(")\n SELECT ");
		first = true;
		for (int i = 0; i < r.getNumCols(); i++) {
			insStatement.append(((first) ? "" : ",") + r.getColName(i));
			first = false;
		}
		insStatement.append("\n FROM " + r.getFullQualifiedDbId()
				+ sourceSuffix);

		if (Config.getApply())
			st.execute(insStatement.toString());
		else
			Debug.println(insStatement.toString());

		if (Config.getApply())
			st
			.execute("DELETE FROM " + r.getFullQualifiedDbId()
					+ sourceSuffix);
		else
			Debug.println("DELETE FROM " + r.getFullQualifiedDbId()
					+ sourceSuffix);
	}

	public static String skolemExpr(AtomSkolem sk,
			Map<String, String> skolemVarSymbols) {
		// assume for now all parameters are plain variables
		StringBuffer buf = new StringBuffer();

		for (int i = 0; i <= sk.getParams().size(); i++)
			buf.append("CONCAT(");

		buf.append("'" + sk.getName() + "('");
		boolean f = true;
		for (AtomArgument val : sk.getParams()) {
			buf.append(",");
			buf.append((f ? "" : "CONCAT(',' ,"));
			if (val instanceof AtomVariable) {
				buf.append("CONCAT(CONCAT(");
				buf.append("'''', " + skolemVarSymbols.get(val.toString()));
				buf.append("), '''')");
			} else if (val instanceof AtomConst) {
				buf.append(val.toString());
			} else if (val instanceof AtomSkolem)
				buf.append(skolemExpr((AtomSkolem) val, skolemVarSymbols));

			buf.append((f ? "" : ")"));
			f = false;
			buf.append(")");
		}
		buf.append(",')')");

		return buf.toString();
	}

	public void setAllTables(List<String> allTables) {
		_allTables = allTables;
	}

	public List<Schema> getSchemas() {
		return _schemas;
	}

	public void resetConnections() {
		try {
			// closeDS();
			disconnect();

			connect();
		} catch (Exception e) {

		}
	}

	public void gatherStats() {
		// if(Config.isOracle()){
		// boolean stats = evaluate("BEGIN DBMS_STATS.GATHER_SCHEMA_STATS('" +
		// Config.DBNAME + "');END;");
		// if(stats)
		// Debug.println("Statistics ok");
		// else
		// Debug.println("Problem Gathering Statistics");
		// }
	}

	public void activateRuleBasedOptimizer() {
		if (Config.isOracle()) {
			System.out.println("ALTER SESSION SET sql_trace = TRUE");
			boolean foo = evaluate("ALTER SESSION SET sql_trace = TRUE");
			System.out.println("alter session set optimizer_mode='rule'");
			foo = evaluate("alter session set optimizer_mode='rule'");
			if (foo)
				System.out.println("Optimizer ok");
			else
				System.out.println("Optimizer KO");
		}
	}

	public void serialize(Document doc, Element db) {
		db.setAttribute("type", "sql");
		db.setAttribute("server", _server);
		db.setAttribute("username", _username);
		db.setAttribute("password", _password);
	}

	public static SqlDb deserialize(OrchestraSystem catalog, Map<String, Schema> builtInSchemas, Element db) {
		String server = db.getAttribute("server");
		String username = db.getAttribute("username");
		String password = db.getAttribute("password");
		List<String> tables = SqlEngine.getNamesOfAllTables(catalog, false,
				false, true);
		List<Schema> schemas = catalog.getAllSchemas();
		return new SqlDb(server, username, password, tables, schemas, catalog, builtInSchemas);
	}

	static class Result extends ResultSetIterator<Tuple> {
		protected Relation m_relation;
		protected int[] m_labels; // m_labels[i] is index in result of null
		// label column for i

		protected boolean m_hasProv = false;
		protected Type m_provType;

		protected int oneBased(int i) {
			return i + 1;
		}

		public Result(ResultSet rs, /* Abstract */Relation rel/* , Relation r */)
		throws SQLException {
			super(rs);
			/*
			 * edu.upenn.cis.orchestra.datamodel.Schema schema = new
			 * edu.upenn.cis.orchestra.datamodel.Schema(); try { m_relation =
			 * schema.getOrCreateRelationSchema(rel); schema.markFinished(); }
			 * catch (AbstractRelation.BadColumnName e) { assert(false); //
			 * shouldn't happen }
			 */
			m_relation = rel;
			m_labels = new int[m_relation.getNumCols()];

			ResultSetMetaData md = rs.getMetaData();
			for (int i = 0; i < m_relation.getNumCols(); i++) {
				String name = m_relation.getColName(i) + RelationField.LABELED_NULL_EXT;
				m_labels[i] = -1;
				for (int j = 0; j < md.getColumnCount(); j++) {
					if (md.getColumnLabel(oneBased(j))
							.compareToIgnoreCase(name) == 0) {
						m_labels[i] = j;
						break;
					}
				}
			}
			Debug.println("RESULT SET SCHEMA:");
			for (int i = 0; i < md.getColumnCount(); i++) {
				Debug.println(md.getColumnLabel(oneBased(i)) + " "
						+ md.getColumnTypeName(oneBased(i)));
				if (md.getColumnLabel(oneBased(i)).equals("PROV__"))
					m_hasProv = true;
			}
			Debug.println("CATALOG SCHEMA:");
			Debug.println(m_relation.toString());

			m_provType = new StringType(true, true, true, 255);
		}

		public Tuple readCurrent() throws IteratorException {
			Tuple tup = new Tuple(m_relation);
			int count = m_relation.getNumCols();
			for (int i = 0; i < count; i++) {
				Type type = m_relation.getColType(i);
				try {
					int label = m_labels[i] != -1 ? rs
							.getInt(oneBased(m_labels[i])) : 1;
							if (label < 0) {
								assert (type.getFromResultSet(this.rs, oneBased(i)) == null);
								tup.setLabeledNull(i, label);
							} else {
								Object value = type.getFromResultSet(this.rs,
										oneBased(i));
								tup.set(i, value);
							}
				} catch (SQLException e) {
					throw new IteratorException(e);
				} catch (ValueMismatchException e) {
					throw new IteratorException(e);
				}
			}
			try {
				if (m_hasProv)
					tup.setProvenance((String) m_provType.getFromResultSet(
							this.rs, this.rs.getMetaData().getColumnCount()));
			} catch (SQLException e) {
				throw new IteratorException(e);
			}
			return tup;
		}
	}

	protected void checkConnected() throws SQLException {
		if (_con == null) {
			throw new SQLException("Not connected to database");
		}
	}

	public ResultSetIterator<Tuple> evalQueryRule(Rule rule)
	throws SQLException {
		if (Config.getApply()) {
			RuleSqlGen gen = new RuleSqlGen(rule, _builtins, false, true);
			ISqlSelect q = gen.toQuery();
			String str = q.toString();
			ResultSet rs = evaluateQuery(str);
			Debug.println("evalQueryRule: " + str);
			return new Result(rs, rule.getHead().getRelation());
		}
		return null;
	}

	public List<Rule> eliminateDuplicateRules(List<Rule> inRules){
		Debug.println("Number of rules: " + inRules.size());
		List<Rule> rules = new ArrayList<Rule>();
		for(Rule r : inRules){
			if(!rules.contains(r)){
				rules.add(r);
			}
		}
		Debug.println("Number of rules after deduplication: " + rules.size());
		return rules;
	}
	public List<Rule> minimizeRules(List<Rule> rules){
		int j = 0;
		for(Rule rule : rules){
			rule.minimize();
			j++;
		}
		return rules;
	}
	public List<ResultSetIterator<Tuple>> evalRuleSet(List<Rule> inRules, String semiringName, boolean provenanceQuery) throws SQLException {
		List<ResultSetIterator<Tuple>> ret = new ArrayList<ResultSetIterator<Tuple>>();

		//		List<Rule> rules = eliminateDuplicateRules(minimizeRules(inRules));
		List<Rule> rules = eliminateDuplicateRules(inRules);
		//		List<Rule> rules = minimizeRules(inRules);
		Debug.println("Rules to evaluate");
		//		System.out.println("NET EVAL - NUM UNION QUERIES: " + numUnionedQueries);
		for(Rule rule : rules){
			Debug.println(rule.toString());
		}

		if (Config.getApply()) {

			ISqlSelect q = null;

			for(int k = 0 ; k < rules.size();){
				int j = k;
				String str = "";
				for(; j < rules.size() && j < k + numUnionedQueries; j++){
					Rule rule = rules.get(j);
					System.out.println("Rule has " + rule.getBody().size() + " body atoms");
//					System.out.println("evalRuleSet: " + rule.toString());

//					RuleSqlGen gen = new RuleSqlGen(rule, _builtins, false, true);
					RuleSqlGen gen = new RuleSqlGen(rule, _builtins, false, !provenanceQuery);
					q = gen.toQuery();

					if(str.length() > 0){
						str = str + " UNION ALL ";
					}
					str = str + q.toString();
				}
				k = j;

				List<ISqlSelectItem> s = q.getSelect();
				StringBuffer sel = new StringBuffer();
				StringBuffer grp = new StringBuffer();
				int i = 0;
				boolean firstSel = true;
				boolean firstGrp = true;

				for(i = 0; i < s.size()-1; i++){
					if(s.get(i).getTable() != null){
						//						String attr = s.get(i).getTable() + "." + s.get(i).getColumn();
						//						String attr = s.get(i).getTable() + "." + s.get(i).getAlias();
						String attr = "RRR." + s.get(i).getAlias();
						String alias = s.get(i).getAlias();
						if(!firstSel){
							sel.append(", ");
						}
						if(!firstGrp){
							grp.append(", ");
						}

						firstSel = false;
						firstGrp = false;

						sel.append(attr + " " + alias);
						grp.append(attr);
					}else{
						//						String attr = s.get(i).getColumn();
						String attr = "RRR." + s.get(i).getAlias();
						String alias = s.get(i).getAlias();
						if(!firstSel){
							sel.append(", ");
						}else{
							firstSel = false;

						}
						//						Maybe I should skip these altogether?
						sel.append(attr + " " + alias);

						if(!firstGrp){
							grp.append(", ");
						}
						firstGrp = false;

						grp.append(alias);
					}
				}
				String attr = s.get(i).getTable() + "." + s.get(i).getColumn();

				String alias = s.get(i).getAlias();
				if(s.get(i) != null && !s.get(i).toString().contains("PROV__")){
					sel.append(", " + attr + " " + alias);
					grp.append(", " + attr);
				}
				//				if(s.get(i).getTable() != null){
				//				attr = s.get(i).getTable() + "." + s.get(i).getColumn();
				//				if(!attr.contains("PROV__")){
				//				sel.append(", " + attr + " " + alias);
				//				grp.append(", " + attr);
				//				}
				//				}else{
				//				attr = s.get(i).getColumn();
				//				if(!attr.contains("PROV__")){
				//				sel.append(", " + attr + " " + alias);
				//				}
				//				}
				String grouped;
				if(Config.getValueProvenance()){
					String plusOp = "";
					if(ProvenanceNode.TRUST_SEMIRING.equalsIgnoreCase(semiringName) ||
							ProvenanceNode.TROPICAL_MAX_SEMIRING.equalsIgnoreCase(semiringName))
						plusOp = "MAX";
					else if(ProvenanceNode.RANK_SEMIRING.equalsIgnoreCase(semiringName) ||
							ProvenanceNode.TROPICAL_MIN_SEMIRING.equalsIgnoreCase(semiringName))
						plusOp = "MIN";
					else if (ProvenanceNode.BAG_SEMIRING.equalsIgnoreCase(semiringName))
						plusOp = "SUM";
					else
						plusOp = "SUM";

					grouped = "SELECT " + sel + ", " + plusOp + "(PROV__) PROV__" 
					+ " FROM (" + str+ ") AS RRR GROUP BY " + grp;
				}else{
					grouped = str;
				}

//				Debug.println("evalRuleSet: " + grouped);
				//				System.out.println("Comparing running times");
				//				Calendar bef1 = Calendar.getInstance();
				//				ResultSet rs = evaluateQuery(grouped);
				//				Calendar aft1 = Calendar.getInstance();
				//				long time1 = aft1.getTimeInMillis() - bef1.getTimeInMillis();
				//				System.out.println("TIME WITH AGGREGATE: " + time1);
				Calendar bef2 = Calendar.getInstance();
				try{
					System.out.println("Query size (bytes): " + str.length());
//					System.out.println("evalRuleSet - SQL query:" + str);
					ResultSet rs2 = evaluateQuery(str);

					Calendar aft2 = Calendar.getInstance();
					long time2 = aft2.getTimeInMillis() - bef2.getTimeInMillis();
					System.out.println("TIME WITHOUT AGGREGATE: " + time2 + " msec");
					ret.add(new Result(rs2, rules.get(0).getHead().getRelation()));
				}catch(SQLException e){
					System.out.println("Query threw Exception: " + str);
					throw e;
				}
			}
			//			ResultSet rs = evaluateQuery(str);
			//			System.out.println("evalRuleSet: " + str);
			return ret;
		}
		return null;
	}

	public int evalUpdateRule(Rule rule) throws SQLException {
		if (Config.getApply()) {
			RuleSqlGen gen = new RuleSqlGen(rule, _builtins, false, true);
			ISqlStatement s;
			if (rule.getHead().isNeg()) {
				s = gen.toDelete();
			} else {
				s = gen.toInsert();
			}
			String str = s.toString();
			Debug.println("evalUpdateRule: " + str);
			return evaluateUpdate(str);
		}
		return 0;
	}

	public List<String> createSQLTableCode(final String suffix, final Relation rel, boolean addMRule,
			boolean addStratum,
			boolean normalizeNames, boolean withLogging) {
		return createSQLTableCode(rel.getName(), suffix, rel, addMRule, addStratum, rel.hasLabeledNulls(),
				normalizeNames, withLogging);
	}
	
	/**
	 * Create a SQL table
	 * 
	 * @param tableName
	 *            physical table name prefix
	 * @param suffix
	 *            suffix to add, e.g., "_RCH"
	 * @param rel
	 *            logical relation
	 * @param addMRule
	 *            whether to add a physical MRULE column
	 * @param addStratum
	 *            whether to add a physical STRATUM column
	 * @param addLabeledNulls
	 *            whether to add labeled null columns
	 * @param normalizeNames
	 *            whether to use rel names or C0, C1, ...
	 * @param withLogging
	 *            whether to add to DB transaction log
	 * @return
	 */
	public List<String> createSQLTableCode(final String tableName,
			final String suffix, final Relation rel, boolean addMRule,
			boolean addStratum, boolean addLabeledNulls,
			boolean normalizeNames, boolean withLogging) {
		final List<String> statements = new ArrayList<String>();

		//		final String qualifiedRelName = (Config.getUseTempTables() ? "SESSION." :
		//		(rel.getDbSchema() != null ? (rel .getDbSchema() + ".") : "") ) 
		String qualifiedRelName = (rel.getDbSchema() != null ? (rel
				.getDbSchema() + ".") : "")
				+ tableName;

		// String typ;
		// //if (Config.isHsql())
		// // typ = "CACHED ";
		// //else
		// typ = "";

		// final StringBuffer buffStat = new StringBuffer (" (");
		List<ISqlColumnDef> cols = newArrayList();
		List<ISqlColumnDef> labNullCols = newArrayList();
		// StringBuffer labeledNulls = new StringBuffer();
		// boolean first = true;
		if (addStratum) {
			// buffStat.append("STRATUM INTEGER");
			cols.add(_sqlFactory.newColumnDef("STRATUM", "INTEGER", "0"));
			// first = false;
		}
		if (addMRule) {
			// if (!first)
			// buffStat.append(",");
			// buffStat.append("MRULE INTEGER");
			cols.add(_sqlFactory.newColumnDef(ProvenanceRelation.MRULECOLNAME, "INTEGER", null));
			// first = false;
		}

		int indCol = 0;
		int inx = 0;
		for (RelationField f : rel.getFields()) {
			String nam;
			if (normalizeNames)
				nam = "C" + indCol++;
			else
				nam = f.getName();
			// buffStat.append(((!first) ? "," : "") + nam + " " +
			// f.getSQLType());
			if(Config.getValueProvenance() && Relation.valueAttrName.equals(nam))
				cols.add(_sqlFactory.newColumnDef(nam, f.getSQLType(), "1"));
			else
				cols.add(_sqlFactory.newColumnDef(nam, f.getSQLType(), null));
			// first = false;

			if (addLabeledNulls) {
				// labeledNulls.append("," + nam + RelationField.LABELED_NULL_EXT
				// + " INTEGER");
				if (!Config.useCompactNulls() || rel.isNullable(inx))
					if(nam != ProvenanceRelation.MRULECOLNAME){
						labNullCols.add(_sqlFactory.newColumnDef(nam
								+ RelationField.LABELED_NULL_EXT, "INTEGER", null));
					}
			}
			inx++;
		}

		cols.addAll(labNullCols);

		// statements.add("CREATE " + typ + "TABLE " + qualifiedRelName + suffix
		// + buffStat);

		// zives
		if (!Config.getTempTables() 
				|| !(qualifiedRelName.startsWith(ISqlStatementGen.sessionSchema) 
						//						|| qualifiedRelName.endsWith("_DEL") || qualifiedRelName.endsWith("_INS") 
						//						|| qualifiedRelName.endsWith("_RCH") || qualifiedRelName.endsWith("_INV")
						//						|| qualifiedRelName.endsWith("_NEW") || qualifiedRelName.endsWith("_D")
				)
		) {
			//			System.out.println("* Adding non-temp table " + qualifiedRelName + suffix);
			statements.addAll(getSqlTranslator().createTable(
					qualifiedRelName + suffix, cols, !withLogging));

		} else {
			System.out.println("* Adding temp table " + qualifiedRelName + suffix);
			statements.addAll(getSqlTranslator().createTempTable(
					qualifiedRelName + suffix, cols));
		}

		return statements;
	}

	
	public List<String> createSQLIndexCode(
			final String suffix, final Relation rel,
			final List<RelationField> indexes,
			boolean normalizeNames, boolean withLogging) {
		return createSQLIndexCode(rel.getName(), suffix, rel, indexes, rel.hasLabeledNulls(),
				normalizeNames, withLogging);
	}
	/**
	 * This is only used for Outer union ...
	 * 
	 * @param tableName
	 * @param suffix
	 * @param rel
	 * @param indexes
	 * @param addLabeledNulls
	 * @param normalizeNames
	 * @param withLogging
	 * @return
	 */
	public List<String> createSQLIndexCode(final String tableName,
			final String suffix, final Relation rel,
			final List<RelationField> indexes, boolean addLabeledNulls,
			boolean normalizeNames, boolean withLogging) {
		final List<String> statements = new ArrayList<String>();

		final String qualifiedRelName = (rel.getDbSchema() != null ? (rel
				.getDbSchema() + ".") : "")
				+ tableName;

		// StringBuffer buff = new StringBuffer();
		Vector<ISqlColumnDef> cols = new Vector<ISqlColumnDef>();

		// boolean first = true;
		Iterator<RelationField> iter = indexes.iterator();
		for (int j = 0; j < indexes.size() && cols.size() < MAX_INDEX_COLS; j++) {
			// buff.append((first?"":",")); first = false;

			String nam;
			//			if (normalizeNames) {
			//			nam = "C" + iter.next();
			//			} else {
			nam = iter.next().getName();
			//			nam = rel.getField(iter.next()).getName();
			//			}

			// buff.append(nam);
			cols.add(_sqlFactory.newColumnDef(nam, "", null));
		}

		if (addLabeledNulls) {
			for (int i = 0; i < rel.getNumCols() && cols.size() < MAX_INDEX_COLS; i++) {
				if (!Config.useCompactNulls() || rel.isNullable(i)) {
					// buff.append(",");
					if (normalizeNames) {
						// buff.append("C" + i + RelationField.LABELED_NULL_EXT);
						cols.add(_sqlFactory.newColumnDef("C" + i
								+ RelationField.LABELED_NULL_EXT, "", null));
					} else {
						if(rel.getField(i).getName() != ProvenanceRelation.MRULECOLNAME){
							// buff.append(rel.getField(i).getName() +
							// RelationField.LABELED_NULL_EXT);
							cols.add(_sqlFactory.newColumnDef(rel.getField(i).getName()
									+ RelationField.LABELED_NULL_EXT, "", null));
						}
					}
				}
			}
		}

		// String stmt = "CREATE INDEX " +
		// (rel.getDbSchema()!=null?rel.getDbSchema() + ".":"")
		// + tableName + "_INDX ON " + qualifiedRelName + " (" + buff.toString()
		// + ") " + clustSpec;
		// statements.add(stmt);

		boolean cluster = true;
		statements.add(getSqlTranslator().createIndex(
				qualifiedRelName + suffix + "_INDX", qualifiedRelName + suffix, cols, cluster,
				!withLogging));
		return statements;
	}

	/**
	 * Go through the local _INS, _DEL tables and add each tuple as an
	 * independent transaction to the global update store.
	 * 
	 * @param store
	 *            Global update store
	 * @return Total updates added
	 * @throws SQLException
	 * @throws RelationNotFoundException
	 */
	private int convertInsDelToTransactions(Peer p, Db store)
	throws SQLException, IteratorException, ClassNotFoundException,
	DbException, RelationNotFoundException {
		if (!isConnected()) {
			connect();
		}
		int totalCount = 0;
		for (Schema s : p.getSchemas()) {
			for (Relation r : s.getRelations()) {
				if (r.isInternalRelation()
						//|| r.getName().endsWith(Relation.LOCAL)
						//|| r.getName().endsWith(Relation.REJECT)
						)
					continue;

				Relation rl = s.getRelation(r.getName() + Relation.LOCAL);

				// Read each tuple.
				// Convert into update. Add transaction to Db object.

				RelationContext rc = new RelationContext(rl, s, p, false);

				totalCount += convert(r, rc, AtomType.DEL, store);
				totalCount += convert(r, rc, AtomType.INS, store);

				dropExistingData(rl.getFullQualifiedDbId() + Relation.DELETE);
				dropExistingData(rl.getFullQualifiedDbId() + Relation.INSERT);
			}
		}
		return totalCount;
	}

	/**
	 * Go through the original base tuples and copy them to the update store.
	 * 
	 * @param p
	 * @param store
	 * @return
	 * @throws SQLException
	 * @throws IteratorException
	 * @throws ClassNotFoundException
	 * @throws DbException
	 */
	public int convertTuplesToIndependentTransactions(Peer p, Schema s,
			Relation r, Db store) throws SQLException, IteratorException,
			ClassNotFoundException, DbException {
		if (!isConnected()) {
			connect();
		}
		int totalCount = 0;
		if (r.isInternalRelation())
			return 0;

		// Read each tuple.
		// Convert into update. Add transaction to Db object.

		RelationContext rc = new RelationContext(r, s, p, false);

		totalCount += convert(r, rc, AtomType.INS, store);
		dropExistingData(r);
		return totalCount;
	}

	/**
	 * Copy the contents of a relation to the update store
	 * 
	 * @param log
	 *            Relation context for the logical relation (for _L tuples)
	 * @param rc
	 *            Relation context for the physical / local relation
	 * @param typ
	 *            NONE, DEL, or INS relation
	 * @param store
	 *            Destination global store
	 * @return
	 * @throws SQLException
	 * @throws IteratorException
	 * @throws ClassNotFoundException
	 * @throws DbException
	 */
	public int convert(Relation log, RelationContext rc, AtomType typ, Db store)
	throws SQLException, IteratorException, ClassNotFoundException,
	DbException {
		List<AtomArgument> a = new ArrayList<AtomArgument>();
		Relation r = rc.getRelation();
		for (int i = 0; i < r.getNumCols(); i++)
			a.add(new AtomVariable("x" + i));

		Atom b = new Atom(rc, a, typ);
		Atom h = new Atom(rc, a, AtomType.NONE);
		Rule rul = new Rule(h, b, null, this.getBuiltInSchemas());
		
		final int numLogCols = log.getNumCols();
		
		ResultSetIterator<Tuple> result = null;
		try { 
			result = evalQueryRule(rul);
			int count = 0;
			while (result != null && result.hasNext()) {
				Tuple tuple = result.next();

				Tuple logicalTuple = new Tuple(log);
				for (int i = 0; i < numLogCols; ++i) {
					logicalTuple.set(i, tuple.get(i));
				}
				
				List<Update> updates = new ArrayList<Update>();
				Update u;

				if (typ == AtomType.DEL)
					u = new Update(logicalTuple, null);
				else if (typ == AtomType.INS || typ == AtomType.NONE)
					u = new Update(null, logicalTuple);
				else
					throw new DbException("Unsupported atom type " + typ.toString());
				updates.add(u);
				store.addTransaction(updates);
				count++;
			}
			Debug.println("Added "
					+ count
					+ " updates from "
					+ rc.toString()
					+ ((typ == AtomType.DEL) ? " deletions" : " insertions"
						+ " with target relation " + log.getName()));
			return count;

		} catch (ValueMismatchException e) {
			throw new DbException("Error converting to logical schema", e);
		} finally { 
			if (null != result) { result.close(); }
		}

	}

	public int fetchDbTransactions(Peer p, Db store) throws Exception {
		// TODO: import from trans log

		// For now, we just convert the insert and delete tables
		return convertInsDelToTransactions(p, store);
	}

	/**
	 * Takes all data published by a different peer and tries to apply it to the
	 * update exchange local copy
	 * 
	 * @param sys
	 * @param recno
	 * @param reconciler
	 * @param store
	 * @return
	 * @throws DbException
	 * @throws IteratorException
	 * @throws SQLException
	 */
	public int fetchPublishedTransactions(OrchestraSystem sys, int lastrec,
			int recno, Peer reconciler, Db store) throws DbException,
			IteratorException, SQLException, UnsupportedTypeException,
			DuplicateRelationIdException {
		Db db = sys.getRecDb(reconciler.getId());

		BatchInsert batches = new BatchInsert();

		for (int rec = lastrec; rec <= recno; rec++) {
			ResultIterator<TxnPeerID> tList = null;
			try { 
				tList = db
				.getTransactionsForReconciliation(rec);
				while (tList.hasNext()) {
					TxnPeerID txn = tList.next();

					List<Update> ul = db.getTransaction(txn);

					String pid = txn.getPeerID().toString();

					// Convert back to string name
					if (pid.startsWith("@"))
						pid = pid.substring(1);

					Peer p = sys.getPeer(pid);

					for (Update u : ul) {
						boolean found = false;
						for (Schema s : p.getSchemas()) {
							try {
								Relation r = s.getRelation(u.getRelationName());

								batches.add(r, u);
								found = true;
								break;
							} catch (RelationNotFoundException rnf) {

							}
						}

						if (!found) {
							throw new DbException(
									"Relation "
									+ u.getRelationName()
									+ " not found in corresponding schema for peer "
									+ pid + "!");
						}
					}
				}
			} finally { 
				if (null != tList) { tList.close(); }
			}
		}
		return batches.applyAll();
	}

	/**
	 * @deprecated
	 * 
	 * @param sys
	 * @param lastrec
	 * @param recno
	 * @param reconciler
	 * @param store
	 * @throws DbException
	 * @throws NoSuchElementException
	 * @throws IteratorException
	 */
	public void acceptPublishedTransactions(OrchestraSystem sys, int lastrec,
			int recno, Peer reconciler, Db store) throws DbException,
			NoSuchElementException, IteratorException,
			DuplicateRelationIdException {
		Db db = sys.getRecDb(reconciler.getId());

		for (int rec = lastrec; rec <= recno; rec++) {
			ResultIterator<TxnPeerID> tList = null; 
			try { 
				tList = db
				.getTransactionsForReconciliation(rec);

				while (tList.hasNext()) {
					TxnPeerID txn = tList.next();

					List<Update> ul = db.getTransaction(txn);

					String pid = txn.getPeerID().toString();

					// Convert back to string name
					if (pid.startsWith("@"))
						pid = pid.substring(1);

					((ClientCentricDb) db).applySingleTrans(recno, txn, ul);
				}
			} finally {
				if (null != tList) { tList.close(); } 
			}
		}
	}

	public void registerBuiltInSchema(Schema s) {
		_builtins.put(s.getSchemaId(), s);
	}

	public Schema getBuiltInSchema(String s) {
		return _builtins.get(s);
	}

	public Map<String, Schema> getBuiltInSchemas() {
		return _builtins;
	}

	/**
	 * Determines whether a relation is a built-in function
	 * 
	 * @param a
	 * @return
	 */
	public boolean isBuiltInAtom(Atom a) {
		return BuiltinFunctions.isBuiltIn(a.getSchema().getSchemaId(), a
				.getRelation().getName(), _builtins);
	}
	
	private String relationToInsert(Relation r) {
		ISqlInsert insert = _sqlFactory.newSqlInsert(r.getFullQualifiedDbId());
		ISqlExpression valuesTemplate = _sqlFactory
				.newExpression(Code.COMMA);
		int ncol = r.getNumCols();
		for (int i = 0; i < ncol; i++) {
			valuesTemplate
					.addOperand(_sqlFactory
							.newConstant(
									"?",
									edu.upenn.cis.orchestra.sql.ISqlConstant.Type.PREPARED_STATEMENT_PARAMETER));

		}
		insert.addValueSpec(valuesTemplate);
		return insert.toString();
	}
}
