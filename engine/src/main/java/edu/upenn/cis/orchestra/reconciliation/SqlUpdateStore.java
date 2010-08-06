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
package edu.upenn.cis.orchestra.reconciliation;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.TrustConditions;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.datamodel.Type;
import edu.upenn.cis.orchestra.datamodel.Update;
import edu.upenn.cis.orchestra.datamodel.AbstractPeerID.PeerIDFormatException;
import edu.upenn.cis.orchestra.datamodel.TrustConditions.Trusts;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.datamodel.iterators.IteratorException;
import edu.upenn.cis.orchestra.datamodel.iterators.ResultSetIterator;
import edu.upenn.cis.orchestra.reconciliation.USDump.RecnoEpoch;

public class SqlUpdateStore extends UpdateStore implements TransactionDecisions {
	public static class Factory implements UpdateStore.Factory {
		private String dbUrl;
		private String username;
		private String password;
		public Factory(String dbUrl, String username, String password) {
			this.dbUrl = dbUrl;
			this.username = username;
			this.password = password;
		}
		public SqlUpdateStore getUpdateStore(AbstractPeerID pid, ISchemaIDBinding sch, Schema s, TrustConditions tc) throws USException {
			return new SqlUpdateStore(dbUrl, username, password, s, pid, tc.getConditions());
		}
		
		// Is the factory creating a local update store?  Here we say yes
		// -- there is a local proxy representing the factory
		public boolean isLocal() {
			return true;
		}
		

		public void serialize(Document doc, Element update) {
			update.setAttribute("type", "sql");
			update.setAttribute("server", dbUrl);
			update.setAttribute("username", username);
			update.setAttribute("password", password);
		}

		static public Factory deserialize(Element update) {
			String server = update.getAttribute("server");
			String username = update.getAttribute("username");
			String password = update.getAttribute("password");
			return new Factory(server, username, password);
		}
		public void resetStore(Schema schema) throws USException {
			Connection conn = null;
			try {
				Class.forName(Config.getUSJDBCDriver());
				conn = DriverManager.getConnection(dbUrl, username, password);
				conn.setAutoCommit(true);
				Statement s = conn.createStatement();
				s.execute("SET PATH = orchestr");

				SQLException err = checkDatabase(schema, s);
				if (err == null) {
					// Database has correct schema
					Set<String> tables = getUpdatesTables(s);
					tables.addAll(Arrays.asList(nonUpdateTables));

					try {
						for (String table : tables) {
							s.execute("DELETE FROM " + table);
						}
						for (String sequence : sequences) {
							s.execute("ALTER SEQUENCE " + sequence + " RESTART");
						}
					} catch (SQLException e) {
						throw new USException("Error clearing database state", e);
					}
				} else {
					dropDatabase(conn);
					createDatabase(schema, s);
				}
				s.close();
			} catch (ClassNotFoundException c) {
				throw new USException("Error connecting to SQL database", c);
			} catch (SQLException e) {
				throw new USException("Error resetting SQL database", e);
			} finally {
				try {
					if (conn != null) {
						conn.close();
					}
				} catch (SQLException e) {
					throw new USException("Error resetting SQL database", e);
				}
			}
		}
		public USDump dumpUpdateStore(ISchemaIDBinding binding, Schema schema) throws USException {
			Connection conn = null;
			try {
				Class.forName(Config.getUSJDBCDriver());
				conn = DriverManager.getConnection(dbUrl, username, password);
				conn.setAutoCommit(false);
				conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
				Statement s = conn.createStatement();
				s.execute("SET PATH = orchestr");

				SQLException err = checkDatabase(schema, s);
				if (err != null) {
					throw new USException("SQL database does not have correct schema");
				}

				Map<AbstractPeerID,Schema> schemas = new HashMap<AbstractPeerID,Schema>();
				ResultSet rs = s.executeQuery("SELECT DISTINT peer FROM tids");
				while (rs.next()) {
					AbstractPeerID pid = AbstractPeerID.deserialize(rs.getString(1));
					schemas.put(pid, schema);
				}
				rs.close();
				
				USDump d = new USDump(binding, schemas);

				rs = s.executeQuery("SELECT peer, recno, epoch FROM recnos");
				while (rs.next()) {
					AbstractPeerID pid = AbstractPeerID.deserialize(rs.getString(1));
					int recno = rs.getInt(2);
					int epoch = rs.getInt(3);
					d.addRecnoEpoch(pid, recno, epoch);
				}
				rs.close();

				rs = s.executeQuery("SELECT truster, recno, trusted, tid FROM accepted");
				while (rs.next()) {
					AbstractPeerID truster = AbstractPeerID.deserialize(rs.getString(1));
					int recno = rs.getInt(2);
					AbstractPeerID trusted = AbstractPeerID.deserialize(rs.getString(3));
					int tid = rs.getInt(4);
					d.addDecision(truster, new TxnPeerID(tid,trusted), recno, true);
				}
				rs.close();

				rs = s.executeQuery("SELECT truster, recno, trusted, tid FROM rejected");
				while (rs.next()) {
					AbstractPeerID truster = AbstractPeerID.deserialize(rs.getString(1));
					int recno = rs.getInt(2);
					AbstractPeerID trusted = AbstractPeerID.deserialize(rs.getString(3));
					int tid = rs.getInt(4);
					d.addDecision(truster, new TxnPeerID(tid,trusted), recno, false);
				}
				rs.close();

				StringBuilder sb = new StringBuilder("SELECT DISTINCT peer, tid, epoch FROM (");

				for (int i = 0; i < schema.getNumRelations(); ++i) {
					String relName = schema.getNameForID(i);
					if (i != 0) {
						sb.append(" UNION ");
					}
					sb.append("SELECT peer, tid, epoch FROM updates_" + relName);
				}
				sb.append(") AS R");

				rs = s.executeQuery(sb.toString());
				while (rs.next()) {
					AbstractPeerID pid = AbstractPeerID.deserialize(rs.getString(1));
					int tid = rs.getInt(2);
					int epoch = rs.getInt(3);
					d.addTxnEpoch(new TxnPeerID(tid,pid), epoch);
				}
				rs.close();

				Map<TxnPeerID,List<Update>> txns = new HashMap<TxnPeerID,List<Update>>();

				for (int i = 0; i < schema.getNumRelations(); ++i) {
					rs = s.executeQuery("SELECT * FROM updates_" + schema.getNameForID(i));
					parseUpdates(schema.getRelationSchema(i), rs, txns);
					rs.close();
				}

				rs = s.executeQuery("SELECT peer, tid, serialno, prevpeer, prevtid FROM immediateAntecedents");
				while (rs.next()) {
					TxnPeerID tid = new TxnPeerID(rs.getInt(2), AbstractPeerID.deserialize(rs.getString(1)));
					TxnPeerID prevTid = new TxnPeerID(rs.getInt(5), AbstractPeerID.deserialize(rs.getString(4)));
					int serialno = rs.getInt(3);
					txns.get(tid).get(serialno).addPrevTid(prevTid);
				}
				rs.close();

				d.addTxns(txns);

				s.close();
				return d;
			} catch (ClassNotFoundException c) {
				throw new USException("Error connecting to SQL database", c);
			} catch (SQLException e) {
				throw new USException("Error dumping SQL database", e);
			} catch (PeerIDFormatException e) {
				throw new USException("Error decoding Peer ID from SQL database", e);
			} finally {
				try {
					if (conn != null) {
						conn.commit();
						conn.close();
					}
				} catch (SQLException e) {
					throw new USException("Error dumping SQL database", e);
				}
			}
		}
		public void restoreUpdateStore(USDump d) throws USException {
			Schema s = null;
			for (Schema ss : d.getSchemas().values()) {
				if (s == null) {
					s = ss;
				} else if (! s.equals(ss)) {
					throw new USException("SqlUpdateStore does not support multiple schemas");
				}
			}
			resetStore(s);
			Connection conn = null;
			try {
				Class.forName(Config.getUSJDBCDriver());
				conn = DriverManager.getConnection(dbUrl, username, password);
				conn.setAutoCommit(false);
				conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

				Set<AbstractPeerID> pids = d.getPeers();

				PreparedStatement addRecon = conn.prepareStatement("INSERT INTO recnos VALUES(?,?,?,0)");
				PreparedStatement addAccept = conn.prepareStatement("INSERT INTO accepted VALUES(?,?,?,?)");
				PreparedStatement addReject = conn.prepareStatement("INSERT INTO rejected VALUES(?,?,?,?)");

				for (AbstractPeerID pid : pids) {
					String pidString = pid.serialize();
					Iterator<Decision> ds = d.getPeerDecisions(pid);
					while (ds.hasNext()) {
						Decision dec = ds.next();
						PreparedStatement ps;
						if (dec.accepted) {
							ps = addAccept;
						} else {
							ps = addReject;
						}
						ps.setString(1, pidString);
						ps.setInt(2, dec.recno);
						ps.setString(3, dec.tpi.getPeerID().serialize());
						ps.setInt(4, dec.tpi.getTid());
						ps.addBatch();
					}
					Iterator<RecnoEpoch> recnos = d.getPeerRecons(pid);
					while (recnos.hasNext()) {
						RecnoEpoch recon = recnos.next();
						addRecon.setString(1, pidString);
						addRecon.setInt(2, recon.recno);
						addRecon.setInt(3, recon.epoch);
						addRecon.addBatch();
					}
				}

				addRecon.executeBatch();
				addAccept.executeBatch();
				addReject.executeBatch();

				Iterator<TxnPeerID> tids = d.getTids();
				final int numRel = s.getNumRelations();
				List<PreparedStatement> insertUpdate = new ArrayList<PreparedStatement>(numRel);
				for (int i = 0; i < numRel; ++i) {
					StringBuilder sb = new StringBuilder("INSERT INTO updates_" + s.getNameForID(i) + " VALUES(?,?,?,?");
					int numCols = s.getRelationSchema(i).getNumCols();
					while (numCols > 0) {
						sb.append(",?,?,?,?");
						--numCols;
					}
					sb.append(")");
					insertUpdate.add(conn.prepareStatement(sb.toString()));
				}

				PreparedStatement addAntecedent = conn.prepareStatement("INSERT INTO immediateAntecedents VALUES(?,?,?,?,?)");

				while (tids.hasNext()) {
					TxnPeerID tid = tids.next();
					String pid = tid.getPeerID().serialize();
					int epoch = d.getTxnEpoch(tid);
					List<Update> contents = d.getTxnContents(tid);

					int serialno = 0;
					for (Update u : contents) {
						int rel = u.getRelationID();
						PreparedStatement ps = insertUpdate.get(rel);
						ps.setString(1, pid);
						ps.setInt(2, tid.getTid());
						ps.setInt(3, serialno);
						ps.setInt(4, epoch);

						Relation rs = s.getRelationSchema(rel);
						final int numCols = rs.getNumCols();
						for (int i = 0; i < numCols; ++i) {
							Type t = rs.getColType(i);
							Tuple oldVal = u.getOldVal();
							Tuple newVal = u.getNewVal();
							if (u.isDeletion() || u.isUpdate()) {
								if (oldVal.isLabeledNull(i)) {
									ps.setNull(4 + 4*i + 1, t.getSqlTypeCode());
									ps.setInt(4 + 4*i + 2, oldVal.getLabeledNull(i));
								} else {
									Object val = oldVal.get(i);
									if (val == null) {
										ps.setNull(4 + 4*i + 1, t.getSqlTypeCode());
									} else {
										ps.setObject(4 + 4*i + 1, val, t.getSqlTypeCode());
									}
									ps.setNull(4 + 4 * i + 2, Types.INTEGER);
								}
							} else {
								// Actual data
								ps.setNull(4 + 4 * i + 1, t.getSqlTypeCode());
								// Labeled null
								ps.setNull(4 + 4 * i + 2, Types.INTEGER);
							}
							if (u.isInsertion() || u.isUpdate()) {
								if (newVal.isLabeledNull(i)) {
									ps.setNull(4 + 4*i + 3, t.getSqlTypeCode());
									ps.setInt(4 + 4*i + 4, newVal.getLabeledNull(i));
								} else {
									Object val = newVal.get(i);
									if (val == null) {
										ps.setNull(4 + 4*i + 3, t.getSqlTypeCode());
									} else {
										ps.setObject(4 + 4*i + 3, val, t.getSqlTypeCode());
									}
									ps.setNull(4 + 4 * i + 4, Types.INTEGER);
								}
							} else {
								// Actual data
								ps.setNull(4 + 4 * i + 3, t.getSqlTypeCode());
								// Labeled null
								ps.setNull(4 + 4 * i + 4, Types.INTEGER);
							}
						}
						ps.addBatch();

						addAntecedent.setString(1, pid);
						addAntecedent.setInt(2, tid.getTid());
						addAntecedent.setInt(3, serialno);
						for (TxnPeerID ante : u.getPrevTids()) {
							addAntecedent.setString(4, ante.getPeerID().serialize());
							addAntecedent.setInt(5, ante.getTid());
							addAntecedent.addBatch();
						}

						++serialno;
					}
				}
				addAntecedent.executeBatch();
				for (PreparedStatement ps : insertUpdate) {
					ps.executeBatch();
				}
				Statement stmt = conn.createStatement();
				stmt.executeUpdate("INSERT INTO publishEpochs SELECT DISTINCT peer, epoch, 1 AS finished FROM tids");
				int lastEpoch = -1;
				ResultSet rs = stmt.executeQuery("SELECT MAX(epoch) FROM publishEpochs");
				while (rs.next()) {
					lastEpoch = rs.getInt(1);
					if (rs.wasNull()) {
						lastEpoch = -1;
					}
				}
				rs.close();
				if (lastEpoch == -1) {
					stmt.execute("ALTER SEQUENCE epochNum RESTART");
				} else {
					stmt.execute("ALTER SEQUENCE epochNum RESTART WITH " + (lastEpoch + 1));
				}

			} catch (ClassNotFoundException c) {
				throw new USException("Error connecting to SQL database", c);
			} catch (SQLException e) {
				throw new USException("Error restoring SQL database", e);
			} finally {
				try {
					if (conn != null) {
						conn.commit();
						conn.close();
					}
				} catch (SQLException e) {
					throw new USException("Error restoring SQL database", e);
				}
			}

		}
		/* (non-Javadoc)
		 * @see edu.upenn.cis.orchestra.reconciliation.UpdateStore.Factory#getSchemaIDBindingClient(edu.upenn.cis.orchestra.datamodel.AbstractPeerID, edu.upenn.cis.orchestra.reconciliation.SchemaIDBinding)
		 */
		@Override
		public ISchemaIDBindingClient getSchemaIDBindingClient() {
			// TODO Auto-generated method stub
			return null;
		}
		/* (non-Javadoc)
		 * @see edu.upenn.cis.orchestra.reconciliation.UpdateStore.Factory#startUpdateStoreServer()
		 */
		@Override
		public Process startUpdateStoreServer() throws USException {
			// TODO Auto-generated method stub
			return null;
			
		}
		/* (non-Javadoc)
		 * @see edu.upenn.cis.orchestra.reconciliation.UpdateStore.Factory#stopUpdateStoreServer()
		 */
		@Override
		public void stopUpdateStoreServer() throws USException {
			// TODO Auto-generated method stub
			
		}
		/* (non-Javadoc)
		 * @see edu.upenn.cis.orchestra.reconciliation.UpdateStore.Factory#updateStoreServerIsRunning()
		 */
		@Override
		public boolean updateStoreServerIsRunning() {
			Connection conn = null;
			Exception ex = null;
			try {
				Class.forName(Config.getUSJDBCDriver());
				conn = DriverManager.getConnection(dbUrl, username, password);
			} catch (Exception e) {
				ex = e;
			} finally {
				if (conn != null) {
					try {
						conn.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			}
			return ex == null;
		}
	}

	private static final int pidLength = 32;
	private static final String[] nonUpdateTables = {"recnos", "accepted", "rejected", "publishEpochs",
		"immediateAntecedents", "relevantTxns", "trustedTxns"};
	private static final String[] sequences = {"epochNum"};


	// Database connections
	Connection conn;
	Connection cursorConn;
	Connection benchConn;
	// JDBC Database URL
	private String dbUrl;
	// DB Connection Properties
	private Properties connProp;
	// Schema of shared database
	private Schema schema;
	// ID of the peer that owns this store
	private final AbstractPeerID id;
	// Trust conditions, as from Db class.
	// Mapping from relation ID to priority to trusts object
	// Recall that the TreeMap uses a reverse comparator, so the entries
	// come conveniently sorted in decreasing order by priority
	private Map<Integer,TreeMap<Integer,Set<Trusts>>> trustConds;

	// Benchmark data
	Benchmark benchmark;

	/**
	 * Create a new object to handle connections to the shared store of updates
	 * 
	 * @param dbUrl			JDBC DB URL
	 * @param username		DB username
	 * @param password		DB password
	 * @param s				Shared database schema
	 * @param id			ID of the peer that owns this store
	 * @param trustConds	Trust conditions from Db class
	 */
	public SqlUpdateStore(String dbUrl, String username, String password, Schema s,
			AbstractPeerID id, Map<Integer,TreeMap<Integer,Set<Trusts>>> trustConds)
	throws USException {
		try {
			Class.forName(Config.getUSJDBCDriver());
		} catch (ClassNotFoundException e) {
			throw new USException("Could not load JDBC driver for update store", e);
		}
		this.dbUrl = dbUrl;
		this.schema = s;
		this.id = id.duplicate();
		if (trustConds == null) {
			this.trustConds = new HashMap<Integer,TreeMap<Integer,Set<Trusts>>>();
		} else {
			this.trustConds = trustConds;
		}
		final int numRelations = schema.getNumRelations();
		retrieveUpdateStmts = new ArrayList<PreparedStatement>(numRelations);
		addUpdateStmts = new ArrayList<PreparedStatement>(numRelations);

		connProp = new Properties();
		connProp.put("user", username);
		connProp.put("password", password);
		connProp.put("clientAccountingInformation", getSQLLit(id.serialize()));
		connProp.put("clientApplicationInformation", "SqlUpdateStore");
		connProp.put("deferPrepares", "false");

		reconnect();
	}

	@Override
	public void disconnect() throws USException {
		if (conn == null) {
			return;
		}
		try {
			recordAcceptedTxnStmt.close();
			recordRejectedTxnStmt.close();
			recordReconcileStmt.close();
			startPublishStmt.close();
			finishPublishStmt.close();
			getTxnPriosStmt.close();

			for (PreparedStatement ps : addUpdateStmts) {
				ps.close();
			}

			for (PreparedStatement ps : retrieveUpdateStmts) {
				ps.close();
			}

			addUpdateStmts.clear();
			retrieveUpdateStmts.clear();

			conn.rollback();
			conn.close();
			cursorConn.close();
			cursorConn = null;
			conn = null;
		} catch (SQLException sqle) {
			throw new USException("Error when closing database connection: " + sqle.getMessage(), sqle);
		}
	}

	@Override
	public boolean isConnected() {
		return (conn != null);
	}

	@Override
	public void reconnect() throws USException {
		try {
			Class.forName(Config.getUSJDBCDriver());
			conn = DriverManager.getConnection(dbUrl, connProp);
			conn.setAutoCommit(false);
			conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
			cursorConn = DriverManager.getConnection(dbUrl, connProp.getProperty("user"), connProp.getProperty("password"));
			cursorConn.setAutoCommit(true);

			Statement s = conn.createStatement();
			s.execute("SET PATH = orchestr");

			Statement ccs = cursorConn.createStatement();
			ccs.execute("SET PATH = orchestr");
			ccs.close();

			SQLException e = checkDatabase(schema, s); 
			if (e != null) {
				throw new USException("SQL database does not have correct schema, run SqlUpdateStore.Factory.resetStore before connecting", e);
			}
			s.close();
			prepareStatements();
			conn.commit();
		} catch (SQLException sqle) {
			throw new USException("Database error in SqlUpdateStore constructor: " + sqle.getMessage(), sqle);
		} catch (ClassNotFoundException cnf) {
			throw new USException("Database error in SqlUpdateStore constructor: " + cnf.getMessage(), cnf);
		}
	}

	private ArrayList<PreparedStatement> addUpdateStmts;
	private ArrayList<PreparedStatement> retrieveUpdateStmts;
	private PreparedStatement recordAcceptedTxnStmt;
	private PreparedStatement recordRejectedTxnStmt;
	private PreparedStatement recordReconcileStmt;
	private PreparedStatement recordEmptyReconcileStmt;
	private PreparedStatement startPublishStmt;
	private PreparedStatement finishPublishStmt;
	private PreparedStatement recordAntecedentsStmt;
	private PreparedStatement getTxnPriosStmt;
	private PreparedStatement getRecnoStmt;
//	private PreparedStatement getElapsedExecTimeStmt;
	private PreparedStatement findTrustedTxnsStmt;
	private PreparedStatement clearRelevantTxnsStmt;
	private PreparedStatement insertTrustedTxnsStmt;
	private PreparedStatement updateRelevantTxnsStmt;
	private PreparedStatement getPrevTidsForRelevantTxnsStmt;
	private PreparedStatement getPrevTidsStmt;
	private PreparedStatement txnAcceptedStmt;
	private PreparedStatement txnRejectedStmt;
	private PreparedStatement decisionsStmt;
	private PreparedStatement reconciliationsStmt;
	private ArrayList<PreparedStatement> getUpdatesForRelationStmts;
	private ArrayList<PreparedStatement> getTransactionStmts;

	private PreparedStatement getFirstReconTxnsStmt;
	private PreparedStatement getLastReconTxnsStmt;
	private PreparedStatement getReconTxnsStmt;
	private PreparedStatement getAllTxnsStmt;

	private ArrayList<PreparedStatement> getAcceptedForRecnoStmts;
	private PreparedStatement getAcceptedForRecnoPrevTidsStmt;

	private PreparedStatement hasFoundTrustedTxnsStmt;
	private PreparedStatement setFoundTrustedTxnsStmt;

	private PreparedStatement getMaxTidStmt;

	private void prepareStatements() throws SQLException {
		String peerID = "peerId(" + getSQLLit(id.serialize()) + ")";
		String pidParam = "peerId(CAST(? AS VARCHAR(" + pidLength + ")))";
		final int numRelations = schema.getNumRelations();
		for (int i = 0; i < numRelations; ++i) {
			Relation table = schema.getRelationSchema(i);
			String name = schema.getNameForID(i);
			StringBuilder stmt = new StringBuilder("INSERT INTO updates_" + name +
					" VALUES (" + peerID + ",?,?,(SELECT MAX(epoch) FROM publishEpochs pe WHERE peer = " + peerID + ")");
			final int numCols = table.getNumCols();
			for (int j = 0; j < 4 * numCols; ++j) {
				stmt.append(",?");
			}
			stmt.append(")");
			addUpdateStmts.add(conn.prepareStatement(stmt.toString()));
		}

		recordAcceptedTxnStmt = conn.prepareStatement("INSERT INTO accepted VALUES(" + peerID + ",?," + pidParam + ",?)");
		recordRejectedTxnStmt = conn.prepareStatement("INSERT INTO rejected VALUES(" + peerID + ",?," + pidParam + ",?)");
		recordReconcileStmt = conn.prepareStatement("INSERT INTO recnos VALUES(" + peerID + ", COALESCE((SELECT MAX(recno) + 1 FROM recnos WHERE peer = " + peerID + ")," + StateStore.FIRST_RECNO + "), COALESCE((SELECT MAX(epoch) FROM publishEpochs pe WHERE finished <> 0 AND NOT EXISTS (SELECT * from publishEpochs pe2 WHERE pe2.epoch < pe.epoch AND pe2.finished = 0)),-1),0)");
		recordEmptyReconcileStmt = conn.prepareStatement("INSERT INTO recnos VALUES(" + peerID + ", (SELECT MAX(recno) + 1 FROM recnos WHERE peer = " + peerID + "), (SELECT MAX(epoch) FROM recnos WHERE peer = " + peerID + "),1)");
		startPublishStmt = conn.prepareStatement("INSERT INTO publishEpochs VALUES(" + peerID + ", NEXT VALUE FOR epochNum, 0)");
		finishPublishStmt = conn.prepareStatement("UPDATE publishEpochs SET finished = 1 WHERE peer = " + peerID + " AND finished = 0");
		getRecnoStmt = conn.prepareStatement("SELECT MAX(recno) FROM recnos WHERE peer = " + peerID);


		for (int i = 0; i < numRelations; ++i) {
			Relation rs = schema.getRelationSchema(i);
			String relname = schema.getNameForID(i);
			StringBuilder query = new StringBuilder("SELECT ");
			final int numCols = rs.getNumCols();
			for (int j = 0; j < numCols; ++j) {
				String colName = rs.getColName(j);
				query.append("u." + colName + "_old, u." + colName + "_oldnull, " + "u." + colName + "_new, u." + colName + "_newnull, ");
			}
			query.append("u.peer, u.tid, u.serialno ");
			query.append("FROM updates_" + relname + " u, relevantTxns rt WHERE rt.truster = " + peerID + " AND rt.trusted = u.peer AND rt.tid = u.tid AND rt.recno = ? ORDER BY peer, tid, serialno");
			retrieveUpdateStmts.add(conn.prepareStatement(query.toString()));
		}

		getPrevTidsForRelevantTxnsStmt = conn.prepareStatement("SELECT ia.peer, ia.tid, ia.serialno, ia.prevpeer, ia.prevtid FROM immediateAntecedents ia, relevantTxns rt WHERE rt.truster = " + peerID + " AND rt.trusted = ia.peer AND rt.tid = ia.tid AND rt.recno = ? ORDER BY peer, tid, serialno");
		getPrevTidsStmt = cursorConn.prepareStatement("SELECT serialno, prevpeer, prevtid FROM immediateAntecedents WHERE peer = " + pidParam + " AND tid = ?");
		getAcceptedForRecnoPrevTidsStmt = conn.prepareStatement("SELECT ia.peer, ia.tid, ia.serialno, ia.prevPeer, ia.prevtid FROM immediateAntecedents ia, accepted a WHERE a.truster = " + peerID + " AND a.recno = ? AND a.trusted = ia.peer AND a.tid = ia.tid");

		StringBuilder sb = new StringBuilder("INSERT INTO trustedTxns ");
		sb.append("SELECT " + peerID + " AS truster, rn.recno, up.peer AS trusted, tid, MAX(prio) AS prio FROM (");
		for (int i = 0; i < numRelations; ++i) {
			Relation rs = schema.getRelationSchema(i);
			TreeMap<Integer,Set<Trusts>> condsForRel = trustConds.get(i);
			if (i != 0) {
				sb.append(" UNION ");
			}
			sb.append("(SELECT peer, tid, epoch, CASE WHEN peer = " + peerID + " THEN " + TrustConditions.OWN_TXN_PRIORITY);
			if (condsForRel != null) {
				for (Map.Entry<Integer,Set<Trusts>> me : condsForRel.entrySet()) {
					Set<Trusts> conds = me.getValue();
					int prio = me.getKey();
					for (Trusts cond : conds) {
						sb.append(" WHEN peer = peerId(" + getSQLLit(cond.trustedPeer.serialize()) + ")");
						if (cond.condition  != null) {
							sb.append(" AND (("+ cond.condition.getSqlCondition(rs,"","_old") + ") OR (" + cond.condition.getSqlCondition(rs,"","_new") + "))");
						}
						sb.append(" THEN " + prio);
					}
				}
			}

			sb.append(" ELSE " + (TrustConditions.OWN_TXN_PRIORITY + 1) + " END AS prio FROM updates_" + schema.getNameForID(i) + ")");
		}
		sb.append(") AS up, recnos rn ");
		sb.append("WHERE rn.peer = " + peerID + " AND rn.recno = ? ");
		sb.append("AND up.epoch <= rn.epoch ");
		sb.append("AND NOT EXISTS (SELECT * FROM recnos rnn WHERE rnn.peer = rn.peer AND rnn.recno < rn.recno AND rnn.epoch >= up.epoch)");
		sb.append("GROUP BY up.peer, tid, recno HAVING MAX(prio) < " + (TrustConditions.OWN_TXN_PRIORITY + 1));

		findTrustedTxnsStmt = conn.prepareStatement(sb.toString());

		recordAntecedentsStmt = conn.prepareStatement("INSERT INTO immediateAntecedents VALUES(" + peerID + ",?,?," + pidParam + ",?)");

		clearRelevantTxnsStmt = conn.prepareStatement("DELETE FROM relevantTxns WHERE truster = " + peerID + " AND recno = ?");

		getTxnPriosStmt = conn.prepareStatement("SELECT trusted, tid, prio FROM trustedTxns tt WHERE truster = " + peerID + " AND recno = ? AND NOT EXISTS (SELECT * FROM accepted WHERE truster = tt.truster AND trusted = tt.trusted AND tid = tt.tid) AND NOT EXISTS (SELECT * FROM rejected WHERE truster = tt.truster AND trusted = tt.trusted AND tid = tt.tid)");

		insertTrustedTxnsStmt = conn.prepareStatement("INSERT INTO relevantTxns (SELECT truster, trusted, tid, recno FROM trustedTxns tt WHERE truster = " + peerID + " AND recno = ? AND NOT EXISTS (SELECT * FROM accepted WHERE truster = tt.truster AND trusted = tt.trusted AND tid = tt.tid) AND NOT EXISTS (SELECT * FROM rejected WHERE truster = tt.truster AND trusted = tt.trusted AND tid = tt.tid))");

		sb.setLength(0);
		sb.append("INSERT INTO relevantTxns ");
		sb.append("(SELECT DISTINCT truster, prevPeer AS trusted, prevTid AS tid, recno ");
		sb.append("FROM relevantTxns rt, immediateAntecedents ia ");
		sb.append("WHERE truster = " + peerID + " AND recno = ? ");
		sb.append("AND ia.peer = rt.trusted AND ia.tid = rt.tid AND ia.prevPeer IS NOT NULL AND ia.prevTid IS NOT NULL ");
		sb.append("AND NOT EXISTS (SELECT * FROM accepted a WHERE a.truster = rt.truster AND a.trusted = ia.prevPeer AND a.tid = ia.prevTid) ");
		sb.append("AND NOT EXISTS (SELECT * FROM rejected r WHERE r.truster = rt.truster AND r.trusted = ia.prevPeer AND r.tid = ia.prevTid) ");
		sb.append("AND NOT EXISTS (SELECT * FROM relevantTxns rtt WHERE rtt.truster = rt.truster AND rtt.trusted = ia.prevPeer AND rtt.tid = ia.prevTid AND rtt.recno = rt.recno))");
		updateRelevantTxnsStmt = conn.prepareStatement(sb.toString());

		txnAcceptedStmt = conn.prepareStatement("SELECT recno FROM accepted WHERE truster = " + peerID + " AND trusted = " + pidParam + " AND tid = ?");
		txnRejectedStmt = conn.prepareStatement("SELECT recno FROM rejected WHERE truster = " + peerID + " AND trusted = " + pidParam + " AND tid = ?");

		decisionsStmt = cursorConn.prepareStatement("SELECT trusted, tid, accepted FROM (SELECT recno, truster, trusted, tid, 1 AS accepted FROM accepted UNION SELECT recno, truster, trusted, tid, 0 AS accepted FROM rejected) AS decisions(recno, truster, trusted, tid, accepted) WHERE truster = " + peerID + " AND recno = ? ORDER BY accepted DESC, trusted, tid",
				ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		reconciliationsStmt = cursorConn.prepareStatement("SELECT recno, epoch FROM recnos WHERE peer = " + peerID + " ORDER BY recno",
				ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

		getUpdatesForRelationStmts = new ArrayList<PreparedStatement>(numRelations);
		getTransactionStmts = new ArrayList<PreparedStatement>(numRelations);

		for (int i = 0; i < numRelations; ++i) {
			String relname = schema.getNameForID(i);
			String queryStart = "SELECT * FROM updates_" + relname + " u";
			String query1End = " ORDER BY epoch, peer, tid, serialno";
			String query2End = " WHERE peer = " + pidParam + " AND tid = ? ORDER BY serialno";
			getUpdatesForRelationStmts.add(cursorConn.prepareStatement(queryStart + query1End, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY));
			getTransactionStmts.add(cursorConn.prepareStatement(queryStart + query2End));
		}

		getFirstReconTxnsStmt = cursorConn.prepareStatement("SELECT t.peer, t.tid FROM tids t, recnos r WHERE t.epoch <= r.epoch AND r.recno = " + StateStore.FIRST_RECNO + " AND r.peer = " + peerID + " ORDER BY t.epoch, t.peer, t.tid",
				ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		getLastReconTxnsStmt = cursorConn.prepareStatement("SELECT t.peer, t.tid FROM tids t, recnos r WHERE t.epoch > r.epoch AND r.recno = (SELECT MAX(recno) FROM recnos rr WHERE rr.peer = r.peer) AND r.peer = " + peerID + " ORDER BY t.epoch, t.peer, t.tid",
				ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		getReconTxnsStmt = cursorConn.prepareStatement("SELECT t.peer, t.tid FROM tids t, recnos r1, recnos r2 WHERE t.epoch > r1.epoch AND t.epoch <= r2.epoch AND r2.recno = ? AND r2.peer = " + peerID + " AND r1.peer = r2.peer AND r1.recno = r2.recno - 1 ORDER BY t.epoch, t.peer, t.tid",
				ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		getAllTxnsStmt = cursorConn.prepareStatement("SELECT t.peer, t.tid FROM tids t ORDER BY t.epoch, t.peer, t.tid",
				ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

		getAcceptedForRecnoStmts = new ArrayList<PreparedStatement>(numRelations);
		for (int i = 0; i < numRelations; ++i) {
			String relname = schema.getNameForID(i);
			getAcceptedForRecnoStmts.add(conn.prepareStatement("SELECT * FROM updates_" + relname + " u  WHERE (u.peer, u.tid) IN (SELECT trusted, tid FROM accepted a WHERE a.truster = " + peerID + " AND recno = ?)"));
		}

		hasFoundTrustedTxnsStmt = conn.prepareStatement("SELECT trustedComputed FROM recnos WHERE peer = " + peerID + " AND recno = ?");
		setFoundTrustedTxnsStmt = conn.prepareStatement("UPDATE recnos SET trustedComputed = 1 WHERE peer = " + peerID + " AND recno = ?");
		getMaxTidStmt = conn.prepareStatement("SELECT MAX(tid) FROM tids WHERE peer = " + peerID);
	}

	/**
	 * Drop the tables from the SQL database (if they exist).
	 * @throws SQLException
	 * 
	 */
	private static void dropDatabase(Connection conn) throws SQLException
	{
		boolean auto = conn.getAutoCommit();
		conn.setAutoCommit(true);
		Statement s = conn.createStatement();

		Set<String> tables = getUpdatesTables(s);
		tables.addAll(Arrays.asList(nonUpdateTables));

		// Ignore SQL errors, since they probably mean one of the objects
		// doesn't exist
		for (String table : tables) {
			try {
				s.execute("DROP TABLE " + table);
			}
			catch (SQLException e) {
			}
		}

		for (String sequence : sequences) {
			try {
				s.execute("DROP SEQUENCE " + sequence);
			}
			catch (SQLException e) {
			}
		}

		try {
			s.execute("DROP DISTINCT TYPE orchestr.peerId");
		} catch (SQLException e) {
		}
		s.close();
		conn.setAutoCommit(auto);
	}

	/**
	 * Make sure the database has the correct tables and schema
	 * 
	 * @return	<code>true</code> if it does, <code>false</code> if it
	 * 			does not
	 */
	private static SQLException checkDatabase(Schema schema, Statement st)
	{
		try {
			st.execute("SELECT COUNT(*) FROM recnos");
			st.execute("SELECT COUNT(*) FROM accepted");
			st.execute("SELECT COUNT(*) FROM rejected");
			st.execute("SELECT COUNT(*) FROM publishEpochs");
			for (Relation s : schema.getRelationSchemas()) {
				StringBuilder sb = new StringBuilder();
				final int numCols = s.getNumCols();
				sb.append(s.getColName(0) + "_old");
				for (int i = 0; i < numCols; ++i) {
					sb.append(", " + s.getColName(i) + "_old");
				}
				st.execute("SELECT " + sb + " FROM updates_" + s.getRelationName());
			}
			st.execute("SELECT COUNT(*) FROM trustedTxns");
			st.execute("SELECT COUNT(*) FROM relevantTxns");
			st.execute("SELECT COUNT(*) FROM immediateAntecedents");
			st.execute("SELECT COUNT(*) FROM tids");
		} catch (SQLException e) {
			return e;
		}
		return null;
	}


	/**
	 * Create all of the needed tables in the SQL database
	 * 
	 * @throws Exception
	 */
	private static void createDatabase(Schema schema, Statement s) throws SQLException
	{
		s.execute("CREATE DISTINCT TYPE orchestr.peerId AS VARCHAR(" + pidLength + ") WITH COMPARISONS");
		s.execute("CREATE TABLE immediateAntecedents(peer peerId NOT NULL, tid INT NOT NULL, serialno INT NOT NULL, prevPeer peerId NOT NULL, prevTid INT NOT NULL, PRIMARY KEY (peer,tid,serialno,prevpeer,prevtid))");
		s.execute("CREATE INDEX ia1 on immediateAntecedents(peer, tid, prevPeer, prevTid) ALLOW REVERSE SCANS");
		s.execute("CREATE TABLE recnos(peer peerId NOT NULL, recno INT NOT NULL, epoch INT NOT NULL, trustedComputed INT NOT NULL, PRIMARY KEY(peer,recno))");
		s.execute("CREATE UNIQUE INDEX r1 ON recnos(peer ASC, recno DESC) include(epoch) ALLOW REVERSE SCANS");
		s.execute("CREATE TABLE accepted(truster peerId NOT NULL, recno INT NOT NULL, trusted peerId NOT NULL, tid INT NOT NULL, " +
		"PRIMARY KEY(truster,trusted,tid))");
		s.execute("CREATE TABLE rejected(truster peerId NOT NULL, recno INT NOT NULL, trusted peerId NOT NULL, tid INT NOT NULL, " +
		"PRIMARY KEY(truster,trusted,tid))");
		s.execute("CREATE TABLE publishEpochs(peer peerId NOT NULL, epoch INT NOT NULL PRIMARY KEY, finished INT NOT NULL)");
		s.execute("CREATE INDEX pe1 ON publishEpochs(finished ASC, epoch DESC) ALLOW REVERSE SCANS");
		s.execute("CREATE INDEX pe2 ON publishEpochs(peer ASC, finished DESC) ALLOW REVERSE SCANS");
		for (Relation table : schema.getRelationSchemas()) {
			StringBuilder createColsPair = new StringBuilder();
			final int numCols = table.getNumCols();
			for (int i = 0; i < numCols; ++i) {
				Type st = table.getColType(i);
				String name = table.getColName(i);
				createColsPair.append(name + "_old " + st.getSQLTypeName() + ", " 
						+ name + "_oldnull INT, "
						+ name + "_new " + st.getSQLTypeName() + ", "
						+ name + "_newnull INT");
				if (i != (numCols - 1)) {
					createColsPair.append(", ");
				}
			}
			s.execute("CREATE TABLE updates_" + table.getRelationName() +
					"(peer peerId NOT NULL, tid INT NOT NULL, serialno INT NOT NULL, epoch INT NOT NULL, " +
					createColsPair + ", PRIMARY KEY(peer, tid, serialno))");
			s.execute("CREATE INDEX u_" + table.getRelationName() + " ON updates_" + table.getRelationName() +"(peer, tid, epoch)");
		}
		s.execute("CREATE TABLE trustedTxns(truster peerId NOT NULL, recno INT NOT NULL, trusted peerId NOT NULL, tid INT NOT NULL, prio INT NOT NULL, PRIMARY KEY(truster,trusted,tid))");
		s.execute("CREATE INDEX tt1 ON trustedTxns(truster,recno)");
		s.execute("CREATE SEQUENCE epochNum AS INTEGER MINVALUE 0 NO CYCLE ORDER");

		s.execute("CREATE TABLE relevantTxns(truster peerId NOT NULL, trusted peerId NOT NULL, tid INT NOT NULL, recno INT NOT NULL, PRIMARY KEY (truster,trusted,tid,recno))");

		s.execute("CREATE INDEX tt2 ON trustedTxns(truster,recno,trusted,tid)");		
		s.execute("CREATE INDEX rt1 ON relevantTxns(truster,recno,trusted,tid)");

		final int numRelations = schema.getNumRelations();
		StringBuilder createTids = new StringBuilder("CREATE VIEW tids AS ");
		if (numRelations > 1) {
			createTids.append("(");
		}
		for (int i = 0; i < numRelations; ++i) {
			createTids.append("(SELECT DISTINCT peer, tid, epoch FROM updates_" + schema.getNameForID(i) + ")");
			if (i != (numRelations - 1)) {
				createTids.append(" UNION ");
			}
		}
		if (numRelations > 1) {
			createTids.append(")");
		}
		s.execute(createTids.toString());
	}

	private String getSQLLit(String s) {
		StringBuilder sb = new StringBuilder("'");

		final int length = s.length();
		for (int i = 0; i < length; ++i) {
			char c = s.charAt(i);
			if (c == '\'') {
				sb.append("''");
			} else {
				sb.append(c);
			}
		}

		sb.append("'");

		return sb.toString();
	}

	@Override
	public void publish(List<List<Update>> txns) throws USException {
		try {
			if (benchmark != null) {
				getExecTime();
				resetElapsedTime();
			}

			startPublishStmt.execute();
			conn.commit();

			if (benchmark != null) {
				long netTime = getElapsedTime();
				long execTime = getExecTime();
				benchmark.publishServer += execTime;
				benchmark.publishNet += netTime - execTime;
				resetElapsedTime();
			}

			for (List<Update> txn : txns) {
				int serialno = 0;
				for (Update u : txn) {
					final int relId = u.getRelationID();
					PreparedStatement aus = addUpdateStmts.get(relId);
					aus.setInt(1, u.getLastTid().getTid());
					aus.setInt(2, serialno);
					recordAntecedentsStmt.setInt(1, u.getLastTid().getTid());
					recordAntecedentsStmt.setInt(2, serialno);
					for (TxnPeerID prev : u.getPrevTids()) {
						recordAntecedentsStmt.setString(3, prev.getPeerID().serialize());
						recordAntecedentsStmt.setInt(4, prev.getTid());
						recordAntecedentsStmt.addBatch();
					}
					final Relation rs = schema.getRelationSchema(relId);
					final int numCols = rs.getNumCols();
					Tuple oldTuple = u.getOldVal();
					Tuple newTuple = u.getNewVal();
					for (int i = 0; i < numCols; ++i) {
						final int type = rs.getColType(i).getSqlTypeCode();
						Object oldVal = oldTuple == null ? null : oldTuple.get(i);
						Object newVal = newTuple == null ? null : newTuple.get(i);
						if (oldTuple != null && oldTuple.isLabeledNull(i)) {
							// Set old value to null
							aus.setNull(3 + 4*i, type);
							try {
								// Set old value labeled null to label
								aus.setInt(3 + 4 * i + 1, oldTuple.getLabeledNull(i));
							} catch (Tuple.IsNotLabeledNull inln) {
								throw new USException("Inconsistent labeled null values from tuple");
							}
						} else if (oldVal == null) {
							// Set old value to null
							aus.setNull(3 + 4*i, type);
							// Set old value labeled null to null
							aus.setNull(3+ 4*i + 1, Types.INTEGER);
						} else {
							// Set old value
							aus.setObject(3 + 4*i, oldVal, type);
							// Set old value labeled null to null
							aus.setNull(3+ 4*i + 1, Types.INTEGER);
						}
						if (newTuple != null && newTuple.isLabeledNull(i)) {
							// Set new value to null
							aus.setNull(3 + 4*i + 2, type);
							try {
								// Set new value labeled null to label
								aus.setInt(3 + 4 * i + 3, newTuple.getLabeledNull(i));
							} catch (Tuple.IsNotLabeledNull inln) {
								throw new USException("Inconsitent labeled null values from tuple");
							}
						} else if (newVal == null) {
							// Set new value to null
							aus.setNull(3 + 4*i + 2, type);
							// Set new value labeled null to null
							aus.setNull(3+ 4*i + 3, Types.INTEGER);
						} else {
							// Set new value
							aus.setObject(3 + 4*i + 2, newVal, type);
							// Set new value labeled null to null
							aus.setNull(3+ 4*i + 3, Types.INTEGER);
						}
					}
					aus.addBatch();
					++serialno;
				}
			}

			if (benchmark != null) {
				benchmark.publish += getElapsedTime(true);
			}

			try {
				recordAcceptedTxnStmt.executeBatch();
				recordAntecedentsStmt.executeBatch();

				for (PreparedStatement aus : addUpdateStmts) {
					aus.executeBatch();
				}
			} catch (SQLException sqle) {
				SQLException batchException = sqle.getNextException();
				if (batchException != null) {
					System.out.println("SQL Batch error:");
					while (batchException != null) {
						batchException.printStackTrace(System.out);
						System.out.println("--------");
						batchException = batchException.getNextException();
					}
				}
				throw sqle;
			}

			finishPublishStmt.execute();

			conn.commit();

			if (benchmark != null) {
				long netTime = getElapsedTime();
				long execTime = getExecTime();

				benchmark.publishServer += execTime;
				benchmark.publishNet += netTime - execTime;
			}

		} catch (SQLException sqle) {
			throw new USException("Error while publishing: " + sqle.getMessage(), sqle);
		}
	}

	@Override
	public void recordTxnDecisionsImpl(Iterable<Decision> decisions) throws USException {
		try {
			if (benchmark != null) {
				resetElapsedTime();
			}
			HashSet<TxnPeerID> processedTxns = new HashSet<TxnPeerID>();
			for (Decision td : decisions) {
				if (processedTxns.contains(td.tpi)) {
					continue;
				}
				if (td.accepted) {
					recordAcceptedTxnStmt.setInt(1, td.recno);
					recordAcceptedTxnStmt.setString(2, td.tpi.getPeerID().serialize());
					recordAcceptedTxnStmt.setInt(3, td.tpi.getTid());
					recordAcceptedTxnStmt.addBatch();
				} else {
					recordRejectedTxnStmt.setInt(1, td.recno);
					recordRejectedTxnStmt.setString(2, td.tpi.getPeerID().serialize());
					recordRejectedTxnStmt.setInt(3, td.tpi.getTid());
					recordRejectedTxnStmt.addBatch();
				}
				processedTxns.add(td.tpi);
			}
			if (benchmark != null) {
				benchmark.recordTxnDecisions += getElapsedTime();
				getExecTime();
				resetElapsedTime();
			}
			try {
				recordRejectedTxnStmt.executeBatch();
				recordAcceptedTxnStmt.executeBatch();
			} catch (SQLException sqle) {
				SQLException batchException = sqle.getNextException();
				if (batchException != null) {
					System.out.println("SQL Batch error:");
					while (batchException != null) {
						batchException.printStackTrace(System.out);
						System.out.println("--------");
						batchException = batchException.getNextException();
					}
				}
				throw sqle;
			}

			conn.commit();
			if (benchmark != null) {
				long netTime = getElapsedTime();
				long execTime = getExecTime();
				benchmark.recordTxnDecisionsNet += netTime - execTime;
				benchmark.recordTxnDecisionsServer += execTime;
			}


		} catch (SQLException sqle) {
			throw new USException("Error while recording transaction decisions: " + sqle.getMessage(), sqle);
		}
	}

	@Override
	public void recordReconcile(boolean empty) throws USException {
		try {
			if (benchmark != null) {
				getExecTime();
				resetElapsedTime();
			}
			if (empty == true) {
				if ((getCurrentRecno() - 1) < StateStore.FIRST_RECNO) {
					throw new IllegalArgumentException("Cannot have empty first reconciliation");
				}
				recordEmptyReconcileStmt.executeUpdate();
			} else {
				recordReconcileStmt.executeUpdate();
			}
			conn.commit();
			if (benchmark != null) {
				long netTime = getElapsedTime();
				long execTime = getExecTime();
				benchmark.recordReconcileNet += netTime - execTime;
				benchmark.recordReconcileServer += execTime;
			}
		} catch (SQLException sqle) {
			throw new USException("Error while recording reconciliation: " + sqle.getMessage(), sqle);
		}

	}

	@Override
	public void getReconciliationData(int recno, final Set<TxnPeerID> alreadyAccepted,
			Map<Integer, List<TxnChain>> trustedTxns, final Set<TxnPeerID> mustReject)
	throws USException {
		if (trustConds.isEmpty()) {
			return;
		}
		HashMap<TxnPeerID,Integer> priorities = new HashMap<TxnPeerID,Integer>();
		final HashMap<TxnPeerID,List<Update>> relevantTransactions = new HashMap<TxnPeerID,List<Update>>();

		TransactionSource ts = new TransactionSource() {
			public List<Update> getTxn(TxnPeerID tpi) {
				return relevantTransactions.get(tpi);
			}
		};

		TransactionDecisions td = new TransactionDecisions() {

			public boolean hasAcceptedTxn(TxnPeerID tpi) throws USException {
				return (alreadyAccepted.contains(tpi) || SqlUpdateStore.this.hasAcceptedTxn(tpi));
			}

			public boolean hasRejectedTxn(TxnPeerID tpi) throws USException {
				return SqlUpdateStore.this.hasRejectedTxn(tpi);
			}

		};

		try {
			if (benchmark != null) {
				getExecTime();
				resetElapsedTime();
			}

			hasFoundTrustedTxnsStmt.setInt(1, recno);
			ResultSet rs = hasFoundTrustedTxnsStmt.executeQuery();
			Boolean hasFound = null;
			while (rs.next()) {
				hasFound = rs.getInt(1) != 0;
			}
			rs.close();
			if (hasFound == null) {
				throw new USException("Have not recorded reconciliation " + recno);
			} else if (hasFound == false) {
				// If we haven't already done so, compute the trusted transactions
				findTrustedTxnsStmt.setInt(1, recno);
				findTrustedTxnsStmt.executeUpdate();
				setFoundTrustedTxnsStmt.setInt(1, recno);
				setFoundTrustedTxnsStmt.executeUpdate();
			}

			clearRelevantTxnsStmt.setInt(1,recno);
			clearRelevantTxnsStmt.executeUpdate();
			insertTrustedTxnsStmt.setInt(1,recno);
			insertTrustedTxnsStmt.executeUpdate();
			int updateCount = 0;
			do {
				updateRelevantTxnsStmt.setInt(1,recno);
				updateCount = updateRelevantTxnsStmt.executeUpdate();
			} while (updateCount > 0);

			getTxnPriosStmt.setInt(1,recno);
			rs = getTxnPriosStmt.executeQuery();

			if (benchmark != null) {
				long netTime = getElapsedTime();
				long execTime = getExecTime();
				benchmark.getReconciliationDataServer += execTime;
				benchmark.getReconciliationDataNet += netTime - execTime;
				resetElapsedTime();
			}

			while (rs.next()) {

				AbstractPeerID trusted = AbstractPeerID.deserialize(rs.getString(1));
				int tid = rs.getInt(2);
				int prio = rs.getInt(3);
				TxnPeerID tpi = new TxnPeerID(tid, trusted);
				if (! alreadyAccepted.contains(tpi)) {
					priorities.put(tpi, prio);
				}
			}

			if (benchmark != null) {
				benchmark.getReconciliationData += getElapsedTime();
			}

			rs.close();

			final int numRel = schema.getNumRelations();
			for (int i = 0; i < numRel; ++i) {
				PreparedStatement ps = retrieveUpdateStmts.get(i);
				ps.setInt(1,recno);
				if (benchmark != null) {
					resetElapsedTime();
				}
				rs = ps.executeQuery();
				if (benchmark != null) {
					long netTime = getElapsedTime();
					long execTime = getExecTime();
					benchmark.getReconciliationDataServer += execTime;
					benchmark.getReconciliationDataNet += netTime - execTime;
					resetElapsedTime();
				}

				parseUpdates(schema.getRelationSchema(i), rs, relevantTransactions);

				rs.close();
			}

			getPrevTidsForRelevantTxnsStmt.setInt(1, recno);
			rs = getPrevTidsForRelevantTxnsStmt.executeQuery();

			if (benchmark != null) {
				long netTime = getElapsedTime();
				long execTime = getExecTime();
				benchmark.getReconciliationDataServer += execTime;
				benchmark.getReconciliationDataNet += netTime - execTime;
				resetElapsedTime();
			}

			while (rs.next()) {
				TxnPeerID tpi = new TxnPeerID(rs.getInt(2), AbstractPeerID.deserialize(rs.getString(1)));
				int serialno = rs.getInt(3);
				TxnPeerID prevTpi = new TxnPeerID(rs.getInt(5), AbstractPeerID.deserialize(rs.getString(4)));
				relevantTransactions.get(tpi).get(serialno).addPrevTid(prevTpi);
			}

			rs.close();

			conn.commit();

			resetElapsedTime();

			// Build the transaction chains for the fully trusted transactions with
			// no rejected antecedents
			for (Map.Entry<TxnPeerID, Integer> trustedTxn : priorities.entrySet()) {
				try {
					TxnChain tc = new TxnChain(trustedTxn.getKey(), ts, td);
					List<TxnChain> txns = trustedTxns.get(trustedTxn.getValue());
					if (txns == null) {
						txns = new ArrayList<TxnChain>();
						trustedTxns.put(trustedTxn.getValue(), txns);
					}
					txns.add(tc);
				} catch (AlreadyRejectedAntecedent are) {
					mustReject.add(trustedTxn.getKey());
				}
			}

			if (benchmark != null) {
				benchmark.getReconciliationData += getElapsedTime();
			}

		} catch (SQLException sqle) {
			throw new USException("Error while retrieving reconciliation data: " + sqle.getMessage(), sqle);
		} catch (PeerIDFormatException e) {
			throw new USException("Malformed data from SQL database", e);
		}		
	}

	private static Update parseUpdate(ResultSet rs, Relation s) throws SQLException, ValueMismatchException, PeerIDFormatException {
		final int numCols = s.getNumCols();

		AbstractPeerID pi = AbstractPeerID.deserialize(rs.getString("peer"));
		TxnPeerID tpi = new TxnPeerID(rs.getInt("tid"), pi);
		Tuple oldVal = new Tuple(s), newVal = new Tuple(s);


		for (int i = 0; i < numCols; ++i) {
			Type t = s.getColType(i);
			String colName = s.getColName(i);
			Object oldColVal = t.getFromResultSet(rs, colName + "_old");
			if (! rs.wasNull()) {
				oldVal.set(i, oldColVal);
			}
			Object newColVal = t.getFromResultSet(rs, colName + "_new");
			if (! rs.wasNull()) {
				newVal.set(i, newColVal);
			}
			int oldColLabeledNull = rs.getInt(colName + "_oldnull");
			if (! rs.wasNull()) {
				oldVal.setLabeledNull(i, oldColLabeledNull);
			}
			int newColLabeledNull = rs.getInt(colName + "_newnull");
			if (! rs.wasNull()) {
				newVal.setLabeledNull(i, newColLabeledNull);
			}
		}

		Update u = new Update(oldVal, newVal);
		u.addTid(tpi);

		return u;
	}

	/**
	 * Parses ResultSet containing at least the columns peer, tid, serialno
	 * and the old and new values columns
	 * 
	 * @param s			The RelationSchema of the udpates table being queried
	 * @param rs		The ResultSet to extract the row from
	 * @param txns		A mapping for txn id to a list of updates for
	 * 					that transaction. New txns will be added by this method
	 * 					as needed, and entries added to existing txns.
	 * @throws SQLException
	 * @throws PeerIDFormatException 
	 * @throws USException
	 */
	private static void parseUpdates(Relation s, ResultSet rs, Map<TxnPeerID,List<Update>> txns)  throws SQLException, PeerIDFormatException, USException {
		// Make sure that we don't add a transaction that's already in txns
		HashSet<TxnPeerID> alreadySeenThisTime = new HashSet<TxnPeerID>();
		HashSet<TxnPeerID> skip = new HashSet<TxnPeerID>();

		int peerCol = rs.findColumn("peer");
		int tidCol = rs.findColumn("tid");
		int serialnoCol = rs.findColumn("serialno");

		final int numCols = s.getNumCols();
		int oldColPos[] = new int[numCols];
		int newColPos[] = new int[numCols];
		int oldNullColPos[] = new int[numCols];
		int newNullColPos[] = new int[numCols];
		Type colTypes[] = new Type[numCols];

		for (int i = 0; i < numCols; ++i) {
			String colname = s.getColName(i);
			oldColPos[i] = rs.findColumn(colname + "_old");
			newColPos[i] = rs.findColumn(colname + "_new");
			oldNullColPos[i] = rs.findColumn(colname + "_oldnull");
			newNullColPos[i] = rs.findColumn(colname + "_newnull");
			colTypes[i] = s.getColType(i);
		}


		while (rs.next()) {
			Tuple oldVal = new Tuple(s);
			Tuple newVal = new Tuple(s);
			int serialno = rs.getInt(serialnoCol);
			AbstractPeerID pi = AbstractPeerID.deserialize(rs.getString(peerCol));
			TxnPeerID tpi = new TxnPeerID(rs.getInt(tidCol), pi);
			if (! alreadySeenThisTime.contains(tpi)) {
				alreadySeenThisTime.add(tpi);
				if (txns.get(tpi) != null) {
					skip.add(tpi);
				}
			}
			if (skip.contains(tpi)) {
				// Txn was already in cache, so we don't want to add its updates
				// again
				continue;
			}

			for (int i = 0; i < numCols; ++i) {
				try {
					Object oldColVal = colTypes[i].getFromResultSet(rs, oldColPos[i]);
					if (! rs.wasNull()) {
						oldVal.set(i, oldColVal);					
					}
					Object newColVal = colTypes[i].getFromResultSet(rs, newColPos[i]);
					if (! rs.wasNull()) {
						newVal.set(i, newColVal);

					}
				}
				catch (ValueMismatchException vm) {
					throw new USException("Error reading update from SQL database", vm);
				}
				int oldColLabeledNull = rs.getInt(oldNullColPos[i]);
				if (! rs.wasNull()) {
					oldVal.setLabeledNull(i, oldColLabeledNull);
				}
				int newColLabeledNull = rs.getInt(newNullColPos[i]);
				if (! rs.wasNull()) {
					newVal.setLabeledNull(i, newColLabeledNull);
				}
			}

			Update u = new Update(oldVal, newVal);
			u.addTid(tpi);
			List<Update> txn = txns.get(tpi);
			if (txn == null) {
				txn = new ArrayList<Update>();
				txns.put(tpi.duplicate(),txn);
			}
			// Make sure that this update ends up at the appropriate place in
			// the transaction
			while (txn.size() <= serialno) {
				txn.add(null);
			}
			txn.set(serialno, u);
		}
	}

	@Override
	public int getCurrentRecno() throws USException {
		try {
			if (benchmark != null) {
				getExecTime();
				resetElapsedTime();
			}
			int retval = getCurrentRecnoNoCommit();
			conn.commit();
			if (benchmark != null) {
				long netTime = getElapsedTime();
				long execTime = getExecTime();
				benchmark.getCurrentRecnoNet += netTime - execTime;
				benchmark.getCurrentRecnoServer += execTime;
			}
			return retval;
		} catch (SQLException sqle) {
			throw new USException("Error while retrieving reconciliation number: " + sqle.getMessage(), sqle);
		}
	}

	private int getCurrentRecnoNoCommit() throws SQLException {
		ResultSet rs = getRecnoStmt.executeQuery();
		rs.next();
		int retval = rs.getInt(1);
		if (rs.wasNull()) {
			// Peer hasn't reconciled yet
			rs.close();
			return StateStore.FIRST_RECNO;
		} else {
			rs.close();
			return retval + 1;
		}
	}

	public Benchmark getBenchmark() {
		return benchmark;
	}

	//private long execTime = 0;

	/**
	 * Get the execution time on the server for this participant
	 * since the last call to this function
	 * 
	 * @return	The execution time, in nanoseconds
	 * @throws SQLException
	 * @throws USException
	 */
	private long getExecTime() throws SQLException, USException {
		return 0;
		/*
		ResultSet rs = getElapsedExecTimeStmt.executeQuery();

		if (! rs.next()) {
			throw new USException("Couldn't retrieve elapsed execution time");
		}

		// Want execution time in nanoseconds
		long currTime = rs.getLong(1) * 1000;

		if (rs.next()) {
			throw new USException("Should only get one row back from getElapsedExecTimeStmt");
		}

		rs.close();

		long retval = currTime - execTime;

		if (retval < 0) {
			System.err.println("Statement took negative execution time: " + currTime + " - " + execTime);
		}

		execTime = currTime;
		return retval;
		 */
	}

	private long startTime = 0;

	private void resetElapsedTime() {
		getElapsedTime(true);
	}

	private long getElapsedTime() {
		return getElapsedTime(false);
	}

	private long getElapsedTime(boolean reset) {
		long currTime = System.nanoTime();
		long retval = currTime - startTime;
		if (reset) {
			startTime = currTime;
		}
		return retval;
	}

	public void setBenchmark(Benchmark b) throws USException {
//		boolean hadBenchmark = (benchmark != null);
		benchmark = b;
		/*
		try {
			if (b == null && hadBenchmark) {
				getElapsedExecTimeStmt.close();
				benchConn.close();
				getElapsedExecTimeStmt = null;
				benchConn.close();
			} else if (b != null && (! hadBenchmark)) {
				benchConn = DriverManager.getConnection(dbUrl, connProp.getProperty("user"), connProp.getProperty("password"));

				String getElapsedExecTimeQuery = "select" +
				// This doesn't seem to be working correctly in DB2 right now...
				//	"elapsed_exec_time_ms" +
				" (agent_usr_cpu_time_ms + agent_sys_cpu_time_ms) AS exec_time_ms" +
				" from table (snapshot_appl_info('ORCHESTR',-1)) as snap_appl_info," +
				" table (snapshot_appl ('ORCHESTR',-1)) as snap_appl" +
				" where tpmon_client_app = '" + connProp.getProperty("clientApplicationInformation") + "' AND" +
				" tpmon_acc_str = '" + connProp.getProperty("clientAccountingInformation") + "'" +
				" AND snap_appl_info.agent_id = snap_appl.agent_id";

				getElapsedExecTimeStmt = benchConn.prepareStatement(getElapsedExecTimeQuery);
			}
		} catch (SQLException sqle) {
			throw new USException(sqle);
		}
		 */
	}

	@Override
	protected TxnStatus getTxnStatus(TxnPeerID tpi) throws USException {
		try {
			TxnStatus retval = null;
			txnAcceptedStmt.setString(1, tpi.getPeerID().serialize());
			txnAcceptedStmt.setInt(2, tpi.getTid());
			ResultSet rs = txnAcceptedStmt.executeQuery();
			if (rs.next()) {
				// Transaction is accepted;
				int recno = rs.getInt(1);
				retval =  TxnStatus.acceptedAt(recno);
			}
			rs.close();
			if (retval != null) {
				conn.commit();
				return retval;
			}

			txnRejectedStmt.setString(1, tpi.getPeerID().serialize());
			txnRejectedStmt.setInt(2, tpi.getTid());
			rs = txnRejectedStmt.executeQuery();
			if (rs.next()) {
				int recno = rs.getInt(1);
				retval = TxnStatus.rejectedAt(recno);
			}
			rs.close();
			conn.commit();
			if (retval != null) {
				return retval;
			}

			return TxnStatus.undecided();
		} catch (SQLException sqle) {
			throw new USException(sqle);
		}

	}

	@Override
	public List<Decision> getDecisions(int recno) throws USException {
		List<Decision> retval = new ArrayList<Decision>();
		try {
			decisionsStmt.setInt(1, recno);
			ResultSet rs = decisionsStmt.executeQuery();
			while (rs.next()) {
				AbstractPeerID trusted = AbstractPeerID.deserialize(rs.getString(1));
				int tid = rs.getInt(2);
				boolean accepted = rs.getInt(3) > 0;
				retval.add(new Decision(new TxnPeerID(tid,trusted), recno, accepted));
			}
			rs.close();
		} catch (SQLException e) {
			throw new USException("Error retrieving decisions", e);
		} catch (PeerIDFormatException e) {
			throw new USException("Malformed data from SQL database", e);
		}
		return retval;
	}

	@Override
	public ResultSetIterator<ReconciliationEpoch> getReconciliations() throws USException {
		final ResultSet rs;
		try {
			rs = reconciliationsStmt.executeQuery();
		} catch (SQLException e) {
			throw new USException("Error retrieving reconciliations", e);
		}

		try {
			return new ResultSetIterator<ReconciliationEpoch>(rs) {

				@Override
				public ReconciliationEpoch readCurrent() throws IteratorException {
					try {
						return new ReconciliationEpoch(rs.getInt(1), rs.getInt(2));
					} catch (SQLException e) {
						throw new IteratorException(e);
					}
				}
			};
		} catch (SQLException e) {
			throw new USException(e);
		}
	}

	@Override
	public ResultSetIterator<Update> getPublishedUpdatesForRelation(String relname) throws USException {
		final Relation s = schema.getRelationSchema(relname);
		int relNum = s.getRelationID();
		if (s == null) {
			throw new USException("Couldn't find RelationSchema for " + relname);
		}
		try {
			final ResultSet rs = getUpdatesForRelationStmts.get(relNum).executeQuery();
			return new ResultSetIterator<Update>(rs) {

				@Override
				public Update readCurrent() throws IteratorException {
					try {
						return parseUpdate(rs, s);
					} catch (ValueMismatchException e) {
						throw new IteratorException(e);
					} catch (SQLException e) {
						throw new IteratorException(e);
					} catch (PeerIDFormatException e) {
						throw new IteratorException(e);
					}
				}

			};
		} catch (SQLException e) {
			throw new USException("Error retrieving updates for relation " + relname, e);
		}
	}

	@Override
	public List<Update> getTransaction(TxnPeerID txn) throws USException {
		HashMap<TxnPeerID,List<Update>> txns = new HashMap<TxnPeerID,List<Update>>();

		final int numRelations = schema.getNumRelations();
		for (int i = 0; i < numRelations; ++i) {
			try {
				PreparedStatement ps = getTransactionStmts.get(i);
				ps.setString(1, txn.getPeerID().serialize());
				ps.setInt(2, txn.getTid());
				ResultSet rs = ps.executeQuery();
				parseUpdates(schema.getRelationSchema(i), rs, txns);
				rs.close();
			} catch (SQLException e) {
				throw new USException("Error retrieving updates from relation " + schema.getNameForID(i) + " for txn " + txn, e);
			} catch (PeerIDFormatException e) {
				throw new USException("Malformed data from SQL database", e);
			}
		}

		if (! txns.containsKey(txn)) {
			return null;
		} else if (txns.size() > 1) {
			throw new USException("Received unexpected data from SQL database which retrieving txn " + txn);
		}

		List<Update> retval = txns.get(txn);

		try {
			getPrevTidsStmt.setString(1, txn.getPeerID().serialize());
			getPrevTidsStmt.setInt(2, txn.getTid());
			ResultSet rs = getPrevTidsStmt.executeQuery();

			while (rs.next()) {
				int serialno = rs.getInt(1);
				AbstractPeerID prevPeer = AbstractPeerID.deserialize(rs.getString(2));
				int prevTid = rs.getInt(3);
				retval.get(serialno).addPrevTid(new TxnPeerID(prevTid, prevPeer));
			}
			rs.close();
		} catch (SQLException e) {
			throw new USException("Error retrieving previous tids for transaction " + txn);
		} catch (PeerIDFormatException e) {
			throw new USException("Malformed data from SQL database", e);
		}

		return retval;

	}

	@Override
	public ResultSetIterator<TxnPeerID> getTransactionsForReconciliation(int recno) throws USException {
		try {
			final ResultSet rs;
			int currentRecno = getCurrentRecno();
			if (currentRecno == StateStore.FIRST_RECNO) {
				rs = getAllTxnsStmt.executeQuery();
			} else if (recno == StateStore.FIRST_RECNO) {
				rs = getFirstReconTxnsStmt.executeQuery();
			} else if (recno >= currentRecno) {
				rs = getLastReconTxnsStmt.executeQuery();
			} else {
				getReconTxnsStmt.setInt(1, recno);
				rs = getReconTxnsStmt.executeQuery();
			}


			return new ResultSetIterator<TxnPeerID>(rs) {

				@Override
				public TxnPeerID readCurrent() throws IteratorException {
					try {
						AbstractPeerID pid = AbstractPeerID.deserialize(rs.getString(1));
						int tid = rs.getInt(2);
						return new TxnPeerID(tid,pid);
					} catch (SQLException e) {
						throw new IteratorException(e);
					} catch (PeerIDFormatException e) {
						throw new IteratorException("Malformed data from SQL database", e);
					}
				}

			};


		} catch (SQLException e) {
			throw new USException("Error creating iterator of transactions for reconciliation " + recno, e);
		}
	}

	private static Set<String> getUpdatesTables(Statement s) throws SQLException {
		HashSet<String> retval = new HashSet<String>();
		ResultSet rs = s.executeQuery("SELECT tabname FROM syscat.tables WHERE tabschema = CURRENT SCHEMA AND UPPER(tabname) LIKE 'UPDATES_%'");
		while (rs.next()) {
			retval.add(rs.getString("tabname"));
		}
		rs.close();
		return retval;
	}

	@Override
	public Map<TxnPeerID, List<Update>> getTransactionsAcceptedAtRecno(int recno) throws USException {
		HashMap<TxnPeerID,List<Update>> retval = new HashMap<TxnPeerID,List<Update>>();

		try {
			final int numRelations = schema.getNumRelations();
			for (int i = 0; i < numRelations; ++i) {
				PreparedStatement ps = getAcceptedForRecnoStmts.get(i);
				ps.setInt(1, recno);
				ResultSet rs = ps.executeQuery();
				parseUpdates(schema.getRelationSchema(i), rs, retval);
				rs.close();
			}

			getAcceptedForRecnoPrevTidsStmt.setInt(1, recno);
			ResultSet rs = getAcceptedForRecnoPrevTidsStmt.executeQuery();
			while (rs.next()) {
				TxnPeerID tpi = new TxnPeerID(rs.getInt(2), AbstractPeerID.deserialize(rs.getString(1)));
				List<Update> txn = retval.get(tpi);
				int serialno = rs.getInt(3);
				AbstractPeerID prevPeer = AbstractPeerID.deserialize(rs.getString(4));
				int prevTid = rs.getInt(5);
				txn.get(serialno).addPrevTid(new TxnPeerID(prevTid, prevPeer));
			}
			rs.close();
		} catch (SQLException e) {
			throw new USException("Error retrieving transactions accepted at recno " + recno, e);
		} catch (PeerIDFormatException e) {
			throw new USException("Malformed data from SQL database", e);
		}

		return retval;
	}

	@Override
	public int getLargestTidForPeer() throws USException {
		try {
			ResultSet rs = getMaxTidStmt.executeQuery();
			try {
				if (! rs.next()) {
					return -1;
				}
				int last = rs.getInt(1);
				if (rs.wasNull()) {
					return -1;
				} else {
					return last;
				}
			} finally {
				rs.close();
			}
		} catch (SQLException e) {
			throw new USException(e);
		}
	}
}
