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
package edu.upenn.cis.orchestra.reconciliation.bdbstore;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DeadlockException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.ByteBufferReader;
import edu.upenn.cis.orchestra.datamodel.ByteBufferWriter;
import edu.upenn.cis.orchestra.datamodel.IntPeerID;
import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.StringPeerID;
import edu.upenn.cis.orchestra.datamodel.TrustConditions;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.datamodel.Update;
import edu.upenn.cis.orchestra.datamodel.Update.SerializationLevel;
import edu.upenn.cis.orchestra.predicate.AndPred;
import edu.upenn.cis.orchestra.predicate.ComparePredicate;
import edu.upenn.cis.orchestra.predicate.NotPred;
import edu.upenn.cis.orchestra.predicate.OrPred;
import edu.upenn.cis.orchestra.reconciliation.Decision;
import edu.upenn.cis.orchestra.reconciliation.StateStore;
import edu.upenn.cis.orchestra.reconciliation.TransactionDecisions;
import edu.upenn.cis.orchestra.reconciliation.TxnChain;
import edu.upenn.cis.orchestra.reconciliation.USDump;
import edu.upenn.cis.orchestra.reconciliation.USDump.RecnoEpoch;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore.AlreadyRejectedAntecedent;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore.TransactionSource;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore.TxnStatus;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore.USException;


public class BerkeleyDBStoreServer implements TransactionSource {
	public static final int DEFAULT_PORT = 9999;
	static final int MAX_DEADLOCK_RETRIES = 5;
	static final int FIRST_EPOCH = 0;

	static final String lastRecnoName = "lastRecno";
	static final String recnoEpochsName = "recnoEpochs";
	static final String epochContentsName = "epochContents";
	static final String decisionsName = "decisions";
	static final String reconAcceptedTxnsName = "reconAcceptedTxns";
	static final String reconRejectedTxnsName = "reconRejectedTxns";
	static final String txnsName = "txns";

	// Subclasses that must be loaded before we can deserialize anything
	@SuppressWarnings("unused")
	private static Class<?>[] classesToLoad = {IntPeerID.class,
		StringPeerID.class, AndPred.class,
		OrPred.class, NotPred.class,
		ComparePredicate.class};


	private Map<AbstractPeerID,Schema> schemas = null;

	private Environment env;

	// peer -> last reconciliation number
	private Database lastRecno;
	// (peer, recno) -> epoch
	private Database recnoEpochs;
	// epoch -> list of tids
	private Database epochContents;
	// (peer, tid) -> decision (accepted[boolean], recno[int])
	private Database decisions;
	// (peer, recno) -> tid (allows multiple values for each key)
	private Database reconAcceptedTxns;
	// (peer, recno) -> tid (allows multiple values for each key)
	private Database reconRejectedTxns;
	// tid -> transaction contents
	private Database txns;
	
	private SchemaIDBinding _mapStore;
	
	private final int port;

	private DatabaseConfig dc, reconTxnsDc;

//	File configFile;
	private LinkedList<WorkerThread> workers = new LinkedList<WorkerThread>();
	private ListenerThread listener;
	private ThreadGroup tg;

	public BerkeleyDBStoreServer(Environment env)
	throws IOException, ClassNotFoundException, DatabaseException {
		this(env,/*configFile,*/DEFAULT_PORT);
	}

	public BerkeleyDBStoreServer(Environment env, int port)
	throws IOException, ClassNotFoundException, DatabaseException {
		//Runtime.getRuntime().addShutdownHook(new ShutdownThread());
		//Runtime.getRuntime().addShutdownHook(new ShutdownThread());
		tg = new ThreadGroup("BerkeleyDBStoreServer on port " + port);
		this.port = port;


		schemas = new HashMap<AbstractPeerID,Schema>();

		// Open databases
		this.env = env;
		dc = new DatabaseConfig();
		dc.setAllowCreate(true);
		dc.setSortedDuplicates(false);
		dc.setTransactional(true);
		reconTxnsDc = new DatabaseConfig();
		reconTxnsDc.setAllowCreate(true);
		reconTxnsDc.setSortedDuplicates(true);
		reconTxnsDc.setTransactional(true);
		
		lastRecno = env.openDatabase(null, lastRecnoName, dc);
		recnoEpochs = env.openDatabase(null, recnoEpochsName, dc);
		epochContents = env.openDatabase(null, epochContentsName, dc);
		decisions = env.openDatabase(null, decisionsName, dc);
		reconAcceptedTxns = env.openDatabase(null, reconAcceptedTxnsName, reconTxnsDc);
		reconRejectedTxns = env.openDatabase(null, reconRejectedTxnsName, reconTxnsDc);
		txns = env.openDatabase(null, txnsName, dc);

		dc.setAllowCreate(false);
		reconTxnsDc.setAllowCreate(false);
		
		for (Class<?> obj : classesToLoad) {
			try {
				obj.newInstance();
			} catch (Exception e) {
				
			}
		}
		
		_mapStore = new SchemaIDBinding(env);
		
		// Start listener thread
		listener = new ListenerThread();
	}

	public void quit() throws IOException, DatabaseException, InterruptedException {
		quit(null);
	}
	
	public void quit(WorkerThread thread) throws IOException, DatabaseException, InterruptedException {
		// Shutdown listener thread
		logger.debug("Starting to quit BDB update store.");
		listener.interrupt();
		listener.join();


		
		// Close databases
		lastRecno.close();
		recnoEpochs.close();
		epochContents.close();
		decisions.close();
		txns.close();
		reconAcceptedTxns.close();
		reconRejectedTxns.close();
		_mapStore.quit();

		/*
		// Store non-database state
		if (configFile != null) {
			ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(configFile));
			oos.writeObject(schemas);
			oos.flush();
			oos.close();
		}*/
		this.env.close();
		logger.debug("BDBs closed up.");
		// Shutdown worker threads
		synchronized (workers) {
			for (WorkerThread wt : workers) {
				wt.requestExit();
			}
			workers.remove(thread);
		}

		synchronized (workers) {
			while (! workers.isEmpty()) {
				workers.wait();
			}
		}
		logger.debug("BDB update store shutdown complete.");
	}
	
	/**
	 * Get the status of a transaction for a peer during a particular reconciliation.
	 * 
	 * If a BerkeleyDB transaction is given, that is used and a DeadlockException is
	 * thrown if deadlock occurs; otherwise, a new transaction is created and a
	 * DeadlockException is only thrown if multiple retries fail
	 * 
	 * @param pid		The ID of the peer to check for
	 * @param tpi		The ID of the transaction to check for
	 * @param recno		The reconciliation during which to check
	 * @param t			An optional BerkeleyDB transaction to use when checking
	 * @return			The status of the transaction
	 * @throws DatabaseException
	 */
	private TxnStatus getTxnStatus(Transaction t, AbstractPeerID pid, TxnPeerID tpi) throws DatabaseException {
		ByteBufferWriter bbw = new ByteBufferWriter();
		bbw.addToBuffer(pid);
		bbw.addToBuffer(tpi);

		DatabaseEntry key = new DatabaseEntry(bbw.getByteArray());
		DatabaseEntry value = new DatabaseEntry();

		OperationStatus os = decisions.get(t, key, value, LockMode.DEFAULT);
		if (os == OperationStatus.NOTFOUND) {
			return TxnStatus.undecided();
		} else {
			ByteBufferReader bbr = new ByteBufferReader(null, value.getData());
			boolean accepted = bbr.readBoolean();
			int recno = bbr.readInt();
			if (accepted) {
				return TxnStatus.acceptedAt(recno);
			} else {
				return TxnStatus.rejectedAt(recno);
			}
		}
	}

	private void doPublish(Transaction t, AbstractPeerID owner, PublishMsg pm) throws DatabaseException {
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry value = new DatabaseEntry();

		int epoch = getLastEpoch(t) + 1;

		ByteBufferWriter bbw = new ByteBufferWriter();
		pm.startReading();
		PublishMsg.TxnToPublish ttp;
		while ((ttp = pm.readTxn()) != null) {
			key.setData(ttp.tpi.getBytes());
			value.setData(ttp.contents);
			OperationStatus os = txns.putNoOverwrite(t, key, value);
			if (os == OperationStatus.KEYEXIST) {
				throw new RuntimeException("Transaction " + ttp.tpi + " is already recorded");
			}
			bbw.addToBuffer(ttp.tpi);
		}
		key.setData(IntType.getBytes(epoch));
		value.setData(bbw.getByteArray());
		OperationStatus os = epochContents.putNoOverwrite(t, key, value);
		if (os == OperationStatus.KEYEXIST) {
			throw new RuntimeException("Epoch " + epoch + " is already recorded");
		}
	}

	private void recordReconcile(Transaction t, AbstractPeerID pid, boolean empty) throws DatabaseException {
		DatabaseEntry key = new DatabaseEntry(pid.getBytes());
		DatabaseEntry value = new DatabaseEntry();

		int epoch = -1;
		if (! empty) {
			epoch = getLastEpoch(t);
		}


		OperationStatus os = lastRecno.get(t, key, value, LockMode.RMW);
		int recno;
		if (os == OperationStatus.NOTFOUND) {
			recno = StateStore.FIRST_RECNO;
			if (empty) {
				throw new RuntimeException("First reconciliation for peer " + pid + " cannot be empty");
			}
		} else {
			recno = IntType.getValFromBytes(value.getData());
			++recno;
		}
		value.setData(IntType.getBytes(recno));
		lastRecno.put(t, key, value);

		ByteBufferWriter bbw = new ByteBufferWriter();

		if (empty) {
			bbw.addToBuffer(pid);
			bbw.addToBuffer(recno - 1);
			key.setData(bbw.getByteArray());
			bbw.clear();
			if (recnoEpochs.get(t, key, value, LockMode.RMW) != OperationStatus.SUCCESS) {
				throw new RuntimeException("Couldn't find epoch for previous reconciliation");
			}
			epoch = IntType.getValFromBytes(value.getData());
		}

		bbw.addToBuffer(pid);
		bbw.addToBuffer(recno);
		key.setData(bbw.getByteArray());
		value.setData(IntType.getBytes(epoch));

		recnoEpochs.putNoOverwrite(t, key, value);
	}

	private void recordDecisions(Transaction t, AbstractPeerID pid, RecordDecisions rd) throws DatabaseException {
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry value = new DatabaseEntry();
		key.setData(pid.getBytes());
		if (lastRecno.get(t, key, value, LockMode.DEFAULT) != OperationStatus.SUCCESS) {
			throw new RuntimeException("Could not determine last reconciliation");
		}
		int recno = IntType.getValFromBytes(value.getData());
		rd.startReading();
		ByteBufferWriter bbw = new ByteBufferWriter();

		Decision d;

		while ((d = rd.readDecision()) != null) {
			bbw.clear();
			bbw.addToBuffer(pid);
			bbw.addToBuffer(d.tpi);
			key.setData(bbw.getByteArray());
			bbw.clear();
			bbw.addToBuffer(d.accepted);
			bbw.addToBuffer(d.recno);
			value.setData(bbw.getByteArray());
			OperationStatus os = decisions.putNoOverwrite(t, key, value);
			if (os == OperationStatus.KEYEXIST) {
				throw new RuntimeException("Transaction " + d.tpi + " is already decided for peer " + pid);
			}

			bbw.clear();
			bbw.addToBuffer(pid);
			bbw.addToBuffer(recno);
			key.setData(bbw.getByteArray());

			// Append to the reconAcceptedTxns

			value.setData(d.tpi.getBytes());
			if (d.accepted) {
				reconAcceptedTxns.putNoDupData(t, key, value);
			} else {
				reconRejectedTxns.putNoDupData(t, key, value);
			}
		}
	}

	private int getEpochForRecno(Transaction t, AbstractPeerID pid, int recno) throws DatabaseException {
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry value = new DatabaseEntry();

		ByteBufferWriter bbw = new ByteBufferWriter();
		bbw.addToBuffer(pid);
		bbw.addToBuffer(recno);
		key.setData(bbw.getByteArray());

		OperationStatus os = recnoEpochs.get(t, key, value, LockMode.DEFAULT);

		if (os == OperationStatus.NOTFOUND) {
			throw new RuntimeException("Could not find epoch for peer " + pid + " recno " + recno);
		}

		return IntType.getValFromBytes(value.getData());
	}

	private ReconciliationData getReconciliationData(final Transaction t, TrustConditions tc, int recno, final Set<TxnPeerID> alreadyAcceptedTids)
	throws Exception {
		final AbstractPeerID pid = tc.getOwner();
		int startEpoch;
		if (recno == StateStore.FIRST_RECNO) {
			startEpoch = FIRST_EPOCH;
		} else {
			startEpoch = getEpochForRecno(t, pid, recno - 1) + 1;
		}
		int endEpoch = getEpochForRecno(t, pid, recno);

		ByteBufferReader bbr = new ByteBufferReader(_mapStore);//getSchema(tc.getOwner()));

		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry value = new DatabaseEntry();

		List<TxnPeerID> possibleRelevantTids = new ArrayList<TxnPeerID>();

		for (int i = startEpoch; i <= endEpoch; ++i) {
			key.setData(IntType.getBytes(i));
			OperationStatus os = epochContents.get(t, key, value, LockMode.DEFAULT);
			if (os == OperationStatus.NOTFOUND) {
				throw new RuntimeException("Epoch " + i + " not found");
			}
			bbr.reset(value.getData());
			while (! bbr.hasFinished()) {
				possibleRelevantTids.add(bbr.readTxnPeerID());
			}
		}

		TransactionDecisions td = new TransactionDecisions() {
			public boolean hasAcceptedTxn(TxnPeerID tpi) throws USException {
				try {
					return (alreadyAcceptedTids.contains(tpi) || getTxnStatus(t, pid, tpi).isAccepted());
				} catch (DatabaseException e) {
					throw new USException(e);
				}
			}

			public boolean hasRejectedTxn(TxnPeerID tpi) throws USException {
				try {
					return (getTxnStatus(t, pid, tpi).isRejected());
				} catch (DatabaseException e) {
					throw new USException(e);
				}
			}

		};
		
		final Schema peerSchema = getSchema(tc.getOwner());

		TransactionSource ts = new TransactionSource() {
			public List<Update> getTxn(TxnPeerID tpi) throws USException {
				if (! peerSchema.equals(getSchema(tpi.getPeerID()))) {
					// TODO: need to translate into peer's schema
					throw new USException("Need to call update translation");
				}
				return BerkeleyDBStoreServer.this.getTxn(tpi);
			}
			
		};

		HashMap<TxnPeerID,Integer> priorities = new HashMap<TxnPeerID,Integer>();

		
		for (TxnPeerID tpi : possibleRelevantTids) {
			if (td.hasAcceptedTxn(tpi) || td.hasRejectedTxn(tpi)) {
				continue;
			}
			if (! peerSchema.equals(getSchema(tpi.getPeerID()))) {
				// TODO: need to translate into peer's schema
				continue;
			}
			int prio = tc.getTxnPriority(getTxn(tpi));
			if (prio > 0) {
				priorities.put(tpi, prio);
			}
		}

		HashMap<TxnPeerID,TxnChain> chains = new HashMap<TxnPeerID,TxnChain>();

		ReconciliationData rd = new ReconciliationData();

		for (Map.Entry<TxnPeerID,Integer> entry : priorities.entrySet()) {
			TxnPeerID tpi = entry.getKey();
			int prio = entry.getValue();
			try {
				TxnChain chain = new TxnChain(tpi, ts, td);
				chains.put(tpi, chain);
				rd.writeEntry(tpi, prio, chain);
			} catch (AlreadyRejectedAntecedent e) {
				rd.writeEntry(tpi, prio, null);
			}
		}
		rd.finish();
		return rd;
	}

	public static void main(String[] args) throws Exception {
		logger.debug("Starting server.");
		boolean reset = false;
		int port = DEFAULT_PORT;
		for (int i = 0; i < args.length - 1; ++i) {
			if (args[i].equalsIgnoreCase("-reset")) {
				reset = true;
			} else if (args[i].equalsIgnoreCase("-port")) {
				port = Integer.parseInt(args[++i]);
			}
		}
		if (args.length < 1) {
			System.err.println("Syntax: BerkeleyDBStoreServer [-reset] [-port {no}] {data directory}");
			System.exit(1);
		}
		String envDir = args[args.length-1];

		File f = new File(envDir);
		if (f.exists()) {
			if (reset) {
				File[] files = f.listFiles();
				for (File file : files) {
					file.delete();
				}
			}
		} else {
			f.mkdir();
		}
		
		//File configFile = new File(f,"bdbstoreconfig");

		EnvironmentConfig ec = new EnvironmentConfig();
		ec.setAllowCreate(true);
		ec.setTransactional(true);
		Environment e = new Environment(f, ec);

		/*System.out.println("Orchestra BerkeleyDB Store Server, Copyright (C) 2009 Trustees of the University of Pennsylvania");
		System.out.println("Running on port " + port);*/

		BerkeleyDBStoreServer server = new BerkeleyDBStoreServer(e, /*configFile,*/ port);
		//Runtime.getRuntime().addShutdownHook(server.new ShutdownThread());
		/*logger.debug("Entering command loop.");
		int command = 0;
		do
		{
			System.out.print("\nEnter a command (? for help): ");
			System.out.flush();
			char c = (char) command;
			logger.debug("Current command: {}", command);
			if (c == 'q') {
				break;
			} else {
				System.out.print("Commands:\n? - Print this help message" +
				"\nq - Terminate the store");
			}
		} while ((command = System.in.read()) != -1);
		logger.debug("Exiting command loop.");
		server.quit();*/
		logger.debug("Exiting main.");
	}

	private class WorkerThread extends Thread {
		private boolean shouldExit = false;
		Socket socket;
		ObjectInputStream ois;
		ObjectOutputStream oos;

		WorkerThread(Socket s) throws IOException {
			super(tg,"WorkerThead: " + s.getRemoteSocketAddress());
			socket = s;
			socket.setSoTimeout(500);
			ois = new ObjectInputStream(socket.getInputStream());
			oos = new ObjectOutputStream(socket.getOutputStream());
			start();
			logger.debug("New WorkerThread created and started for socket {}.", socket);
		}

		public void run() {
			logger.debug("New WorkerThread running for socket {}.", socket);
			TrustConditions tc = null;
			try {
				for ( ; ; ) {
					try {
						synchronized (this) {
							if (shouldExit) {
								if (oos != null) {
									oos.writeObject(new EndOfStreamMsg());
									ois.close();
									oos.close();
									socket.close();
								}
								break;
							}
						}
						Object o = ois.readObject();
						logger.debug("Handling {}", o);
						Object response;
						if (o instanceof EndOfStreamMsg) {
							requestExit();
						} else if (o instanceof Reset) {
							logger.debug("Handling Reset");
							final Reset r = (Reset) o;
							final ObjectOutputStream out = oos;
							final Socket s = socket;
							new Thread() {
								public void run() {
									reset(r.dump, out, s);
								}
							}.start();
							response = null;
							oos = null;
							socket = null;
						} else {
							try {
								if (o instanceof Ping) {
									response = new Ack();
								} else if (o instanceof SendTrustConditions) {
									SendTrustConditions stc = (SendTrustConditions) o;
									tc = stc.getTrustConditions(_mapStore);
									AbstractPeerID pid = tc.getOwner();
									synchronized (schemas) {
										Schema oldSchema = schemas.get(pid);
										if (oldSchema == null) {
											schemas.put(pid, stc.s);
											response = new Ack();
										} else if (! oldSchema.getSchemaId().equals(stc.s.getSchemaId())) {
										//} else if (! oldSchema.equals(stc.s)) {
											response = new Exception("A different schema is already given for peer " + pid);
										} else {
											response = new Ack();
										}
									}
								} else if (o instanceof GetSchema) {
									response = getSchema(((GetSchema) o).getPid());
								} else if (o instanceof GetSchemaByName) {
									GetSchemaByName request = (GetSchemaByName) o;
									response = getSchemaByName(request.getCdss(), request.getPid(), request.getSchemaName());
								} else if (o instanceof GetAllSchemas) {
									GetAllSchemas request = (GetAllSchemas) o;
									Map<AbstractPeerID, Schema> map = _mapStore.getSchemasForNamespace(request.getCdssName());
									response = new GetAllSchemasResponse(map);
								} else if (o instanceof LoadSchemas) {
									LoadSchemas request = (LoadSchemas) o;
									Map<AbstractPeerID, Schema> map = _mapStore.loadSchemas(request.getPeerDocument());
									response = new LoadSchemasResponse(map);
								} else if (o instanceof GetHostedSystems) {
									response = new GetHostedSystemsResponse(_mapStore.getSystems());
								} else if (o instanceof StopUpdateStore) {
									//requestExit();
									//workers.remove(this);
									//new ShutdownThread().start();
									//oos = null;
									//socket = null;
									quit(this);
									//requestExit();
									response = null;
									//oos = null;
									//ois.close();
									//oos.close();
									//socket.close();
								} else if (tc == null && (! (o instanceof DumpMsg))) {
									throw new RuntimeException("Trust conditions (and peer ID) have not been sent to server");
								} else {
									int retryCount = 0;
									Transaction t = null;
									for ( ; ; ) {
										try {
											t = env.beginTransaction(null, null);
											if (o instanceof GetStatusMsg) {
												GetStatusMsg gsm = (GetStatusMsg) o;
												response = getTxnStatus(t, tc.getOwner(), gsm.tpi);
											} else if (o instanceof PublishMsg) {
												doPublish(t, tc.getOwner(), (PublishMsg) o);
												response = new Ack();
											} else if (o instanceof RecordReconcileMsg) {
												RecordReconcileMsg rrm = (RecordReconcileMsg) o;
												recordReconcile(t, tc.getOwner(), rrm.empty);
												response = new Ack();
											} else if (o instanceof RecordDecisions) {
												recordDecisions(t, tc.getOwner(), (RecordDecisions) o);
												response = new Ack();
											} else if (o instanceof GetReconciliationData) {
												GetReconciliationData grd = ((GetReconciliationData) o);
												response = getReconciliationData(t, tc, grd.recno, grd.ownAcceptedTxns);
											} else if (o instanceof GetLastReconciliation) {
												response = getLastReconciliation(t, tc.getOwner());
											} else if (o instanceof GetLastEpoch) {
												response = getLastEpoch(t);
											} else if (o instanceof GetRecnoEpochs) {
												response = getRecnoEpochs(t, tc.getOwner());
											} else if (o instanceof TxnPeerID) {
												List<Update> txn = getTxn((TxnPeerID) o);
												if (txn == null) {
													response = new Exception("Could not find transaction " + o);
												} else {

													ByteBufferWriter bbw = new ByteBufferWriter();
													for (Update u : txn) {
														bbw.addToBuffer(u, SerializationLevel.VALUES_AND_TIDS);
													}
													response = bbw.getByteArray();
												}
											} else if (o instanceof GetTxns) {
												List<TxnPeerID> tids = ((GetTxns) o).getTpis();
												ByteBufferWriter bbw = new ByteBufferWriter();
												response = null;
												for (TxnPeerID tid : tids) {
													bbw.addToBuffer(tid);
													List<Update> txn = getTxn(tid);
													bbw.addToBuffer(txn.size());
													if (txn == null) {
														response = new Exception("Could not find transaction " + tid);
														break;
													} else {
														for (Update u : txn) {
															bbw.addToBuffer(u, Update.SerializationLevel.VALUES_AND_TIDS);
														}
													}
												}
												if (response == null) {
													response = bbw.getByteArray();
												}
											} else if (o instanceof GetDecisions) {
												response = getDecisions(t, tc.getOwner(), ((GetDecisions) o).recno);
											} else if (o instanceof GetEpochContents) {
												response = getEpochContents(t, ((GetEpochContents) o).epoch);
											} else if (o instanceof GetRecnoEpoch) {
												response = getEpochForRecno(t, tc.getOwner(), ((GetRecnoEpoch) o).recno);
											} else if (o instanceof GetEpochTransactions) {
												response = getEpochTransactions(t, ((GetEpochTransactions) o).epoch);
											} else if (o instanceof DumpMsg) {
												response = dump(t);
											} else if (o instanceof GetLargestTidForPeer) {
												response = getLargestTid(t, tc.getOwner());
											} else {
												response = new BadMsg(o);
											}
											t.commit();
											t = null;
											break;
										} catch (DeadlockException de) {
											if (t != null)
												t.abort();
											t = null;
											++retryCount;
											if (retryCount > MAX_DEADLOCK_RETRIES) {
												throw de;
											}
										} finally {
											// Abort transaction if method threw an exception
											if (t != null) {
												try {
													t.abort();
												} catch (DatabaseException de) {
													exceptions.add(de);
												}
											}
										}
									}
								}
							} catch (Exception e) {
								response = e;
								logger.error("Returning error response.", e);
								e.printStackTrace();
								exceptions.add(e);
							}
							if (response != null) {
								logger.debug("Returning response {}", response);
								oos.writeObject(response);
							}
						}
					} catch (SocketTimeoutException ste) {
					}
				}
				logger.debug("WorkerThread finishing for socket: {}", socket);
			} catch (Exception e) {
				logger.error("BerkleyDB Store Server caught exception while handling socket " + socket + ".", e);
				exceptions.add(e);
			} finally {
				synchronized (workers) {
					workers.remove(this);
					workers.notify();
				}
			}
		}

		synchronized void requestExit() {
			shouldExit = true;
		}
	}

	private class ListenerThread extends Thread {
		ServerSocket ss;
		ListenerThread() throws IOException {
			super(tg, "ListenerThread");
			ss = new ServerSocket(port);
			ss.setSoTimeout(500);
			start();
			logger.debug("New ListenerThread created and started.");
		}
		
		public void run() {
			try {
				while (! isInterrupted()) {
					try {
						Socket s = ss.accept();
						logger.debug("ListenerThread accepted new connection");
						WorkerThread wt = new WorkerThread(s);
						synchronized (workers) {
							workers.add(wt);
						}
					} catch (SocketTimeoutException ste) {
					}		
				}
				ss.close();
				logger.debug("Interrupted. Closing ServerSocket");
			} catch (Exception e) {
				exceptions.add(e);
			}

		}
	}

	private class ShutdownThread extends Thread {
		@Override
		public void run() {
			logger.debug("Starting Shutdown thread");
			try {
				quit();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				exceptions.add(e);
			} catch (Exception e) { 
				exceptions.add(e);
			}
		}
	}

	private static final Logger logger = LoggerFactory.getLogger(BerkeleyDBStoreServer.class);
	
	private List<Exception> exceptions = Collections.synchronizedList(new LinkedList<Exception>());

	private int getLastReconciliation(Transaction t, AbstractPeerID pid) throws DatabaseException {
		DatabaseEntry key = new DatabaseEntry(pid.getBytes()), value = new DatabaseEntry();
		OperationStatus os = lastRecno.get(t, key, value, null);
		if (os == OperationStatus.SUCCESS) {
			return IntType.getValFromBytes(value.getData());
		} else {
			return StateStore.FIRST_RECNO - 1;
		}
	}

	private List<Decision> getDecisions(Transaction t, AbstractPeerID peer, int recno) throws DatabaseException {
		List<Decision> retval = new ArrayList<Decision>();

		ByteBufferWriter bbw = new ByteBufferWriter();
		bbw.addToBuffer(peer);
		bbw.addToBuffer(recno);
		byte[] pidAndRecno = bbw.getByteArray();

		DatabaseEntry key = new DatabaseEntry(pidAndRecno), value = new DatabaseEntry();

		Cursor c = null;
		try {
			c = reconAcceptedTxns.openCursor(t, CursorConfig.READ_COMMITTED);

			OperationStatus os = c.getSearchKey(key, value, null);
			while (os != OperationStatus.NOTFOUND) {
				retval.add(new Decision(TxnPeerID.fromBytes(value.getData()), recno, true));
				os = c.getNextDup(key, value, null);
			}

			c.close();
			c = null;

			c = reconRejectedTxns.openCursor(t, CursorConfig.READ_COMMITTED);
			while (os != OperationStatus.NOTFOUND) {
				retval.add(new Decision(TxnPeerID.fromBytes(value.getData()), recno, false));
				os = c.getNextDup(key, value, null);
			}
			c.close();
			c = null;
		} finally {
			if (c != null) {
				c.close();
			}
		}
		return retval;
	}

	private List<Integer> getRecnoEpochs(Transaction t, AbstractPeerID pid) throws DatabaseException {
		List<Integer> retval = new ArrayList<Integer>();

		Cursor c = recnoEpochs.openCursor(t, null);
		try {
			ByteBufferWriter bbw = new ByteBufferWriter();
			bbw.addToBuffer(pid);
			byte[] pidBytes = bbw.getByteArray();
			DatabaseEntry key = new DatabaseEntry(pidBytes), value = new DatabaseEntry();
			OperationStatus os = c.getSearchKeyRange(key, value, null);

			while (os != OperationStatus.SUCCESS) {
				ByteBufferReader bbr = new ByteBufferReader(null, value.getData());
				AbstractPeerID found = bbr.readPeerID();
				int recno = bbr.readInt();
				if (! found.equals(pid)) {
					break;
				}
				if (recno != retval.size()) {
					throw new RuntimeException("Expected recno " + retval.size() + " but found " + recno + " for " + pid);
				}
				retval.add(IntType.getValFromBytes(key.getData()));
			}
		} finally {
			c.close();
		}
		return retval;
	}

	private int getLastEpoch(Transaction t) throws DatabaseException {
		DatabaseEntry key = new DatabaseEntry(), value = new DatabaseEntry();
		OperationStatus os;
		Cursor c = epochContents.openCursor(t, null);
		try {
			value.setPartial(0,0,true);
			os = c.getLast(key, value, null);
		} finally {
			if (c != null) {
				c.close();
			}
		}
		if (os == OperationStatus.SUCCESS) {
			return IntType.getValFromBytes(key.getData());
		} else {
			return FIRST_EPOCH - 1;
		}
	}

	private List<TxnPeerID> getEpochTransactions(Transaction t, int epoch) throws DatabaseException {
		List<TxnPeerID> retval = new ArrayList<TxnPeerID>();

		DatabaseEntry key = new DatabaseEntry(IntType.getBytes(epoch)), value = new DatabaseEntry();

		OperationStatus os = epochContents.get(t, key, value, null);
		if (os != OperationStatus.SUCCESS) {
			throw new RuntimeException("Could not find epoch " + epoch);
		}

		ByteBufferReader bbr = new ByteBufferReader(value.getData());

		while (! bbr.hasFinished()) {
			retval.add(bbr.readTxnPeerID());
		}

		return retval;
	}

	private byte[] getEpochContents(Transaction t, int epoch) throws DatabaseException {
		ByteBufferWriter retval = new ByteBufferWriter();

		DatabaseEntry key = new DatabaseEntry(IntType.getBytes(epoch)), value = new DatabaseEntry();

		OperationStatus os = epochContents.get(t, key, value, null);
		if (os != OperationStatus.SUCCESS) {
			throw new RuntimeException("Could not find epoch " + epoch);
		}

		ByteBufferReader bbr = new ByteBufferReader(value.getData());
		value = new DatabaseEntry();
		while (! bbr.hasFinished()) {
			TxnPeerID tpi = bbr.readTxnPeerID();

			List<Update> txn;
			try {
				txn = getTxn(tpi);
			} catch (USException e) {
				if (e.getCause() instanceof DatabaseException) {
					throw (DatabaseException) e.getCause();
				} else {
					throw new RuntimeException(e);
				}
			}
			if (txn == null) {
				throw new RuntimeException("Could not find transaction " + tpi + " from epoch " + epoch);
			}
			retval.addToBuffer(tpi);
			retval.addToBuffer(txn.size());
			for (Update u : txn) {
				retval.addToBuffer(u, SerializationLevel.VALUES_AND_TIDS);
			}
		}

		return retval.getByteArray();
	}

	public List<Update> getTxn(TxnPeerID tpi) throws USException {
		ByteBufferReader bbr = new ByteBufferReader(_mapStore);//getSchema(tpi.getPeerID()));
		DatabaseEntry key = new DatabaseEntry(tpi.getBytes());
		DatabaseEntry value = new DatabaseEntry();
		OperationStatus os;
		try {
			os = txns.get(null, key, value, LockMode.DEFAULT);
		} catch (DatabaseException e) {
			throw new USException(e);
		}
		if (os == OperationStatus.NOTFOUND) {
			return null;
		}
		bbr.reset(value.getData());
		ArrayList<Update> txn = new ArrayList<Update>();
		while (! bbr.hasFinished()) {
			txn.add(bbr.readUpdate());
		}
		return txn;
	}

	private void reset(USDump dump, ObjectOutputStream oos, Socket socket) {
		logger.debug("Reseting Update Store");
		Object response = null;
		try {
			listener.interrupt();
			listener.join();
			listener = null;

			synchronized (workers) {
				for (WorkerThread wt : workers) {
					wt.requestExit();
				}
				while (workers.size() > 0) {
					workers.wait();
				}
			}
			logger.debug("Closing and truncating update store databases.");
			lastRecno.close();
			recnoEpochs.close();
			epochContents.close();
			decisions.close();
			reconAcceptedTxns.close();
			reconRejectedTxns.close();
			txns.close();
			env.truncateDatabase(null, lastRecnoName, false);
			env.truncateDatabase(null, recnoEpochsName, false);
			env.truncateDatabase(null, epochContentsName, false);
			env.truncateDatabase(null, decisionsName, false);
			env.truncateDatabase(null, reconAcceptedTxnsName, false);
			env.truncateDatabase(null, reconRejectedTxnsName, false);
			env.truncateDatabase(null, txnsName, false);
			_mapStore.clear(env);
			lastRecno = env.openDatabase(null, lastRecnoName, dc);
			recnoEpochs = env.openDatabase(null, recnoEpochsName, dc);
			epochContents = env.openDatabase(null, epochContentsName, dc);
			decisions = env.openDatabase(null, decisionsName, dc);
			reconAcceptedTxns = env.openDatabase(null, reconAcceptedTxnsName, reconTxnsDc);
			reconRejectedTxns = env.openDatabase(null, reconRejectedTxnsName, reconTxnsDc);
			txns = env.openDatabase(null, txnsName, dc);
			schemas.clear();

			Map<Integer,List<TxnPeerID>> epochContents = new HashMap<Integer,List<TxnPeerID>>();

			if (dump != null) {
				schemas.putAll(dump.getSchemas());
				Iterator<TxnPeerID> tids = dump.getTids();

				ByteBufferWriter bbw = new ByteBufferWriter();
				DatabaseEntry key = new DatabaseEntry(), value = new DatabaseEntry();

				while (tids.hasNext()) {
					bbw.clear();
					TxnPeerID tid = tids.next();

					List<Update> txn = dump.getTxnContents(tid);

					for (Update u : txn) {
						bbw.addToBuffer(u, Update.SerializationLevel.VALUES_AND_TIDS);
					}
					key.setData(tid.getBytes());
					value.setData(bbw.getByteArray());
					txns.put(null, key, value);
					int epoch = dump.getTxnEpoch(tid);
					List<TxnPeerID> txnsForEpoch = epochContents.get(epoch);
					if (txnsForEpoch == null) {
						txnsForEpoch = new ArrayList<TxnPeerID>();
						epochContents.put(epoch, txnsForEpoch);
					}
					txnsForEpoch.add(tid);
				}

				for (Map.Entry<Integer, List<TxnPeerID>> me : epochContents.entrySet()) {
					int epoch = me.getKey();
					List<TxnPeerID> contents = me.getValue();

					bbw.clear();
					for (TxnPeerID tpi : contents) {
						bbw.addToBuffer(tpi);
					}
					key.setData(IntType.getBytes(epoch));
					value.setData(bbw.getByteArray());
					this.epochContents.put(null, key, value);
				}

				for (AbstractPeerID pid : dump.getPeers()) {
					int lastRecno = Integer.MIN_VALUE;

					Iterator<Decision> decs = dump.getPeerDecisions(pid);

					while (decs.hasNext()) {
						Decision dec = decs.next();
						bbw.clear();
						bbw.addToBuffer(pid);
						bbw.addToBuffer(dec.tpi);
						key.setData(bbw.getByteArray());
						bbw.clear();
						bbw.addToBuffer(dec.recno);
						bbw.addToBuffer(dec.recno);
						value.setData(bbw.getByteArray());
						decisions.put(null, key, value);

						bbw.clear();
						bbw.addToBuffer(pid);
						bbw.addToBuffer(dec.recno);
						key.setData(bbw.getByteArray());
						value.setData(dec.tpi.getBytes());
						if (dec.accepted) {
							reconAcceptedTxns.put(null, key, value);
						} else {
							reconRejectedTxns.put(null, key, value);
						}
					}

					Iterator<RecnoEpoch> recons = dump.getPeerRecons(pid);
					while (recons.hasNext()) {
						RecnoEpoch recon = recons.next();

						if (recon.recno > lastRecno) {
							lastRecno = recon.recno;
						}

						bbw.clear();
						bbw.addToBuffer(pid);
						bbw.addToBuffer(recon.recno);
						key.setData(bbw.getByteArray());
						value.setData(IntType.getBytes(recon.epoch));

						recnoEpochs.put(null, key, value);
					}

					key.setData(pid.getBytes());
					value.setData(IntType.getBytes(lastRecno));
					this.lastRecno.put(null, key, value);
				}
			}
			logger.debug("Restarting Listener");
			listener = new ListenerThread();
			response = new Ack();
		} catch (Exception e) {
			response = e;
		} finally {
			try {
				logger.debug("Returning response {}", response);
				oos.writeObject(response);
				oos.close();
				socket.close();
			} catch (IOException e) {
				exceptions.add(e);
			}
		}

	}

	private USDump dump(Transaction t) throws DatabaseException, USException {
		USDump dump;
		synchronized (schemas) {
			dump = new USDump(_mapStore, schemas);
		}


		Cursor c = null;
		try {
			c = txns.openCursor(t, null);
			DatabaseEntry key = new DatabaseEntry(), value = new DatabaseEntry();
			OperationStatus os = c.getFirst(key, value, null);

			ByteBufferReader bbr = new ByteBufferReader(_mapStore);
			while (os != OperationStatus.NOTFOUND) {
				TxnPeerID tpi = TxnPeerID.fromBytes(key.getData());
//				bbr.setSchema(getSchema(tpi.getPeerID()));
				List<Update> txn = new ArrayList<Update>();
				bbr.reset(value.getData());
				while (! bbr.hasFinished()) {
					txn.add(bbr.readUpdate());
				}
				dump.addTxn(tpi, txn);
				os = c.getNext(key, value, null);
			}
			c.close();
			c = null;
			c = epochContents.openCursor(t, null);
			os = c.getFirst(key, value, null);
			while (os != OperationStatus.NOTFOUND) {
				int epoch = IntType.getValFromBytes(key.getData());
				bbr.reset(value.getData());
				while (! bbr.hasFinished()) {
					TxnPeerID tpi = bbr.readTxnPeerID();
					dump.addTxnEpoch(tpi, epoch);
				}
				os = c.getNext(key, value, null);
			}
			c.close();
			c = null;
			c = recnoEpochs.openCursor(t, null);
			os = c.getFirst(key, value, null);
			while (os != OperationStatus.NOTFOUND) {
				bbr.reset(key.getData());
				AbstractPeerID pid = bbr.readPeerID();
				int recno = bbr.readInt();
				int epoch = IntType.getValFromBytes(value.getData());
				dump.addRecnoEpoch(pid, recno, epoch);
				os = c.getNext(key, value, null);
			}
			c.close();
			c = null;
			c = decisions.openCursor(t, null);
			os = c.getFirst(key, value, null);
			while (os != OperationStatus.NOTFOUND) {
				bbr.reset(key.getData());
				AbstractPeerID pid = bbr.readPeerID();
				TxnPeerID tpi = bbr.readTxnPeerID();
				bbr.reset(value.getData());
				boolean accepted = bbr.readBoolean();
				int recno = bbr.readInt();
				dump.addDecision(pid, tpi, recno, accepted);

				os = c.getNext(key, value, null);
			}
		} finally {
			if (c != null) {
				c.close();
			}
		}

		return dump;
	}

	private int getLargestTid(Transaction t, AbstractPeerID pid) throws DatabaseException {
		// TODO: this is very inefficient

		DatabaseEntry key = new DatabaseEntry(), value = new DatabaseEntry();
		Cursor c = null;
		int largestTid = -1;
		try {
			c = txns.openCursor(null, null);
			value.setPartial(0, 0, true);
			OperationStatus os = c.getFirst(key, value, null);
			while (os != OperationStatus.NOTFOUND) {
				TxnPeerID tpi = TxnPeerID.fromBytes(key.getData());
				if (tpi.getTid() > largestTid && tpi.getPeerID().equals(pid)) {
					largestTid = tpi.getTid();
				}
				os = c.getNext(key, value, null);
			}
			return largestTid;
		} finally {
			if (c != null) {
				c.close();
			}
		}
	}

	private Schema getSchema(AbstractPeerID pid) throws USException {
		return _mapStore.getSchema(pid);
		/*
		synchronized (schemas) {
			Schema s = schemas.get(pid);
			if (s == null) {
				throw new USException("Cannot find schema for peer " + pid + " in list " + schemas.keySet().toString());
			} else {
				return s;
			}
		}*/
	}
	
	private GetSchemaByNameResponse getSchemaByName(String cdssName, AbstractPeerID pid, String schemaName) {
		Schema s = _mapStore.getSchema(cdssName, pid);
		return new GetSchemaByNameResponse(s);
	}

	public Map<Schema,Map<Relation,Integer>> registerAllSchemas(String namespace, List<Schema> schemas, 
			Map<AbstractPeerID,Integer> peerSchema) {
		return _mapStore.registerAllSchemas(namespace, schemas, peerSchema);
	}
	
	public SchemaIDBinding getBinding() {
		return _mapStore;
	}
	
}
