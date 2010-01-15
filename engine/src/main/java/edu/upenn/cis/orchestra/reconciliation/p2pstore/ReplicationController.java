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
package edu.upenn.cis.orchestra.reconciliation.p2pstore;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import rice.Continuation;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdRange;
import rice.p2p.commonapi.Node;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.NodeHandleSet;
import rice.p2p.replication.manager.ReplicationManager;
import rice.p2p.replication.manager.ReplicationManagerClient;
import rice.p2p.replication.manager.ReplicationManagerImpl;
import rice.pastry.IdSet;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DeadlockException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

import edu.upenn.cis.orchestra.datamodel.ByteBufferReader;
import edu.upenn.cis.orchestra.datamodel.ByteBufferWriter;
import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.reconciliation.PidAndRecno;
import edu.upenn.cis.orchestra.reconciliation.p2pstore.P2PStore.MessageProcessorThread;



class ReplicationController implements ReplicationManagerClient {
	public static final int MAX_DEADLOCK_RETRIES = 5;
	public static final String instanceName = "ReplicationController";
	private final int replicationFactor;
	private Database idsDb;
	private boolean dbClosing = false;
	private int numCurrentScans = 0;
	private P2PStore store;
	@SuppressWarnings("unused")
	private ReplicationManager rm;

	private BlockingQueue<WorkUnit> workUnits;
	private WorkUnitThread workUnitThread;

	ReplicationController(ThreadGroup tg, int replicationFactor, Environment env, String idsDbName,
			Node n, P2PStore store, IdFactory idf) throws DatabaseException {
		DatabaseConfig dc = new DatabaseConfig();
		dc.setAllowCreate(true);
		dc.setTransactional(true);
		dc.setBtreeComparator(new SerializedIdComparator(idf));
		dc.setOverrideBtreeComparator(true);
		idsDb = env.openDatabase(null, idsDbName, dc);
		this.replicationFactor = replicationFactor;
		this.store = store;

		rm = new ReplicationManagerImpl(n, this, this.replicationFactor, instanceName);

		workUnits = new LinkedBlockingQueue<WorkUnit>();
		workUnitThread = new WorkUnitThread(tg);
		workUnitThread.start();
	}

	void halt() throws DatabaseException, InterruptedException {
		workUnitThread.interrupt();
		workUnitThread.join();
		synchronized (this) {
			dbClosing = true;
			while (numCurrentScans > 0) {
				wait();
			}
			idsDb.close();
			idsDb = null;
		}
	}

	private void addWorkUnit(WorkUnit wu) {
		try {
			workUnits.put(wu);
		} catch (InterruptedException ie) {
			System.err.println("Losing WorkUnit " + wu + " due to InterruptedException");
		}
	}

	/**
	 * A class to represent the data items associated with a particular
	 * Pastry ID.
	 * 
	 * @author netaylor
	 *
	 */
	static class Entries {
		Set<TxnPeerID> tids = new HashSet<TxnPeerID>();
		Set<AbstractPeerID> pids = new HashSet<AbstractPeerID>();
		Set<Integer> epochs = new HashSet<Integer>();
		Set<PidAndRecno> recons = new HashSet<PidAndRecno>();

		private Entries() {
		}

		private Entries(byte[] data) {
			addDataFromBytes(data);
		}

		private byte[] getBytes() {
			ByteBufferWriter bbw = new ByteBufferWriter();
			bbw.addToBuffer(tids.size());
			for (TxnPeerID tid : tids) {
				bbw.addToBuffer(tid);
			}
			bbw.addToBuffer(pids.size());
			for (AbstractPeerID pid : pids) {
				bbw.addToBuffer(pid);
			}
			bbw.addToBuffer(epochs.size());
			for (int epoch : epochs) {
				bbw.addToBuffer(epoch);
			}
			bbw.addToBuffer(recons.size());
			for (PidAndRecno recon : recons) {
				bbw.addToBuffer(recon);
			}
			return bbw.getByteArray();
		}

		private void addDataFromBytes(byte[] data) {
			ByteBufferReader bbr = new ByteBufferReader(null,data);
			for (int numTids = bbr.readInt(); numTids > 0; --numTids) {
				tids.add(bbr.readTxnPeerID());
			}
			for (int numPids = bbr.readInt(); numPids > 0; --numPids) {
				pids.add(bbr.readPeerID());
			}
			for (int numEpochs = bbr.readInt(); numEpochs > 0; --numEpochs) {
				epochs.add(bbr.readInt());
			}
			for (int numRecons = bbr.readInt(); numRecons > 0; --numRecons) {
				recons.add(bbr.readPidAndRecno());
			}
			if (! bbr.hasFinished()) {
				throw new RuntimeException("Data remaining after decoding EntriesForId");
			}
		}

		private boolean isEmpty() {
			return (tids.size() + pids.size() + epochs.size()) == 0;
		}

		public String toString() {
			StringBuilder sb = new StringBuilder();
			if (! tids.isEmpty()) {
				sb.append("Txns: ");
				sb.append(tids);
			}
			if (! pids.isEmpty()) {
				if (sb.length() > 0) {
					sb.append(" ");
				}
				sb.append("Trust conditions: ");
				sb.append(pids);
			}
			if (! epochs.isEmpty()) {
				if (sb.length() > 0) {
					sb.append(" ");
				}
				sb.append("Epochs: ");
				sb.append(epochs);
			}
			if (! recons.isEmpty()) {
				if (sb.length() > 0) {
					sb.append(" ");
				}
				sb.append("Reconciliations: ");
				sb.append(recons);
			}
			return sb.toString();
		}
	}

	/**
	 * Add a PeerID to the database of data items stored at this node
	 * 
	 * @param t			The BerkleyDB transaction to use
	 * @param pid		The PeerID that is now stored at this node
	 * @throws DatabaseException
	 */
	void add(Transaction t, AbstractPeerID pid) throws DatabaseException {
		if (pid == null) {
			throw new IllegalArgumentException();
		}
		addDatum(t, store.getId(pid), pid);
	}

	/**
	 * Add a TxnPeerID to the database of data items stored at this node
	 * 
	 * @param t			The BerkleyDB transaction to use
	 * @param tpi		The TxnPeerID that is now stored at this node
	 * @throws DatabaseException
	 */
	void add(Transaction t, TxnPeerID tpi) throws DatabaseException {
		if (tpi == null) {
			throw new IllegalArgumentException();
		}
		addDatum(t, store.getId(tpi), tpi);
	}

	/**
	 * Add a epoch to the database of data items stored at this node
	 * 
	 * @param t			The BerkleyDB transaction to use
	 * @param epoch		The epoch that is now stored at this node
	 * @throws DatabaseException
	 */
	void add(Transaction t, int epoch) throws DatabaseException {
		addDatum(t, store.getId(epoch), epoch);
	}

	/**
	 * Add a reconciliation to the database of data items stored
	 * at this node
	 * 
	 * @param t			The BerkeleyDB transaction to use
	 * @param par		The PeerID and number of the reconciliation
	 * 					now stored at this node
	 * @throws DatabaseException
	 */
	void add(Transaction t, PidAndRecno par) throws DatabaseException {
		addDatum(t, store.getId(par), par);
	}

	/**
	 * Helper method used to add an object to this list of
	 * data associated with a particular pastry ID
	 * 
	 * @param t			The BerkeleyDB transaction to be used
	 * @param id		The P2P Id of the object
	 * @param o			The actual object (must be a PeerID, a TxnPeerID,
	 * 					a PidAndRecno, or an integer representing an epoch)
	 * @throws DatabaseException
	 */
	private void addDatum(Transaction t, Id id, Object o) throws DatabaseException {
		// o is Integer, PeerID, or TxnPeerID and not null
		DatabaseEntry key = new DatabaseEntry(id.toByteArray());
		DatabaseEntry value = new DatabaseEntry();

		OperationStatus os = idsDb.get(t, key, value, LockMode.RMW);

		Entries efi;
		if (os == OperationStatus.NOTFOUND) {
			efi = new Entries();
		} else {
			efi = new Entries(value.getData());
		}
		if (o instanceof Integer) {
			efi.epochs.add((Integer) o);
		} else if (o instanceof AbstractPeerID) {
			efi.pids.add((AbstractPeerID) o);
		} else if (o instanceof TxnPeerID) {
			efi.tids.add((TxnPeerID) o);
		} else if (o instanceof PidAndRecno) {
			efi.recons.add((PidAndRecno) o);
		} else {
			throw new RuntimeException("o should be an Integer, PeerID, PidAndRecno, or TxnPeerID");
		}
		value.setData(efi.getBytes());
		idsDb.put(t, key, value);
	}

	/**
	 * Determine if there is data for a particular Pastry ID stored at this peer
	 * 
	 * @param t			The BerkeleyDB transaction to use, or <code>null</code>
	 * 					to not use BerkeleyDB transactional protections
	 * @param id		The ID to search for
	 * @return			<code>true</code> if there is such data at this peer,
	 * 					<code>false</code> if not
	 * @throws DatabaseException
	 */
	boolean hasEntriesForId(Transaction t, Id id) throws DatabaseException {
		DatabaseEntry key = new DatabaseEntry(id.toByteArray());
		DatabaseEntry value = new DatabaseEntry();
		value.setPartial(0, 0, true);
		OperationStatus os = idsDb.get(t, key, value, LockMode.DEFAULT);
		return (os == OperationStatus.SUCCESS);

	}

	/**
	 * Get the data stored at this peer for a particular pastry ID
	 * 
	 * @param t		The BerkeleyDB transaction to use
	 * @param id	The ID to search for
	 * @return		The data assoicated with that ID
	 * @throws DatabaseException
	 */
	Entries getEntriesForId(Transaction t, Id id) throws DatabaseException {
		DatabaseEntry key = new DatabaseEntry(id.toByteArray());
		DatabaseEntry value = new DatabaseEntry();
		OperationStatus os = idsDb.get(t, key, value, LockMode.DEFAULT);

		Entries efi;
		if (os == OperationStatus.SUCCESS) {
			efi = new Entries(value.getData());
		} else {
			efi = new Entries();
		}
		return efi;
	}

	/**
	 * Get the data stored at this peer for a particular range of Pastry IDs
	 * 
	 * @param t		The BerkeleyDB transaction to use
	 * @param ids	The range of IDs
	 * @return		The data for IDs in that range
	 * @throws DatabaseException
	 */
	@SuppressWarnings("unchecked")
	Entries getEntriesForIds(Transaction t, IdRange ids) throws DatabaseException {
		Entries entriesForIds = new Entries();
		Cursor c = null;
		try {
			c = idsDb.openCursor(t, null);
			DatabaseEntry key = new DatabaseEntry(), value = new DatabaseEntry();

			Id ccw = ids.getCCWId(), cw = ids.getCWId();
			if (ccw.compareTo(cw) < 0) {
				// Range does not wrap around zero, so we only have to do one
				// range lookup
				key.setData(ccw.toByteArray());
				OperationStatus os = c.getSearchKeyRange(key, value, LockMode.DEFAULT);
				while (os != OperationStatus.NOTFOUND) {
					Id id = store.deserializeId(key.getData());
					if (id.compareTo(cw) < 0) {
						entriesForIds.addDataFromBytes(value.getData());
					} else {
						break;
					}
					os = c.getNext(key, value, LockMode.DEFAULT);
				}
			} else {
				// Range wraps around zero, so we have to do two range lookups
				OperationStatus os = c.getFirst(key, value, LockMode.DEFAULT);
				while (os != OperationStatus.NOTFOUND) {
					Id id = store.deserializeId(key.getData());
					if (id.compareTo(cw) < 0) {
						entriesForIds.addDataFromBytes(value.getData());
					} else {
						break;
					}
					os = c.getNext(key, value, LockMode.DEFAULT);
				}

				key.setData(ccw.toByteArray());
				os = c.getSearchKeyRange(key, value, LockMode.DEFAULT);
				while (os != OperationStatus.NOTFOUND) {
					entriesForIds.addDataFromBytes(value.getData());
					os = c.getNext(key, value, LockMode.DEFAULT);
				}
			}

		} finally {
			if (c != null) {
				c.close();
			}
		}
		return entriesForIds;
	}

	/**
	 * Get the Pastry IDs stored at this peer that fall in the specified range
	 * 
	 * @param t				The BerkeleyDB transaction to use
	 * @param idrange		The range of interest
	 * @return				The set of IDs in that range at this peer
	 * @throws DatabaseException
	 */
	@SuppressWarnings("unchecked")
	IdSet getIds(Transaction t, IdRange idrange) throws DatabaseException {
		IdSet idset = new IdSet();

		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry value = new DatabaseEntry();
		value.setPartial(0, 0, true);

		Cursor c = null;

		try {
			c = idsDb.openCursor(t, null);

			Id ccw = idrange.getCCWId(), cw = idrange.getCWId();
			if (ccw.compareTo(cw) < 0) {
				// Range does not wrap around zero, so we only have to do one
				// range lookup
				key.setData(ccw.toByteArray());
				OperationStatus os = c.getSearchKeyRange(key, value, LockMode.DEFAULT);
				while (os != OperationStatus.NOTFOUND) {
					Id id = store.deserializeId(key.getData());
					if (id.compareTo(cw) < 0) {
						idset.addId(id);
					} else {
						break;
					}
					os = c.getNext(key, value, LockMode.DEFAULT);
				}
			} else {
				// Range wraps around zero, so we have to do two range lookups
				OperationStatus os = c.getFirst(key, value, LockMode.DEFAULT);
				while (os != OperationStatus.NOTFOUND) {
					Id id = store.deserializeId(key.getData());
					if (id.compareTo(cw) < 0) {
						idset.addId(id);
					} else {
						break;
					}
					os = c.getNext(key, value, LockMode.DEFAULT);
				}

				key.setData(ccw.toByteArray());
				os = c.getSearchKeyRange(key, value, LockMode.DEFAULT);
				while (os != OperationStatus.NOTFOUND) {
					Id id = store.deserializeId(key.getData());
					idset.addId(id);
					os = c.getNext(key, value, LockMode.DEFAULT);
				}
			}
		} finally {
			if (c != null) {
				c.close();
			}
		}
		return idset;
	}


	P2PMessage processMessage(ReplicationControllerMessage rcm, Transaction t, P2PStore.MessageProcessorThread processor ) throws DatabaseException {
		// Message to be sent after processing is done
		P2PMessage m = null;
		if (rcm instanceof TryToFindId) {
			TryToFindId ttfi = (TryToFindId) rcm;
			if (hasEntriesForId(t,ttfi.id)) {
				m = new IdFound(ttfi);
			} else {
				m = new IdNotFound(ttfi);
			}
		} else if (rcm instanceof DataToAdd) {
			DataToAdd dta = (DataToAdd) rcm;
			dta.decode(t, store.epochController, store.peerController, store.transactionController);
		} else if (rcm instanceof RequestDataForId) {
			RequestDataForId rdfi = (RequestDataForId) rcm;

			Entries e = getEntriesForId(t, rdfi.getId());
			if (e.isEmpty()) {
				m = new ReplyFailure("Could not find any data for Id " + rdfi.getId(), rdfi);
			} else {
				m = new DataToAdd(t, e, store.epochController, store.peerController, store.transactionController, rdfi);
			}
		} else if (rcm instanceof GetRemoteRange) {
			m = new RemoteRangeIs(store.getLocalRange(replicationFactor), (GetRemoteRange) rcm);
		} else {
			m = new ReplyFailure("Replication controller received unexpected message: " + rcm, rcm);
		}

		return m;
	}

	public boolean exists(Id id) {
		try {
			return hasEntriesForId(null, id);
		} catch (DatabaseException de) {
			throw new RuntimeException(de);
		}
	}

	public void existsInOverlay(Id id, final Continuation c) {
		P2PMessage m = new TryToFindId(id);
		store.sendMessageAwaitReply(m, new ReplyContinuation() {
			boolean done = false;
			synchronized public boolean isFinished() {
				return done;
			}

			public void processReply(P2PMessage m, MessageProcessorThread mpt) {
				if (m instanceof IdFound) {
					c.receiveResult(true);
				} else if (m instanceof IdNotFound) {
					c.receiveResult(false);
				} else {
					c.receiveException(new Exception("Received unexpected reply: " + m));
				}
				synchronized (this) {
					done = true;
				}
			}
		}, IdFound.class, IdNotFound.class);
	}

	public void fetch(Id id, NodeHandle hint, final Continuation c) {
		addWorkUnit(new Fetch(id, hint, c));
	}

	public void reInsert(Id id, Continuation c) {
		addWorkUnit(new ReInsert(id,c));
	}

	public void remove(Id id, Continuation c) {
		addWorkUnit(new Remove(id,c));
	}

	public IdSet scan(IdRange idrange) {
		int retryCount = 0;

		IdSet ids = null;
		Transaction t = null;
		synchronized (this) {
			if (dbClosing) {
				return null;
			}
			++numCurrentScans;
		}
		try {
			for ( ; ; ) {
				try {
					t = store.beginTxn();
					ids = getIds(t, idrange);
					t.commit();
					break;
				} catch (DeadlockException de) {
					t.abort();
					++retryCount;
					if (retryCount > MAX_DEADLOCK_RETRIES) {
						System.err.println("Too many deadlocks in ReplicationController.getIds");
					}
				}
			}
		} catch (DatabaseException de) {
			System.err.println("Error in ReplicationController.IdSet: " + de);
		} finally {
			synchronized (this) {
				--numCurrentScans;
				if (numCurrentScans == 0) {
					notify();
				}
			}
		}

		return ids;
	}

	/**
	 * Send a message to every node in the replica set for the supplied Id
	 * 
	 * @param id			The data to send the message to
	 * @param dta			The DataToAdd message to send
	 */
	private void sendToReplicaSet(Id id, ClonableMessage cm) {
		NodeHandleSet replicaNodes = store.getReplicaSet(id, replicationFactor);
		for (int i = 0; i < replicaNodes.size(); ++i) {
			P2PMessage newM = cm.cloneMessage(replicaNodes.getHandle(i));
			store.sendMessage(newM);
		}
	}

	void shareReconciliationData(PidAndRecno par, byte[] entry) {
		DataToAdd dta = new DataToAdd();
		dta.addReconciliation(par, entry);
		dta.finish();
		addWorkUnit(new SendData(store.getId(par), dta));
	}

	void shareEpochData(int epoch, byte[] entry) {
		DataToAdd dta = new DataToAdd();
		dta.addEpoch(epoch, entry);
		dta.finish();
		addWorkUnit(new SendData(store.getId(epoch), dta));
	}

	void shareTransactionData(TxnPeerID tpi, byte[] entry) {
		DataToAdd dta = new DataToAdd();
		dta.addTransaction(tpi, entry);
		dta.finish();
		addWorkUnit(new SendData(store.getId(tpi), dta));
	}

	void shareTrustConditions(AbstractPeerID pid, byte[] trustConditions) {
		DataToAdd dta = new DataToAdd();
		dta.addPeer(pid, trustConditions);
		dta.finish();
		addWorkUnit(new SendData(store.getId(pid), dta));
	}

	void fetchNow(Transaction t, final Id id) throws InterruptedException, DatabaseException {
		NodeHandleSet replicaNodes = store.getReplicaSet(id, replicationFactor);
		ReplyHolder<Id> replies = new ReplyHolder<Id>(replicaNodes.size());

		for (int i = 0; i < replicaNodes.size(); ++i) {
			P2PMessage m = new RequestDataForId(id, replicaNodes.getHandle(i), false);
			store.sendMessageAwaitReply(m, new SimpleReplyContinuation<Id>(replicaNodes.getHandle(i).getId(), replies), 0, 0, P2PStore.DEFAULT_MESSAGE_TIMEOUT, P2PMessage.class);
		}

		replies.waitUntilFinished();

		for (P2PMessage reply : replies) {
			if (reply instanceof DataToAdd) {
				DataToAdd dta = (DataToAdd) reply;
				dta.decode(t, store.epochController, store.peerController, store.transactionController);
			}
			// Ignore errors, since it most likely means that they didn't have anything for the
			// specified ID
		}
	}

	void replicateToNewPeer(NodeHandle newHandle) {
		addWorkUnit(new ReplicateToNewPeer(newHandle));
	}


	private static class SerializedIdComparator implements Comparator<byte[]>, Serializable {
		private static final long serialVersionUID = 1L;
		IdFactory idFactory;

		SerializedIdComparator(IdFactory idf) {
			idFactory = idf;
		}

		@SuppressWarnings("unchecked")
		public int compare(byte[] b1, byte[] b2) {
			Id id1 = idFactory.getIdFromByteArray(b1);
			Id id2 = idFactory.getIdFromByteArray(b2);
			return id1.compareTo(id2);
		}

		private void writeObject(ObjectOutputStream out) throws IOException {
			out.writeObject(idFactory.getClass().getCanonicalName());
		}

		private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
			String className = (String) in.readObject();
			try {
				idFactory = (IdFactory) Class.forName(className).newInstance();
			} catch (Exception e) {
				throw new RuntimeException("Could not deserialize Id Comparator: " + e);
			}
		}
	}

	private class WorkUnitThread extends Thread {
		WorkUnitThread(ThreadGroup tg) {
			super(tg, "ReplicationController " + store.getPeerID() + " WorkUnitThread");
		}

		private boolean interrupted = false;
		private boolean waiting = false;

		public synchronized void interrupt() {
			interrupted = true;
			if (waiting) {
				super.interrupt();
			}
		}

		public void run() {
			for ( ; ; ) {
				synchronized (this) {
					if (interrupted) {
						return;
					}
					waiting = true;
				}
				WorkUnit wu = null;
				try {
					wu = workUnits.take();
				} catch (InterruptedException ie) {
					return;
				}
				synchronized (this) {
					waiting = false;
					if (interrupted) {
						return;
					}
				}
				int retryCount = 0;

				for ( ; ; ) {
					Transaction t = null;

					try {
						t = store.beginTxn();
						Object o = wu.process(t);
						t.commit();
						wu.processResult(o);

					} catch (DeadlockException de) {
						try {
							t.abort();
						} catch (DatabaseException e) {
							System.err.println("Chouldn't abort transaction: " + e);
							return;
						}
						++retryCount;
						if (retryCount < MAX_DEADLOCK_RETRIES) {
							continue;
						} else {
							System.err.println("Too many deadlocks while processing ReplicationController.WorkUnit" + wu);
							de.printStackTrace();
						}
					} catch (DatabaseException de) {
						System.err.println("BerkeleyDB error processing ReplicationController.WorkUnit" + wu);
						de.printStackTrace();
						try {
							t.abort();
						} catch (DatabaseException e) {
							System.err.println("Chouldn't abort transaction: " + e);
						}
					} catch (Exception e) {
						System.err.println("Caught exception while processing ReplicationController.WorkUnit " + wu);
						e.printStackTrace();
					}
					break;
				}
			}
		}
	}

	private abstract class WorkUnit {
		private final Continuation c;

		WorkUnit(Continuation c) {
			this.c = c;
		}

		abstract Object process(Transaction t) throws DatabaseException;

		void processResult(Object o) {
			if (o == null) {
				return;
			}
			if (o instanceof P2PMessage) {
				store.sendMessage((P2PMessage) o);
				if (c != null) {
					c.receiveResult(true);
				}
			} else if (c != null) {
				if (o instanceof Exception) {
					c.receiveException((Exception) o);
				} else {
					c.receiveResult(o);
				}
			}
		}
	}

	private class ReInsert extends WorkUnit {
		final Id id;

		ReInsert(Id id, Continuation c) {
			super(c);
			this.id = id;
		}

		@Override
		Object process(Transaction t) throws DatabaseException {
			Entries efi = getEntriesForId(t, id);
			if (efi == null) {
				return false;
			}
			return new DataToAdd(t, efi, store.epochController, store.peerController, store.transactionController, id);
		}
	}

	private class Remove extends WorkUnit {
		final Id id;

		Remove(Id id, Continuation c) {
			super(c);
			this.id = id;
		}

		@Override
		Object process(Transaction t) throws DatabaseException {
			Entries efi = getEntriesForId(t, id);
			if (efi == null) {
				return false;
			}
			for (int epoch : efi.epochs) {
				store.epochController.removeEpoch(t, epoch);
			}

			for (AbstractPeerID pid : efi.pids) {
				store.peerController.removePeer(t, pid);
			}

			for (TxnPeerID tpi : efi.tids) {
				store.transactionController.removeTransaction(t, tpi);
			}

			for (PidAndRecno par : efi.recons) {
				store.peerController.removeReconciliation(t, par);
			}
			// Now that everything has been removed from the other controllers,
			// clear the id from the replication controller as well
			DatabaseEntry key = new DatabaseEntry(id.toByteArray());
			idsDb.delete(t, key);
			return true;
		}
	}

	private class Fetch extends WorkUnit {
		final Id id;
		final NodeHandle hint;
		final Continuation c;
		Fetch(Id id, NodeHandle hint, Continuation c) {
			super(null);
			this.id = id;
			this.hint = hint;
			this.c = c;
		}
		@Override
		Object process(Transaction t) {
			P2PMessage m = new RequestDataForId(id, hint, true);
			ReplyContinuation rc = new ReplyContinuation() {
				boolean finished = false;
				synchronized public boolean isFinished() {
					return finished;
				}

				public void processReply(P2PMessage m, MessageProcessorThread mpt) {
					if (m instanceof DataToAdd) {
						addWorkUnit(new Store((DataToAdd) m, c));
					} else {
						c.receiveException(new Exception("Recevied unexpected reply to RequestDataForId: " + m));
					}
					synchronized (this) {
						finished = true;
					}
				}

			};
			store.sendMessageAwaitReply(m, rc, DataToAdd.class);
			return null;
		}
	}

	private class Store extends WorkUnit {
		final DataToAdd dta;
		Store(DataToAdd dta, Continuation c) {
			super(c);
			this.dta = dta;
		}
		@Override
		Object process(Transaction t) throws DatabaseException {
			dta.decode(t, store.epochController, store.peerController, store.transactionController);
			return true;
		}
	}

	private class SendData extends WorkUnit {
		final Id id;
		final DataToAdd dta;

		SendData(Id id, DataToAdd dta) {
			super(null);
			this.id = id;
			this.dta = dta;
		}

		@Override
		Object process(Transaction t) {
			sendToReplicaSet(id, dta);
			return null;
		}
	}

	private class ReplicateToNewPeer extends WorkUnit {
		final NodeHandle newHandle;

		ReplicateToNewPeer(NodeHandle newHandle) {
			super(null);
			this.newHandle = newHandle;
		}

		@Override
		Object process(Transaction t) throws DatabaseException {
			IdRange range = store.getRange(newHandle,replicationFactor);
			if (range == null) {
				return null;
			}

			Entries e = getEntriesForIds(t,range);

			return new DataToAdd(t,e,store.epochController,store.peerController,
					store.transactionController, newHandle);
		}
	}

	IdRange getRemoteRange(NodeHandle nh) throws UnexpectedReply, InterruptedException {
		ReplyHolder<Integer> replies = new ReplyHolder<Integer>(1);

		P2PMessage grr = new GetRemoteRange(nh);

		store.sendMessageAwaitReply(grr, new SimpleReplyContinuation<Integer>(1,replies), RemoteRangeIs.class, ReplyException.class);

		replies.waitUntilFinished();

		P2PMessage reply = replies.getReply(1);

		if (reply instanceof RemoteRangeIs) {
			return ((RemoteRangeIs) reply).range;
		} else {
			throw new UnexpectedReply(grr, reply);
		}
	}

	String getLocalData() {
		try {
			Entries e = new Entries();
			Cursor c = null;
			try {
				c = idsDb.openCursor(null, CursorConfig.READ_UNCOMMITTED);
				DatabaseEntry key = new DatabaseEntry(), value = new DatabaseEntry();
				OperationStatus os = c.getFirst(key, value, null);
				while (os != OperationStatus.NOTFOUND) {
					e.addDataFromBytes(value.getData());
					os = c.getNext(key, value, null);
				}
			} finally {
				if (c != null) {
					c.close();
				}
			}
			return e.toString();
		} catch (DatabaseException de) {
			return "Error retrieving contents: " + de;
		}
	}
}
