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

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

import edu.upenn.cis.orchestra.datamodel.IntType;

class EpochController {
	static final int numThreads = 1;
	static final int MAX_DEADLOCK_RETRIES = 5;
	private P2PStore store;
	// Mapping from epoch -> transaction IDs
	private Database epochDb;

	EpochController(ThreadGroup tg, P2PStore store, Environment env, String epochDbName) throws DatabaseException {
		this.store = store;
		DatabaseConfig dc = new DatabaseConfig();
		dc.setAllowCreate(true);
		dc.setSortedDuplicates(false);
		dc.setTransactional(true);
		epochDb = env.openDatabase(null, epochDbName, dc);
	}

	void halt() throws InterruptedException, DatabaseException {		
		epochDb.close();
	}

	int getMostRecentEpoch(Transaction t) throws DatabaseException {
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry val = new DatabaseEntry();
		Cursor c = null;

		try {
			c = epochDb.openCursor(t, null);
			if (c.getLast(key, val, LockMode.DEFAULT) != OperationStatus.SUCCESS) {
				return -1;
			} else {
				return IntType.getValFromBytes(key.getData());
			}
		} finally {
			if (c != null) {
				c.close();						
			}
		}

	}

	P2PMessage processMessage(EpochControllerMessage ecm, Transaction t, P2PStore.MessageProcessorThread processor) throws DatabaseException {
		if (ecm instanceof PublishEpoch) {
			PublishEpoch pe = (PublishEpoch) ecm;


			P2PMessage reply;
			if (addEpoch(t, pe.epoch, pe.tids)) {
				reply = new ReplySuccess(ecm);
				store.replicationController.add(t, pe.epoch);
				store.replicationController.shareEpochData(pe.epoch, pe.tids);
			} else {
				int mostRecentEpoch = getMostRecentEpoch(t);
				if (mostRecentEpoch < 0) {
					reply = new ReplyException("Cannot determine last epoch", ecm);
				} else {
					reply = new CouldNotPublishEpoch(mostRecentEpoch, pe);
				}
			}
			store.epochAllocator.lastEpochIs(pe.epoch);
			return reply;
		} else if (ecm instanceof RequestMostRecentEpoch) {
			return new MostRecentEpochIs(getMostRecentEpoch(t), (RequestMostRecentEpoch) ecm);
		} else if (ecm instanceof RequestPublishedEpoch) {
			RequestPublishedEpoch rpe = (RequestPublishedEpoch) ecm;

			DatabaseEntry key = new DatabaseEntry(IntType.getBytes(rpe.epoch));
			DatabaseEntry val = new DatabaseEntry();

			OperationStatus os = epochDb.get(t, key, val, LockMode.DEFAULT);
			if (os != OperationStatus.SUCCESS) {
				try {
					store.replicationController.fetchNow(t, store.getId(rpe.epoch));
				} catch (InterruptedException e) {
				}
				os = epochDb.get(t, key, val, LockMode.DEFAULT);
			}
			if (os != OperationStatus.SUCCESS) {
				// Epoch not published
				return new EpochNotPublished(rpe);
			} else {
				return new PublishEpoch(rpe.epoch, val.getData(), rpe);
			}
		} else {
			return new ReplyException("Epoch controller received unexpected message " + ecm, ecm);
		}
	}

	/**
	 * Add an epoch to the epoch controller at this peer. Also informs the replication
	 * controller that the epoch is currently stored at this peer.
	 * 
	 * @param t			The BerkeleyDB transaction to use
	 * @param epoch		The epoch to add
	 * @param txnIds	The serialized version of the transaction IDs for the epoch
	 * @return			<code>true</code> if the epoch was added,
	 * 					<code>false</code> if it already existed
	 * @throws DatabaseException
	 */
	boolean addEpoch(Transaction t, int epoch, byte[] txnIds) throws DatabaseException {
		DatabaseEntry key = new DatabaseEntry(IntType.getBytes(epoch));
		DatabaseEntry value = new DatabaseEntry(txnIds);
		OperationStatus os = epochDb.putNoOverwrite(t, key, value);
		if (os == OperationStatus.SUCCESS) {
			store.replicationController.add(t, epoch);
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Add an epoch to a <code>DataToAdd</code> message so it can be replicated on
	 * another peer.
	 * 
	 * @param t				The BerkeleyDB transaction to use
	 * @param epoch			The epoch to retrieve
	 * @param dta			The message to add the epoch to
	 * @throws DatabaseException
	 * @throws IllegalArgumentException
	 */
	void getEpoch(Transaction t, int epoch, DataToAdd dta) throws DatabaseException, IllegalArgumentException {
		DatabaseEntry key = new DatabaseEntry(IntType.getBytes(epoch));
		DatabaseEntry value = new DatabaseEntry();
		OperationStatus os = epochDb.get(t, key, value, LockMode.DEFAULT);

		if (os != OperationStatus.SUCCESS) {
			throw new IllegalArgumentException("Epoch " + epoch + " not found by epoch controller");
		}

		dta.addEpoch(epoch, value.getData());
	}

	/**
	 * Remove an epoch from those stored at this peer, presumably because this
	 * peer is no longer in the replica set of the epoch's pastry id.
	 * 
	 * @param t					The BerkleyDB transaction to use
	 * @param epoch				The epoch to remove
	 * @throws DatabaseException
	 * @throws IllegalArgumentException
	 */
	void removeEpoch(Transaction t, int epoch) throws DatabaseException, IllegalArgumentException {
		DatabaseEntry key = new DatabaseEntry(IntType.getBytes(epoch));
		OperationStatus os = epochDb.delete(t, key);

		if (os != OperationStatus.SUCCESS) {
			throw new IllegalArgumentException("Epoch " + epoch + " not found by epoch controller");
		}
	}
}