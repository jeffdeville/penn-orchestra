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

import java.util.ArrayList;
import java.util.List;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

import edu.upenn.cis.orchestra.datamodel.ByteBufferReader;
import edu.upenn.cis.orchestra.datamodel.ByteBufferWriter;
import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.reconciliation.Decision;
import edu.upenn.cis.orchestra.reconciliation.PidAndRecno;
import edu.upenn.cis.orchestra.reconciliation.StateStore;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore.TxnStatus;
import edu.upenn.cis.orchestra.util.Cache;
import edu.upenn.cis.orchestra.util.LRUCache;


class PeerController {
	static final int RECONCILIATION_CHECKPOINT_INTERVAL = 5;
	static final int FIRST_RECNO = StateStore.FIRST_RECNO;
	static final int numThreads = 1;
	private Database peerDb;
	/*
	 * The format of a of a reconciliationDb entry is:
	 * 
	 * integer: number of reconciliation epochs (n)
	 * 
	 * n records of the following format:
	 * integer: recno
	 * integer: epoch
	 * integer: number of accepted transactions (a)
	 * a TxnPeerIDs: the accepted transactions
	 * integer: number of rejected transactions (r)
	 * r TxnPeerIDs: the rejected transactions
	 */	
	private Database reconciliationDb;
	private P2PStore store;

	/* Cache that holds the most recent checkpoint and incremental
	 * reconciliation records (that we know about) for recently used peers.
	 * The entries are always in increasing order by reconciliation entry,
	 * and the checkpoint one goes first, followed by several immediately
	 * following ones
	 * The format of a cache entry is:
	 * 
	 * integer: number of reconciliation entries (n)
	 * integer: the number of the checkpoint entry
	 * reconciliation record: the checkpoint entry
	 * n - 1 reconcilation records: the following reconciliation data
	 * 
	 */

	private Cache<AbstractPeerID,ReconciliationDataCacheEntry> reconciliationDataCache;

	PeerController(ThreadGroup tg, P2PStore store, Environment env, String peerDbName, String decisionDbName) throws DatabaseException {
		this.store = store;

		DatabaseConfig dc = new DatabaseConfig();
		dc.setAllowCreate(true);
		dc.setSortedDuplicates(false);
		dc.setTransactional(true);
		peerDb = env.openDatabase(null, peerDbName, dc);
		reconciliationDb = env.openDatabase(null, decisionDbName, dc);

		reconciliationDataCache = new LRUCache<AbstractPeerID,ReconciliationDataCacheEntry>(
				256 * 1024 /* 256 KB */,
				new LRUCache.GetSize<ReconciliationDataCacheEntry>() {

					public int getSize(ReconciliationDataCacheEntry rdce) {
						int sum = 0;
						for (byte[] record : rdce.reconEntries) {
							sum += record.length;
						}
						return sum;
					}

				});

	}

	void halt() throws InterruptedException, DatabaseException {
		peerDb.close();
		reconciliationDb.close();
	}

	P2PMessage processMessage(PeerControllerMessage pcm, Transaction t, P2PStore.MessageProcessorThread processor) throws DatabaseException, UnexpectedReply, ReconciliationRecordException, InterruptedException {
		P2PMessage m;
		if (pcm instanceof RecordReconciliation) {
			RecordReconciliation rr = (RecordReconciliation) pcm;
			PidAndRecno par = new PidAndRecno(rr.pid, rr.recno);
			byte[] contents = createReconciliationRecord(rr.pid, rr.recno, rr.epoch, rr.accepted, rr.rejected, processor);
			if (addReconciliation(t,par, contents)) {
				m = new ReplySuccess(rr);
				store.replicationController.shareReconciliationData(par, contents);
			} else {
				m = new ReplyFailure("Reconciliation " + rr.recno + " already stored for peer " + rr.pid, rr);
			}
		} else if (pcm instanceof RequestReconciliationEpoch) {
			RequestReconciliationEpoch rre = (RequestReconciliationEpoch) pcm;
			m = new ReconciliationEpochIs(getReconciliationEpoch(rre.getPid(), rre.getRecno(), rre.getMostRecentRecno(), processor), rre);
		} else if (pcm instanceof RequestTrustConditions) {
			RequestTrustConditions rtc = (RequestTrustConditions) pcm;

			DatabaseEntry key = new DatabaseEntry(rtc.getPidBytes());
			DatabaseEntry value = new DatabaseEntry();

			if (peerDb.get(t, key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
				m = new TrustConditionsAre(rtc.getPidBytes(), value.getData(),rtc);
			} else {
				try {
					store.replicationController.fetchNow(t, store.getId(rtc.getID()));
				} catch (InterruptedException ie) {
				}
				if (peerDb.get(t, key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
					m = new TrustConditionsAre(rtc.getPidBytes(), value.getData(),rtc);
				} else {
					m = new ReplyFailure("Trust conditions for peer " + rtc.getID() + " not found", rtc);
				}
			}
		} else if (pcm instanceof TrustConditionsAre) {
			TrustConditionsAre tca = (TrustConditionsAre) pcm;

			AbstractPeerID pid = tca.getPid();

			if (addPeer(t, pid, tca.getTrustConditionsBytes())) {
				m = new ReplySuccess(tca);
				// TODO: make this a very quick call
				store.replicationController.shareTrustConditions(pid, tca.getTrustConditionsBytes());
			} else {
				m = new ReplyFailure("Trust conditions for peer + " + pid + " have already been set", tca);
			}
		} else if (pcm instanceof RequestReconciliationRecord) {
			RequestReconciliationRecord rrr = (RequestReconciliationRecord) pcm;
			DatabaseEntry key = new DatabaseEntry(rrr.getPar().getBytes()), value = new DatabaseEntry();
			OperationStatus os = reconciliationDb.get(t, key, value, LockMode.DEFAULT);
			if (os == OperationStatus.SUCCESS) {
				m = new ReconciliationRecordIs(value.getData(),rrr);
			} else {
				try {
					store.replicationController.fetchNow(t, store.getId(rrr.getPar()));
				} catch (InterruptedException ie) {
				}
				os = reconciliationDb.get(t, key, value, LockMode.DEFAULT);
				if (os == OperationStatus.SUCCESS) {
					m = new ReconciliationRecordIs(value.getData(),rrr);
				} else {
					m = new ReplyFailure("Reconciliation record for " + rrr.getPar() + " not found", rrr);
				}
			}
		} else {
			m = new ReplyException("Received unexpected message: " + pcm, pcm);
		}

		return m;
	}


	/**
	 * Add a trust conditions record to the data stored at this peer controller.
	 * Also informs the replication controller that the peer ID is now stored
	 * at this peer.
	 * 
	 * @param t				The BerkeleyDB transaction to use
	 * @param pid			The PeerID of the peer being added
	 * @param trustConditions	The serialized version of the trust conditions
	 * @return				<code>true</code> if the trust conditions were added,
	 * 						<code>false</code> if trust conditions were already specified
	 * 						for the peer
	 * @throws DatabaseException
	 */
	boolean addPeer(Transaction t, AbstractPeerID pid, byte[] trustConditions) throws DatabaseException {
		byte[] pidBytes = pid.getBytes();
		DatabaseEntry key = new DatabaseEntry(pidBytes);
		DatabaseEntry value = new DatabaseEntry(trustConditions);
		OperationStatus os = peerDb.putNoOverwrite(t, key, value);

		if (os == OperationStatus.SUCCESS) {
			store.replicationController.add(t, pid);
			return true;
		} else {
			return false;
		}
	}

	void getPeer(Transaction t, AbstractPeerID pid, DataToAdd dta) throws DatabaseException, IllegalArgumentException {
		byte[] pidBytes = pid.getBytes();

		DatabaseEntry key = new DatabaseEntry(pidBytes);
		DatabaseEntry value = new DatabaseEntry();

		OperationStatus os = peerDb.get(t, key, value, LockMode.DEFAULT);

		if (os != OperationStatus.SUCCESS) {
			throw new IllegalArgumentException("Peer " + pid + " not found by peer controller");
		}

		byte[] trustConditions = value.getData();

		dta.addPeer(pid, trustConditions);
	}

	void removePeer(Transaction t, AbstractPeerID pid) throws DatabaseException, IllegalArgumentException {
		byte[] pidBytes = pid.getBytes();

		DatabaseEntry key = new DatabaseEntry(pidBytes);		
		OperationStatus os = peerDb.delete(t, key);

		if (os != OperationStatus.SUCCESS) {
			throw new IllegalArgumentException("Peer " + pid + " not found by peer controller");
		}
	}

	private static class ReconciliationDataCacheEntry {
		private int firstRecon;
		private List<byte[]> reconEntries;

		ReconciliationDataCacheEntry(int firstRecon) {
			this.firstRecon = firstRecon;
			reconEntries = new ArrayList<byte[]>(RECONCILIATION_CHECKPOINT_INTERVAL);
		}

		static ReconciliationDataCacheEntry fromBytes(byte[] entry) {
			ByteBufferReader bbr = new ByteBufferReader(null, entry);
			int numRecons = bbr.readInt();
			int firstRecon = bbr.readInt();

			ReconciliationDataCacheEntry retval = new ReconciliationDataCacheEntry(firstRecon);
			while (numRecons > 0) {
				retval.reconEntries.add(bbr.readByteArray());
				--numRecons;
			}

			return retval;
		}

		byte[] getBytes() {
			ByteBufferWriter bbw = new ByteBufferWriter();
			bbw.addToBuffer(reconEntries.size());
			bbw.addToBuffer(firstRecon);
			for (byte[] reconEntry : reconEntries) {
				bbw.addToBuffer(reconEntry);
			}
			return bbw.getByteArray();
		}

		void addRecon(byte[] reconData) {
			reconEntries.add(reconData);
		}

		int getRecnoEpoch(int recno) throws ReconciliationRecordException {
			final int lastRecon = (firstRecon + reconEntries.size() - 1); 
			if (recno > lastRecon) {
				throw new IllegalArgumentException("Only have data up through reconciliation " + lastRecon);
			}
			if (recno <= firstRecon) {
				// Need to pull it out of checkpoint
				ByteBufferReader bbr = new ByteBufferReader(null, reconEntries.get(0));
				int numRecs = bbr.readInt();
				if (numRecs != (firstRecon + 1)) {
					throw new ReconciliationRecordException("Checkpoint has incorrect number of records");
				}
				while (! bbr.hasFinished()) {
					int foundRecno = bbr.readInt();
					int foundEpoch = bbr.readInt();
					if (foundRecno == recno) {
						return foundEpoch;
					}
					int numAccepted = bbr.readInt();
					while (numAccepted > 0) {
						bbr.readByteArray();
						--numAccepted;
					}
					int numRejected = bbr.readInt();
					while (numRejected > 0) {
						bbr.readByteArray();
						--numRejected;
					}
				}
				throw new ReconciliationRecordException("Did not find record for reconciliation " + recno);
			} else {
				// Need to pull it out of individual entry
				ByteBufferReader bbr = new ByteBufferReader(null, reconEntries.get(recno - firstRecon));
				int numRecs = bbr.readInt();
				if (numRecs != 1) {
					throw new ReconciliationRecordException("Found checkpoint record where non-checkpoint record was expected");
				}
				int foundRecno = bbr.readInt();
				if (foundRecno != recno) {
					throw new ReconciliationRecordException("Found record for reconciliation " + foundRecno + " where " + recno + " was expected");
				}
				int epoch = bbr.readInt();
				return epoch;
			}
		}

		List<Decision> getDecisions(int recno, boolean cumulative) throws ReconciliationRecordException {
			List<Decision> retval = new ArrayList<Decision>();
			
			ByteBufferReader bbr = new ByteBufferReader();
			int nextRecno = firstRecon;
			LOOP: for (byte[] entry : reconEntries) {
				bbr.reset(entry);

				final int numRecons = bbr.readInt();
				int numReconsLeft = numRecons;

				while (! bbr.hasFinished()) {
					int foundRecno = bbr.readInt();
					if (foundRecno > recno) {
						break LOOP;
					}
					--numReconsLeft;

					if (foundRecno != nextRecno) {
						throw new ReconciliationRecordException("Found record for reconciliation " + foundRecno + " where " + nextRecno + " was expected");
					}
					++nextRecno;

					@SuppressWarnings("unused")
					int epoch = bbr.readInt();

					boolean relevant = (cumulative || foundRecno == recno);

					int numAccepted = bbr.readInt();
					while (numAccepted > 0) {
						TxnPeerID foundTid = bbr.readTxnPeerID();
						if (relevant) {
							retval.add(new Decision(foundTid, foundRecno, true));
						}
						--numAccepted;
					}

					int numRejected = bbr.readInt();
					while (numRejected > 0) {
						TxnPeerID foundTid = bbr.readTxnPeerID();
						if (relevant) {
							retval.add(new Decision(foundTid, foundRecno, false));
						}
						--numRejected;
					}

					if ((! cumulative) && foundRecno == recno) {
						break LOOP;
					}
				}
				if (numReconsLeft != 0) {
					throw new ReconciliationRecordException("Expected " + (numRecons) + " reconciliation records, but found " + (numRecons - numReconsLeft));
				}
			}

			
			return retval;
		}
		
		TxnStatus getTxnStatus(TxnPeerID tid, int recno) throws ReconciliationRecordException {
			ByteBufferReader bbr = new ByteBufferReader();
			int nextRecno = firstRecon;
			for (byte[] entry : reconEntries) {
				bbr.reset(entry);

				final int numRecons = bbr.readInt();
				int numReconsLeft = numRecons;

				while (! bbr.hasFinished()) {
					int foundRecno = bbr.readInt();
					--numReconsLeft;

					if (foundRecno != nextRecno) {
						throw new ReconciliationRecordException("Found record for reconciliation " + foundRecno + " where " + nextRecno + " was expected");
					}
					++nextRecno;

					if (foundRecno > recno) {
						break;
					}

					@SuppressWarnings("unused")
					int epoch = bbr.readInt();


					int numAccepted = bbr.readInt();
					while (numAccepted > 0) {
						TxnPeerID foundTid = bbr.readTxnPeerID();
						if (foundTid.equals(tid)) {
							return TxnStatus.acceptedAt(foundRecno);
						}
						--numAccepted;
					}

					int numRejected = bbr.readInt();
					while (numRejected > 0) {
						TxnPeerID foundTid = bbr.readTxnPeerID();
						if (foundTid.equals(tid)) {
							return TxnStatus.rejectedAt(foundRecno);
						}
						--numRejected;
					}

				}
				if (numReconsLeft != 0) {
					throw new ReconciliationRecordException("Expected " + (numRecons) + " reconciliation records, but found " + (numRecons - numReconsLeft));
				}
			}
			return TxnStatus.undecided();
		}
	}

	/**
	 * Get a reconciliation data cache entry for the specified peer is
	 * at least up to date enough to answer questions about the specified
	 * reconciliaiton
	 * 
	 * @param pid			The id of the peer of interest
	 * @param recno			That peer's reconciliation of interest
	 * @param mostRecentRecno
	 * 						The most recent reconciliation known for that peer,
	 * 						in case we need to fetch new data
	 * @throws UnexpectedReply 
	 */
	private ReconciliationDataCacheEntry getDataForRecon(AbstractPeerID pid, int recno, int mostRecentRecno, P2PStore.MessageProcessorThread processor) throws InterruptedException, UnexpectedReply {
		if (mostRecentRecno < FIRST_RECNO) {
			throw new IllegalArgumentException("Cannot call getDataForRecon before any reconciliations have occurred");
		}
		if (recno > mostRecentRecno) {
			throw new IllegalArgumentException("Recno cannot be greater than mostRecentRecno");
		}

		ReconciliationDataCacheEntry retval = reconciliationDataCache.probe(pid);

		int mostRecentCheckpoint = mostRecentRecno - (mostRecentRecno % RECONCILIATION_CHECKPOINT_INTERVAL);

		int numNeededRecons = 0;

		if (retval != null) {
			int lastRecon = retval.firstRecon + retval.reconEntries.size() - 1;
			if (recno < lastRecon) {
				return retval;
			} else if ((recno > mostRecentCheckpoint) && (recno < (retval.firstRecon + RECONCILIATION_CHECKPOINT_INTERVAL - 1))) {
				// We need to fetch some more incremental records, but we already have the right checkpoint
				numNeededRecons = recno - retval.firstRecon - retval.reconEntries.size() + 1;
			} else {
				// Otherwise we need to fetch the most recent checkpoint and maybe
				// some incremental records too, so the data from the cache is useless
				retval = null;
			}
		}

		if (retval == null) {
			retval = new ReconciliationDataCacheEntry(mostRecentCheckpoint);
			if (recno > mostRecentCheckpoint) {
				numNeededRecons = recno - mostRecentCheckpoint + 1;
			} else {
				numNeededRecons = 1;
			}
		}

		// Need to fetch records, assemble them into a reconciliation data cache
		// entry, and put it back into the cache

		ReplyHolder<Integer> replies = new ReplyHolder<Integer>(numNeededRecons);
		ArrayList<P2PMessage> sent = new ArrayList<P2PMessage>(numNeededRecons);

		for (int i = 0; i < numNeededRecons; ++i) {
			PidAndRecno par = new PidAndRecno(pid, retval.firstRecon + retval.reconEntries.size() + i);
			P2PMessage m = new RequestReconciliationRecord(pid, par.getRecno(), store.getId(par));
			sent.add(m);
			store.sendMessageAwaitReply(m, new SimpleReplyContinuation<Integer>(i,replies), ReconciliationRecordIs.class);
		}


		replies.waitUntilFinished(processor);

		for (int i = 0; i < numNeededRecons; ++i) {
			P2PMessage m = replies.getReply(i);
			if (m instanceof ReconciliationRecordIs) {
				retval.addRecon(((ReconciliationRecordIs) m).reconciliationRecord);
			} else {
				throw new UnexpectedReply(sent.get(i), m);
			}
		}

		reconciliationDataCache.store(pid, retval);

		return retval;
	}

	int getReconciliationEpoch(AbstractPeerID pid, int recno, int mostRecentRecno, P2PStore.MessageProcessorThread processor) throws InterruptedException, UnexpectedReply, ReconciliationRecordException {
		ReconciliationDataCacheEntry rdce = getDataForRecon(pid,recno,mostRecentRecno, processor);
		return rdce.getRecnoEpoch(recno);
	}

	TxnStatus getTxnStatus(AbstractPeerID pid, int recno, TxnPeerID tpi, int mostRecentRecno, P2PStore.MessageProcessorThread processor) throws InterruptedException, UnexpectedReply, ReconciliationRecordException {
		if (mostRecentRecno < FIRST_RECNO) {
			return TxnStatus.undecided();
		}
		ReconciliationDataCacheEntry rdce = getDataForRecon(pid,recno,mostRecentRecno, processor);
		return rdce.getTxnStatus(tpi, recno);
	}
	
	List<Decision> getDecisions(AbstractPeerID pid, int recno, int mostRecentRecno, boolean cumulative) throws ReconciliationRecordException, InterruptedException, UnexpectedReply {
		ReconciliationDataCacheEntry rdce = getDataForRecon(pid,recno,mostRecentRecno, null);
		return rdce.getDecisions(recno, cumulative);
	}

	/**
	 * Create a serialized representation of a reconciliation that can be stored in
	 * the database
	 * 
	 * @param pid				The reconciling peer
	 * @param recno 			Its reconciliation
	 * @param epoch				The epoch of the most recent transactions considered by this
	 * 							reconciliation, or <code>-1</code> if the reconciliation did
	 * 							not consider any new transactions but only removed conflicts
	 * @param accepted			Transactions accepted by this reconcilation
	 * @param rejected			Transactions rejected by this reconciliation
	 * @return					The serialized representation
	 * @throws DatabaseException
	 * @throws UnexpectedReply 
	 * @throws InterruptedException 
	 */
	byte[] createReconciliationRecord(AbstractPeerID pid, int recno, int epoch, List<TxnPeerID>accepted,
			List<TxnPeerID> rejected, P2PStore.MessageProcessorThread processor) throws DatabaseException, InterruptedException, UnexpectedReply {

		ByteBufferWriter bbw = new ByteBufferWriter();
		bbw.addToBuffer(1);
		bbw.addToBuffer(recno);
		bbw.addToBuffer(epoch);
		bbw.addToBuffer(accepted.size());
		for (TxnPeerID tpi : accepted) {
			bbw.addToBuffer(tpi);
		}
		bbw.addToBuffer(rejected.size());
		for (TxnPeerID tpi : rejected) {
			bbw.addToBuffer(tpi);
		}

		byte[] currRec = bbw.getByteArray();



		if (recno > FIRST_RECNO && recno % RECONCILIATION_CHECKPOINT_INTERVAL == 0) {
			// Get previous checkpoint and supplemental records
			ReconciliationDataCacheEntry rdce = getDataForRecon(pid,recno-1,recno-1,processor);
			int newRecordLength = currRec.length;
			for (byte[] record : rdce.reconEntries) {
				newRecordLength += record.length - IntType.bytesPerInt;
			}

			byte[] retval = new byte[newRecordLength];
			System.arraycopy(IntType.getBytes(recno + 1), 0, retval, 0, IntType.bytesPerInt);
			int pos = IntType.bytesPerInt;
			for (byte[] record : rdce.reconEntries) {
				System.arraycopy(record, IntType.bytesPerInt, retval, pos, record.length - IntType.bytesPerInt);
				pos += record.length - IntType.bytesPerInt;
			}
			System.arraycopy(currRec, IntType.bytesPerInt, retval, pos, currRec.length - IntType.bytesPerInt);
			return retval;
		} else {
			return currRec;
		}
	}

	boolean addReconciliation(Transaction t, PidAndRecno par, byte[] contents) throws DatabaseException {
		DatabaseEntry key = new DatabaseEntry(par.getBytes()), value = new DatabaseEntry(contents);
		OperationStatus os = reconciliationDb.putNoOverwrite(t, key, value);
		if (os == OperationStatus.SUCCESS) {
			store.replicationController.add(t, par);
			return true;
		} else {
			return false;
		}
	}

	void getReconciliation(Transaction t, PidAndRecno par, DataToAdd dta) throws DatabaseException, IllegalArgumentException {
		DatabaseEntry key = new DatabaseEntry(par.getBytes()), value = new DatabaseEntry();
		OperationStatus os = reconciliationDb.get(t, key, value, LockMode.DEFAULT);
		if (os == OperationStatus.SUCCESS) {
			dta.addReconciliation(par, value.getData());
		} else {
			throw new IllegalArgumentException("Reconciliation " + par + " not found by peer controller");
		}
	}

	void removeReconciliation(Transaction t, PidAndRecno par) throws DatabaseException, IllegalArgumentException {
		DatabaseEntry key = new DatabaseEntry(par.getBytes());
		OperationStatus os = reconciliationDb.delete(t, key);
		if (os != OperationStatus.SUCCESS) {
			throw new IllegalArgumentException("Reconciliation " + par + " not found by peer controller");
		}
	}

	static class ReconciliationRecordException extends Exception {
		private static final long serialVersionUID = 1L;
		final String what;
		ReconciliationRecordException(String what) {
			super(what);
			this.what = what;
		}

		public String toString() {
			return what;
		}
	}
}
