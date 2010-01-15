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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.ByteBufferReader;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.TrustConditions;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.datamodel.Update;
import edu.upenn.cis.orchestra.datamodel.exceptions.CompareMismatch;
import edu.upenn.cis.orchestra.reconciliation.ISchemaIDBinding;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore.TxnStatus;
import edu.upenn.cis.orchestra.reconciliation.p2pstore.PeerController.ReconciliationRecordException;
import edu.upenn.cis.orchestra.util.Cache;
import edu.upenn.cis.orchestra.util.EntryBoundLRUCache;

class TransactionController {
	static final int numThreads = 1;
	private Database transactionDb;
	private ISchemaIDBinding schemaStore;
	private Schema defaultSchema;
	private P2PStore store;
	private Cache<AbstractPeerID,TrustConditions> trustConditions;


	TransactionController(ThreadGroup tg, P2PStore store, ISchemaIDBinding s, 
			Schema schema, Environment env,
			String transactionDbName) throws DatabaseException {
		this.schemaStore = s;
		this.defaultSchema = schema;
		this.store = store;
		trustConditions = new EntryBoundLRUCache<AbstractPeerID,TrustConditions>(128);
		DatabaseConfig dc = new DatabaseConfig();
		dc.setAllowCreate(true);
		dc.setSortedDuplicates(false);
		dc.setTransactional(true);
		transactionDb = env.openDatabase(null, transactionDbName, dc);
	}

	public void halt() throws InterruptedException, DatabaseException {
		transactionDb.close();
	}

	/**
	 * Get the status of the specified transaction for the specified peer
	 * at that peer's specified reconciliation
	 * 
	 * @param pid			The peer to check the transaction for
	 * @param tpi			The transaction of intereest
	 * @param recno			The reconciliation of interest
	 * @param mostRecentRecno
	 * 						The peer's most recent reconciliation
	 * @return				Whether the peer has accepted or rejected the transaction
	 * 						or not, and if so, at what reconciliation
	 * @throws InterruptedException 
	 * @throws ReconciliationRecordException 
	 * @throws UnexpectedReply 
	 */
	TxnStatus getTxnStatus(AbstractPeerID pid, TxnPeerID tpi, int recno, int mostRecentRecno, P2PStore.MessageProcessorThread processor) throws InterruptedException, UnexpectedReply, ReconciliationRecordException {
		return store.peerController.getTxnStatus(pid, recno, tpi, mostRecentRecno, processor);
	}

	P2PMessage processMessage(TransactionControllerMessage tcm, Transaction t, P2PStore.MessageProcessorThread processor) throws DatabaseException, InterruptedException {
		if (tcm instanceof PublishedTxn) {
			PublishedTxn pt = (PublishedTxn) tcm;
			ByteBufferReader bbr = new ByteBufferReader(schemaStore, pt.data, 0, pt.data.length);
			Update u = bbr.readUpdate();
			TxnPeerID tid = u.getLastTid();

			if (addTransaction(t, tid, pt.data)) {
				store.replicationController.shareTransactionData(tid, pt.data);
				return new ReplySuccess(tcm);
			} else {
				return new ReplyFailure("Transaction " + tid + " already published", tcm);
			}
		} else if (tcm instanceof RequestTxnForReconciliation) {
			RequestTxnForReconciliation rtfr = (RequestTxnForReconciliation) tcm;
			return handleRequestTxnForRecon(t, rtfr, processor);
		} else if (tcm instanceof RequestTxn) {
			RequestTxn rt = (RequestTxn) tcm;
			TxnPeerID tpi = rt.getTid();
			DatabaseEntry key = new DatabaseEntry(tpi.getBytes()), value = new DatabaseEntry();
			OperationStatus os = transactionDb.get(t, key, value, LockMode.DEFAULT);
			if (os == OperationStatus.NOTFOUND) {
				try {
					store.replicationController.fetchNow(t,store.getId(tpi));
				} catch (InterruptedException ie) {
				}
				os = transactionDb.get(t, key, value, LockMode.DEFAULT);
			}
			if (os == OperationStatus.SUCCESS) {
				return new PublishedTxnIs(value.getData(), rt);
			} else {
				return new CouldNotRetrieveTxn(tpi, "Transaction not found in DB", rt);
			}
		} else {
			return new ReplyException("Received unexpected message: " + tcm, tcm);
		}
	}

	private P2PMessage handleRequestTxnForRecon(Transaction t, RequestTxnForReconciliation rtfr, P2PStore.MessageProcessorThread processor)
	throws DatabaseException, InterruptedException {
		AbstractPeerID pid = rtfr.getPid();
		TxnPeerID tpi = rtfr.getTid();
		int recno = rtfr.getRecno();
		RequestTxnForReconciliation origRequest = rtfr.getOrigMsg();
		P2PMessage m = null;
		List<Update> txn = null;
		boolean needAntecedents = false;
		TxnStatus status = null;
		try {
			status = getTxnStatus(pid, tpi, recno, rtfr.getMostRecentRecno(), processor);
		} catch (Exception e) {
			m = new ReplyException("Error retrieving transction status", e, rtfr);
		}
		if (status == null) {
			// We've already set m to be a ReplyException
		} else if (status.isRejected()) {
			m = new TxnAlreadyRejected(tpi, rtfr);
		} else if (status.isAccepted()) {
			m = new TxnNotNeeded(tpi, rtfr);
		} else {
			DatabaseEntry key = new DatabaseEntry(tpi.getBytes());
			DatabaseEntry value = new DatabaseEntry();
			byte[] txnContents = null;
			int maxPrio = 0;

			OperationStatus os = transactionDb.get(t, key, value, LockMode.DEFAULT);
			if (os != OperationStatus.SUCCESS) {
				try {
					store.replicationController.fetchNow(t,store.getId(tpi));
				} catch (InterruptedException ie) {
				}
				os = transactionDb.get(t, key, value, LockMode.DEFAULT);
			}
			if (os != OperationStatus.SUCCESS) {
				m = new CouldNotRetrieveTxn(tpi, "Transaction not found in database or replica set", origRequest);
			} else {
				txn = new LinkedList<Update>();
				ByteBufferReader bbr = new ByteBufferReader(schemaStore, value.getData());
				while (! bbr.hasFinished()) {
					txn.add(bbr.readUpdate());
				}
				if (rtfr.withPrio()) {
					TrustConditions tc = trustConditions.probe(pid);
					if (tc == null) {
						// Need to request transaction contents from peer controller
						ReplyHolder<Integer> replies = new ReplyHolder<Integer>(1);

						P2PMessage rtc = new RequestTrustConditions(pid, store.getId(pid));

						store.sendMessageAwaitReply(rtc, new SimpleReplyContinuation<Integer>(1,replies), TrustConditionsAre.class);

						replies.waitUntilFinished(processor);

						P2PMessage reply = replies.getReply(1);
						if (reply instanceof TrustConditionsAre) {
							TrustConditionsAre tca = (TrustConditionsAre) reply;
							tc = tca.getTrustConditions(schemaStore);//defaultSchema);
							trustConditions.store(pid, tc);
						} else {
							m = new CouldNotRetrieveTxn(tpi, "Received unexpected reply to TrustConditionsAre: " + reply, origRequest);
						}

					}
					if (m == null) {
						try {
							for (Update u : txn) {
								int prio = tc.getUpdatePriority(u);
								if (prio == 0) {
									// Not trusted
									maxPrio = 0;
									break;
								} else if (prio > maxPrio) {
									maxPrio = prio;
								}
							}
							if (maxPrio == 0) {
								m = new TxnNotNeeded(tpi, origRequest);
							} else {
								txnContents = value.getData();
								needAntecedents = true;
							}
						} catch (CompareMismatch cm) {
							m = new ReplyFailure("Error processing trust conditions: " + cm.getMessage(), origRequest);
						}
					}
				} else {
					needAntecedents = true;
					txnContents = value.getData();
				}
			}
			// Request antecedent transactions also be sent, if necessary
			List<TxnPeerID> requestedAntecedents = new ArrayList<TxnPeerID>();
			if (needAntecedents) {
				Set<TxnPeerID> antecedents = new HashSet<TxnPeerID>();
				for (Update u : txn) {
					for (TxnPeerID antecedent : u.getPrevTids()) {
						antecedents.add(antecedent);
					}
				}
				for (TxnPeerID antecedentTid : antecedents) {
					if (rtfr.acceptedButNotRecorded(antecedentTid)) {
						continue;
					}
					try {
						status = getTxnStatus(pid, antecedentTid, recno, rtfr.getMostRecentRecno(), processor);
					} catch (Exception e) {
						m = new ReplyException("Error retrieving antecedent transction status", e, rtfr);
						break;
					}
					if (status.isRejected()) {
						m = new AntecedentTxnRejected(antecedentTid, rtfr);
						break;
					} else if (status.isUndecided()) {
						store.sendMessage(new RequestTxnForReconciliation(antecedentTid, origRequest, store.getId(antecedentTid)));
						requestedAntecedents.add(antecedentTid);
					}
				}
			}
			if (m == null) {
				if (rtfr.withPrio()) {
					m = new RetrievedTxn(txnContents, requestedAntecedents, maxPrio, origRequest);
				} else {
					m = new RetrievedTxn(txnContents, requestedAntecedents, origRequest);
				}
			}
		}

		return m;
	}

	/**
	 * Add a transaction to the data stored locally by this transaction controller.
	 * 
	 * @param t				The BerkeleyDB transaction to use
	 * @param tpi			The id of the transaction to add
	 * @param contents		The serialized version of the transaction contents
	 * @return				<code>true</code> if the transaction was added,
	 * 						<code>false</code> if it was already there
	 * @throws DatabaseException
	 */
	boolean addTransaction(Transaction t, TxnPeerID tpi, byte[] contents) throws DatabaseException {
		DatabaseEntry key = new DatabaseEntry(tpi.getBytes());
		DatabaseEntry value = new DatabaseEntry(contents);
		OperationStatus os = transactionDb.putNoOverwrite(t, key, value);
		if (os == OperationStatus.SUCCESS) {
			store.replicationController.add(t, tpi);
			return true;
		} else {
			return false;
		}
	}

	void getTransaction(Transaction t, TxnPeerID tpi, DataToAdd dta) throws DatabaseException, IllegalArgumentException {
		byte[] tpiBytes = tpi.getBytes();
		DatabaseEntry key = new DatabaseEntry(tpiBytes);
		DatabaseEntry value = new DatabaseEntry();

		if (transactionDb.get(t, key, value, LockMode.DEFAULT) != OperationStatus.SUCCESS) {
			throw new IllegalArgumentException("Transaction " + tpi + " not found in transaction controller");
		}		
	}

	void removeTransaction(Transaction t, TxnPeerID tpi) throws DatabaseException, IllegalArgumentException {
		byte[] tpiBytes = tpi.getBytes();
		DatabaseEntry key = new DatabaseEntry(tpiBytes);
		DatabaseEntry value = new DatabaseEntry();
		value.setPartial(0, 0, true);

		if (transactionDb.delete(t, key) != OperationStatus.SUCCESS) {
			throw new IllegalArgumentException("Transaction " + tpi + " not found in transaction controller");
		}		
	}
}
