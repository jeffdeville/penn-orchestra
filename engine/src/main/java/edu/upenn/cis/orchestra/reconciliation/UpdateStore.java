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

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.sleepycat.je.DatabaseException;

import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.TrustConditions;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.datamodel.Update;
import edu.upenn.cis.orchestra.datamodel.iterators.FlatteningIterator;
import edu.upenn.cis.orchestra.datamodel.iterators.IntegerIterator;
import edu.upenn.cis.orchestra.datamodel.iterators.IteratorException;
import edu.upenn.cis.orchestra.datamodel.iterators.ResultIterator;
import edu.upenn.cis.orchestra.reconciliation.bdbstore.BerkeleyDBStoreClient;
import edu.upenn.cis.orchestra.util.XMLParseException;

public abstract class UpdateStore implements TransactionDecisions {
	public interface Factory {
		public UpdateStore getUpdateStore(AbstractPeerID pid, ISchemaIDBinding sch, Schema s, TrustConditions tc) throws UpdateStore.USException;

		/**
		 * Returns a client which can be used to query and update an {@code
		 * UpdateStore}'s {@SchemaIDBinding}.
		 * 
		 * @return a {@code ISchemaIDBindingClient} appropriate for this
		 *         system's {@code UpdatesStore}
		 */
		public ISchemaIDBindingClient getSchemaIDBindingClient();

		/**
		 * If the server is local, attempts to start server.
		 * 
		 * @throws USException
		 * 
		 */
		public void startUpdateStoreServer() throws USException;
		
		/**
		 * Attempts to shutdown the Update Store.
		 * 
		 * @throws USException
		 */
		public void stopUpdateStoreServer() throws USException;
		
		/**
		 * Returns {@code true} if the update store server is running.
		 * 
		 * @return {@code true} if the update store server is running
		 */
		public boolean updateStoreServerIsRunning();
		
		public void serialize(Document doc, Element update);
		public void resetStore(Schema s) throws USException;
		USDump dumpUpdateStore(ISchemaIDBinding binding, Schema schema) throws USException;
		void restoreUpdateStore(USDump d) throws USException;
		public boolean isLocal();
	}
	public static class USException extends DbException {
		private static final long serialVersionUID = 1L;

		public USException() {
			super();
		}

		public USException(String arg0) {
			super(arg0);
		}

		public USException(String arg0, Throwable arg1) {
			super(arg0, arg1);
		}

		public USException(Throwable arg0) {
			super(arg0);
		}
	}
	
	public static class AlreadyRejectedAntecedent extends USException {
		private static final long serialVersionUID = 1L;
		TxnPeerID prevTid;
		AlreadyRejectedAntecedent(TxnPeerID prevTid) {
			super("Already rejected " + prevTid);
			this.prevTid = prevTid;
		}
	}

	/**
	 * Disconnect from the update store.
	 * 
	 * @throws USException
	 */
	public abstract void disconnect() throws USException;

	/**
	 * Reconnect to the update store.
	 * 
	 * @throws USException
	 */
	public abstract void reconnect() throws USException;

	public abstract boolean isConnected();

	/**
	 * Publish this peer's updates to the shared database
	 * 
	 * 
	 * @param txns		The list of updates to be published
	 * @throws USException
	 */
	public abstract void publish(List<List<Update>> txns) throws USException;

	/**
	 * Record that this peer has accepted and rejected the specified
	 * transactions at the specified reconciliation
	 * 
	 * This method updates the transaction status cache and then calls
	 * <code>recordTxnDecisionsImpl</code>.
	 * 
	 * @param decisions			Decisions as to which transactions to accept and reject
	 * @throws USException
	 */
	public final void recordTxnDecisions(Iterable<Decision> decisions) throws USException {
		int recno = getCurrentRecno() - 1;

		synchronized (statusCache) {
			for (Decision td : decisions) {
				if (td.accepted) {
					statusCache.put(td.tpi, TxnStatus.acceptedAt(recno));
				} else {
					statusCache.put(td.tpi, TxnStatus.rejectedAt(recno));
				}
			}
		}

		recordTxnDecisionsImpl(decisions);
	}

	/**
	 * Subclass method that actually publishes the transaction decisions for the last
	 * reconciliation to the database
	 * 
	 * @param decisions
	 * @throws USException
	 */
	protected abstract void recordTxnDecisionsImpl(Iterable<Decision> decisions) throws USException;

	/**
	 * Record that this peer has decided to reconcile. This <code>empty</code> is true,
	 * then this reconciliation is just to record decisions based on deferred transactions,
	 * and therefore has the same epoch as the previous reconciliation. If it is false,
	 * this peer's next reconciliation considers all transactions up to the
	 * first one which has not finished being published.
	 * 
	 * @param empty			<code>true</code> if this reconciliation is being used to
	 * 						record decisions based on deferred transactions, and
	 * 						<code>false</code> if it should consider newly published
	 * 						transactions as well
	 * 
	 * @throws USException
	 */
	public abstract void recordReconcile(boolean empty) throws USException;

	/**
	 * Retrieve all of the undecided, trusted transactions from the specified
	 * reconciliation
	 * 
	 * @param recno				The reconciliation for which to start requesting data
	 * @param ownAcceptedTxns	IDs of the participants own transactions that it
	 * 							has accepted but has not yet told the update store
	 * 							about
	 * @param trustedTxns		A mapping from priority to a list of trusted transactions
	 * 							for that priority
	 * @param mustReject		Transactions that must be rejected due to rejected antecedents
	 * 
	 * @throws USException
	 */
	public abstract void getReconciliationData(int recno, Set<TxnPeerID> ownAcceptedTxns, Map<Integer,List<TxnChain>> trustedTxns, Set<TxnPeerID> mustReject) throws USException;

	/**
	 * Gets the reconciliation number of this peer's current reconciliation.
	 * 
	 * @return				The reconciliation number
	 * @throws USException
	 */
	public abstract int getCurrentRecno() throws USException;

	public interface TransactionSource {
		/**
		 * Retrieve a transaction from an abstract source of transactions
		 * 
		 * @param tpi			The ID of the transaction to retrieve
		 * @return				The contents of the transaction, or
		 * 						<code>null</code> if it is not available for some
		 * 						reason.
		 * @throws USException
		 */
		List<Update> getTxn(TxnPeerID tpi) throws USException;
	}


	public abstract Benchmark getBenchmark();
	public abstract void setBenchmark(Benchmark b) throws USException;

	public static class TxnStatus implements Serializable {
		private static final long serialVersionUID = 1L;
		private boolean accepted;
		private boolean rejected;
		private int recno;

		private TxnStatus(boolean accepted, boolean rejected, int recno) {
			this.accepted = accepted;
			this.rejected = rejected;
			this.recno = recno;
		}

		public static TxnStatus acceptedAt(int recno) {
			return new TxnStatus(true, false, recno);
		}

		public static TxnStatus rejectedAt(int recno) {
			return new TxnStatus(false, true, recno);
		}

		public static TxnStatus undecided() {
			return new TxnStatus(false, false, Integer.MAX_VALUE);
		}

		public boolean isAcceptedAt(int recno) {
			return (this.accepted && this.recno <= recno);
		}

		public boolean isRejectedAt(int recno) {
			return (this.rejected && this.recno <= recno);
		}

		public boolean isRejected() {
			return rejected;
		}

		public boolean isAccepted() {
			return accepted;
		}

		public boolean isUndecided() {
			return (! (accepted || rejected));
		}

	}

	// Cache of transaction status records for the owning peer
	private Map<TxnPeerID,TxnStatus> statusCache = Collections.synchronizedMap(new HashMap<TxnPeerID,TxnStatus>());
	// True if entries not in the cache mean that a transaction has not been decided, false
	// if that is not the case and they must be probed
	private boolean cacheIsComplete = false;


	protected abstract TxnStatus getTxnStatus(TxnPeerID tpi) throws USException;

	private final void getCompleteCache() throws USException {
		statusCache.clear();
		ResultIterator<Decision> decisions = getDecisions();
		try {
			while (decisions.hasNext()) {
				Decision d = decisions.next();
				statusCache.put(d.tpi, d.accepted ? TxnStatus.acceptedAt(d.recno) : TxnStatus.rejectedAt(d.recno));
			}
			decisions.close();
		} catch (IteratorException ie) {
			throw new USException(ie.getMessage(), ie.getCause());
		}
		cacheIsComplete = true;
	}

	public final boolean hasAcceptedTxn(TxnPeerID tpi) throws USException {
		TxnStatus ts;
		ts = statusCache.get(tpi);
		if (ts == null && cacheIsComplete) {
			return false;
		}
		if (ts != null) {
			if (ts.isAccepted()) {
				return true;
			} else if (ts.isRejected()) {
				return false;
			}
		}
		ts = getTxnStatus(tpi);
		statusCache.put(tpi, ts);
		return ts.isAccepted();
	}

	public final boolean hasRejectedTxn(TxnPeerID tpi) throws USException {
		TxnStatus ts;
		ts = statusCache.get(tpi);
		if (ts == null && cacheIsComplete) {
			return false;
		}
		if (ts != null) {
			if (ts.isAccepted()) {
				return false;
			} else if (ts.isRejected()) {
				return true;
			}
		}
		ts = getTxnStatus(tpi);
		statusCache.put(tpi, ts);
		return ts.isRejected();
	}

	/**
	 * Get an iterator over all of a peer's decisions. They are
	 * given in increasing order by reconciliation number, with
	 * acceptances preceeding rejections.
	 * 
	 * @return		The iterator, which must be closed by the caller
	 * @throws USException
	 */
	final public ResultIterator<Decision> getDecisions() throws USException {
		int lastRecno = getCurrentRecno() - 1;
		
		ResultIterator<List<Decision>> it = new IntegerIterator<List<Decision>>(StateStore.FIRST_RECNO, lastRecno) {

			@Override
			protected List<Decision> getData(int recno) throws IteratorException {
				try {
					return getDecisions(recno);
				} catch (USException e) {
					throw new IteratorException(e);
				}
			}
			
		};
		
		try {
			return new FlatteningIterator<Decision>(it);
		} catch (IteratorException e) {
			if (e.getCause() instanceof USException) {
				throw (USException) e.getCause();
			} else {
				throw new USException(e);
			}
		}
		
	}

	/**
	 * Get the decisions for a particular reconciliation (including decisions for
	 * transaction that were deferred and accepted later). Acceptances precede
	 * rejections.
	 * 
	 * @param recno
	 * @return			A list of decisions
	 * @throws USException
	 */
	abstract public List<Decision> getDecisions(int recno) throws USException;
	
	/**
	 * Get an iterator over all of this peers reconciliations,
	 * given in increasing order by reconciliation number.
	 * 
	 * @return		The iterator, which must be closed by the caller
	 * @throws USException
	 */
	abstract public ResultIterator<ReconciliationEpoch> getReconciliations() throws USException;

	/**
	 * Get an iterator over all of the published updates for a particular
	 * relation.
	 * 
	 * @param relname		The relation for which to retrieve the pudates
	 * @return				The iterator, which must be closed by the caller
	 * @throws USException
	 */
	abstract public ResultIterator<Update> getPublishedUpdatesForRelation(String relname) throws USException;

	/**
	 * Retrieve a published transaction from the update store.
	 * 
	 * @param txn		The ID of the transaction to retrieve
	 * @return			The contents of the transaction
	 * @throws USException
	 */
	abstract public List<Update> getTransaction(TxnPeerID txn) throws USException;

	/**
	 * Get an iterator over all of the transactions (trusted or not) that were
	 * published between the specified reconciliation and the participant's
	 * previous reconciliation. If the current reconciliation is specified,
	 * all transactions published since the participant's last reconciliation
	 * will be returned.
	 * 
	 * @param recno			The reconciliation to retrieve transactions for
	 * @return				The iterator, which must be closed by the caller
	 * @throws USException
	 */
	abstract public ResultIterator<TxnPeerID> getTransactionsForReconciliation(int recno) throws USException;

	/**
	 * Resets the update store by clearing any locally cached data
	 * 
	 * @throws USException
	 */
	public void reset() throws USException {
		resetSubclassState();
		getCompleteCache();
	}
	
	abstract public int getLargestTidForPeer() throws USException;
	
	/**
	 * Reset any state stored by the subclass
	 * 
	 * @throws USException
	 */
	protected void resetSubclassState() throws USException {
		// By default, don't do anything
	}
	
	/**
	 * Get the set of transactions accepted at a particular reconciliation
	 * 
	 * @param recno			The reconciliation number of interest
	 * @return				The set of accepted transactions
	 * @throws USException
	 */
	abstract public Map<TxnPeerID, List<Update>> getTransactionsAcceptedAtRecno(int recno) throws USException;


	static public UpdateStore.Factory deserialize(Element update)
			throws XMLParseException {
		String type = update.getAttribute("type");

		if (type.compareToIgnoreCase("sql") == 0) {
			return SqlUpdateStore.Factory.deserialize(update);
		} else if (type.compareToIgnoreCase("bdb") == 0) {
			return BerkeleyDBStoreClient.Factory.deserialize(update);
		}

		throw new XMLParseException("Unrecognized update store type '" + type
				+ "'", update);
	}
}
