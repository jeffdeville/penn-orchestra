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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.upenn.cis.orchestra.Debug;
import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.Subtuple;
import edu.upenn.cis.orchestra.datamodel.TrustConditions;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.TupleSet;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.datamodel.Update;
import edu.upenn.cis.orchestra.datamodel.exceptions.CompareMismatch;
import edu.upenn.cis.orchestra.datamodel.iterators.ResultIterator;
import edu.upenn.cis.orchestra.reconciliation.ConflictType.ConflictTypeCode;
import edu.upenn.cis.orchestra.reconciliation.StateStore.SSException;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore.TransactionSource;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore.USException;

public class ClientCentricDb extends Db {
	private StateStore state;
	private UpdateStore.Factory usf;
	private StateStore.Factory ssf;
	private UpdateStore updateStore;
	private List<List<Update>> unpublishedTransactions;
	// Set of transaction IDs that this peer has added to the database
	// but not yet published or recorded as accepted during reconciliation
	private Set<TxnPeerID> unpublishedTids;
	// Set of transactions IDs that this peer has published
	// but not yet recorded as accepted during reconciliation
	private Set<TxnPeerID> publishedTids;
	// List (one for each relation) of mapping from key
	// subtuples to sets of reconciliations where
	// those keys become dirty
//	private List<Map<Subtuple,Set<Integer>>> dirty;
	private Map<Integer,Map<Subtuple,Set<Integer>>> dirty;
	// Mapping from recno to conflict to option
	private HashMap<Integer,ArrayList<ArrayList<Update>>> conflicts;
	// Index by old key values of update conflicts for each recno
	private HashMap<Integer,HashMap<Tuple,Integer>> conflictsOldValueIndex;
	// Index by new values of key violations for each recno
	private HashMap<Integer,HashMap<Subtuple,Integer>> conflictsNewKeyIndex;
	// Index by initial values of initial value conflicts for each recno
	private HashMap<Integer,HashMap<ValueTidPair,Integer>> conflictsInitialValueIndex;
	// Mapping from recno to transaction to set of conflict/option pairs
	private HashMap<Integer,HashMap<TxnPeerID,HashSet<ConflictOptionPair>>> deferred;
	// Mapping from recno to conflict/option pair to a set of transactions
	private HashMap<Integer,HashMap<ConflictOptionPair,HashSet<TxnPeerID>>> conflictTxns;
	// Stored deltas to bring the peer from this recno specified by the key to
	// the next one.
	private HashMap<Integer,ArrayList<Update>>  deltas;
	
	//private OrchestraSystem _system;

	protected static class ConflictOptionPair {
		int conflict;
		int option;
		ConflictTypeCode type;
		ConflictOptionPair(int conflict, int option, ConflictTypeCode type) {
			this.conflict = conflict;
			this.option = option;
			this.type = type;
		}
		ConflictOptionPair(int conflict, int option) {
			this.conflict = conflict;
			this.option = option;
			this.type = null;
		}
		public int hashCode() {
			return conflict + 37 * option;
		}
		public boolean equals(Object o) {
			if (o == null || o.getClass() != this.getClass()) {
				return false;
			}
			ConflictOptionPair cop = (ConflictOptionPair) o;
			return (cop.conflict == conflict && cop.option == option);
		}
		public ConflictOptionPair duplicate() {
			return new ConflictOptionPair(conflict, option, type);
		}
	}

	protected static class ValueTidPair {
		Tuple value;
		TxnPeerID tid;
		ValueTidPair(Tuple value, TxnPeerID tid) {
			this.value = value.duplicate();
			this.tid = tid.duplicate();
		}
		public int hashCode() {
			return value.hashCode() + 37 * tid.hashCode();
		}
		public boolean equals(Object o) {
			if (o == null || o.getClass() != this.getClass()) {
				return false;
			}
			ValueTidPair vtp = (ValueTidPair) o;
			if (value == null || tid == null) {
				return false;
			}
			return (value.equals(vtp.value) && tid.equals(vtp.tid));
		}
		public ValueTidPair duplicate() {
			return new ValueTidPair(value,tid);
		}
	}

	/**
	 * Create a new object to represent a peer's view of the shared database.
	 * 
	 * @param s			The schema for the shared table
	 * @param peerID	The ID of the viewing peer
	 * @throws SSException
	 */
	public ClientCentricDb(ISchemaIDBinding sch, Schema s, AbstractPeerID pid, TrustConditions tc, UpdateStore.Factory usf, StateStore.Factory ssf)
	throws DbException {
		super(tc, s);

		this.usf = usf;
		this.ssf = ssf;
		
		//_system = sys;
		
		Debug.println("Initializing " + pid.toString() + " with trust conditions " + tc.toString());

		updateStore = this.usf.getUpdateStore(id, sch, schema, tc);
		state = this.ssf.getStateStore(id, sch, updateStore.getLargestTidForPeer());
		unpublishedTransactions = new ArrayList<List<Update>>();
		unpublishedTids = new HashSet<TxnPeerID>();
		publishedTids = new HashSet<TxnPeerID>();
//		dirty = new ArrayList<Map<Subtuple,Set<Integer>>>(numRelations);
		dirty = new HashMap<Integer,Map<Subtuple,Set<Integer>>>();
//		for (int i = 0; i < numRelations; ++i) {
//			dirty.add(new HashMap<Subtuple,Set<Integer>>());
//		}
		conflicts = new HashMap<Integer,ArrayList<ArrayList<Update>>>();
		conflictsOldValueIndex = new HashMap<Integer,HashMap<Tuple,Integer>>();
		conflictsNewKeyIndex = new HashMap<Integer,HashMap<Subtuple,Integer>>();
		conflictsInitialValueIndex = new HashMap<Integer,HashMap<ValueTidPair,Integer>>();
		deferred = new HashMap<Integer,HashMap<TxnPeerID,HashSet<ConflictOptionPair>>>();
		deltas = new HashMap<Integer,ArrayList<Update>>();
		conflictTxns = new HashMap<Integer,HashMap<ConflictOptionPair,HashSet<TxnPeerID>>>();
		
		if (updateStore.getCurrentRecno() != state.getCurrentRecno()) {
			state.reset();
			replayPreviousReconciliations();
		}		
	}

	public ClientCentricDb(ISchemaIDBinding sch, Schema s, AbstractPeerID pid, UpdateStore.Factory usf, StateStore.Factory ssf)
	throws DbException {
		this(sch, s, pid, new TrustConditions(pid), usf, ssf);
	}

	/**
	 * Close the update store's connection to the shared database
	 * 
	 * @throws USException, SSException
	 */
	public synchronized void disconnect() throws USException, SSException {
		if (updateStore != null) {
			updateStore.disconnect();
			state.close();
		}
	}

	/**
	 * Restore the update store's connection to the shared database
	 * 
	 * @throws DbException
	 */
	public synchronized void reconnect() throws DbException {
		if (updateStore != null) {
			updateStore.reconnect();
			state.reopen();
		}
		if (updateStore.getCurrentRecno() != state.getCurrentRecno()) {
			updateStore.disconnect();
			state.close();
			throw new DbException("Update store and state store are not in sync");
		}
	}

	public synchronized boolean isConnected() {
		if (updateStore == null) {
			return false;
		}
		return updateStore.isConnected();
	}

	/**
	 * Inserts a transaction of updates in the shared database. The ID
	 * of the transaction is automatically computed. The state of <code>peer</code>'s
	 * database is updated as well.
	 *
	 * @param updates The sequence of updates to add to the database.
	 * @return The ID that the transaction was given
	 * @throws DbException 
	 */
	public synchronized TxnPeerID addTransaction(List<Update> updates) throws DbException
	{
		if (updates.isEmpty()) {
			return null;
		}

		// Flatten the transaction before preparing it
		ArrayList<Update> flattened = flatten(updates);

		int recno = state.getCurrentRecno();

		TxnPeerID tpi = state.prepareTransaction(flattened);

		boolean isDirty = false;

		for (Update u : flattened) {
			if (isDirty(recno, u)) {
				isDirty = true;
			}
		}

		if (! isDirty) {
			state.applyTransaction(state.getCurrentRecno(),flattened);
			ArrayList<Update> delta = deltas.get(recno);
			if (delta == null) {
				delta = new ArrayList<Update>();
				deltas.put(recno, delta);
			}
			delta.addAll(flattened);

			unpublishedTids.add(tpi);
		}

		unpublishedTransactions.add(flattened);


		return tpi;
	}



	/**
	 * Determine if the viewing peer has accepted a transaction.
	 * 
	 * @param txn	The transaction to check for
	 * @return		<code>true</code> if it has been accepted,
	 * 				<code>false</code> otherwise
	 * @throws USException
	 */
	public synchronized boolean hasAcceptedTxn(TxnPeerID txn) throws USException {
		if (unpublishedTids.contains(txn) || publishedTids.contains(txn)) {
			return true;
		}
		return updateStore.hasAcceptedTxn(txn);
	}

	/**
	 * Determine if the viewing peer has rejected a transaction.
	 * 
	 * @param txn	The transaction to check for
	 * @return		<code>true</code> if it has been rejected,
	 * 				<code>false</code> otherwise
	 * @throws USException
	 */
	public synchronized boolean hasRejectedTxn(TxnPeerID txn) throws USException {
		return updateStore.hasRejectedTxn(txn);
	}

	/**
	 * Determine the number of the last reconciliation that <code>peer</code> performed.
	 * 
	 * @return				The reconciliation number of its most recent
	 * 						reconciliation, or 0 if the peer has not performed
	 * 						any reconciliations yet.
	 * @throws SSException 
	 */
	public synchronized int getCurrentRecno() throws SSException {
		return state.getCurrentRecno();
	}
	
	private Map<Subtuple,Set<Integer>> getDirtyRecordFor(int relID) {
		Map<Subtuple,Set<Integer>> entry = dirty.get(relID);
		
		if (entry == null) {
			entry = new HashMap<Subtuple,Set<Integer>>();
			dirty.put(relID, entry);
		}
		return entry;
	}

	/**
	 * Record that a specific tuple's value is dirtied during a peer's
	 * reconciliation
	 * 
	 * @param recno the number of the reconciliation when the values became
	 * dirty
	 * @param t the tuple whose value has become dirty
	 */
	private void markDirty(int recno, Tuple t) {
		if (t == null) {
			return;
		}
		Map<Subtuple,Set<Integer>> dirtyForRelation = getDirtyRecordFor(t.getRelationID());//dirty.get(t.getRelationID());
		Subtuple st = t.getKeySubtuple();
		Set<Integer> recnos = dirtyForRelation.get(st);
		if (recnos == null) {
			recnos = new HashSet<Integer>();
			t.setReadOnly();
			dirtyForRelation.put(st,recnos);
		}
		recnos.add(recno);
	}

	/**
	 * Record that a specific update's values are dirtied during a peer's
	 * reconciliation
	 * 
	 * @param recno the number of the reconciliation when the values became
	 * dirty
	 * @param u the update whose values have become dirty
	 */
	private void markDirty(int recno, Update u) {
		markDirty(recno,u.getOldVal());
		markDirty(recno,u.getNewVal());
	}

	/**
	 * Determine if a tuple's value is dirty during
	 * a specific reconciliation
	 * 
	 * @param recno the number of the peer's reconciliation
	 * @param t the tuple whose values should be checked
	 * @return <code>true</code> if it is dirty, <code>false</code> if it is not
	 */
	private boolean isDirty(int recno, Tuple t) {
		if (t == null)
			return false;
		Map<Subtuple,Set<Integer>> dirtyForRelation = dirty.get(t.getRelationID());
		if (dirtyForRelation == null)
			return false;
		
		Subtuple st = t.getKeySubtuple();
		Set<Integer> recnos = dirtyForRelation.get(st);

		if (recnos == null) {
			return false;
		}
		for (Integer r : recnos) {
			if (recno >= r) {
				return true;
			}
		}

		return false;
	}

	/**
	 * Determine if an Update's old and new values are dirty during
	 * a specific reconciliation
	 * 
	 * @param recno the number of the peer's reconciliation
	 * @param u the Update whose values should be checked
	 * @return <code>true</code> if one or both of them is dirty,
	 * 	<code>false</code> if neither of them is
	 */
	private boolean isDirty(int recno, Update u) {
		return (isDirty(recno, u.getOldVal()) || isDirty(recno, u.getNewVal()));
	}

	/**
	 * Clear deferred transactions, dirty values, and conflicts from a particular
	 * reconciliation.
	 * 
	 * @param recno the number of the peer's reconciliation
	 */
	private void clearStateForReconcile(int recno) {
		// Remove recno from all of the sets of reconciliation numbers,
		// and remove any empty sets of reconciliation numbers from dirty
		// to save space

		for (Map<Subtuple,Set<Integer>> dirtyForRelation : dirty.values()) {
			Iterator<Set<Integer>> recnosSets = dirtyForRelation.values().iterator();
			while (recnosSets.hasNext()) {
				Set<Integer> recnos = recnosSets.next();
				recnos.remove(recno);
				if (recnos.isEmpty()) {
					recnosSets.remove();
				}
			}
		}
		
		dirty.clear();

		deferred.put(recno, new HashMap<TxnPeerID,HashSet<ConflictOptionPair>>());
		conflictTxns.put(recno, new HashMap<ConflictOptionPair,HashSet<TxnPeerID>>());
		conflicts.put(recno, new ArrayList<ArrayList<Update>>());
		conflictsOldValueIndex.put(recno, new HashMap<Tuple,Integer>());
		conflictsNewKeyIndex.put(recno, new HashMap<Subtuple,Integer>());
		conflictsInitialValueIndex.put(recno, new HashMap<ValueTidPair,Integer>());
	}

	/**
	 * Find a (conflict,option) pair for the supplied update, if one exists. If the
	 * conflict exists but not an option for this value, create a new one. If there is no
	 * matching conflict, return (-1,-1).
	 * 
	 * @param recno			The peer's reconciliation number
	 * @param u				The update to test
	 * @return				The a list of (conflict,option) pairs for this update
	 */
	private List<ConflictOptionPair> identifyConflict(int recno, Update u) {

		ArrayList<ConflictOptionPair> retval = new ArrayList<ConflictOptionPair>(3);

		ArrayList<ArrayList<Update>> conflictsForRecno = conflicts.get(recno);


		Integer conflictID = conflictsOldValueIndex.get(recno).get(u.getOldVal());
		if (conflictID != null) {
			retval.add(new ConflictOptionPair(conflictID, -1, ConflictTypeCode.UPDATE));
		}
		if (u.getNewVal() != null) {
			conflictID = conflictsNewKeyIndex.get(recno).get(u.getNewVal().getKeySubtuple());
		}
		if (conflictID != null) {
			retval.add(new ConflictOptionPair(conflictID, -1, ConflictTypeCode.KEY));
		}
		ValueTidPair vtp = new ValueTidPair(u.getInitialVal(), u.getInitialTid());
		conflictID = conflictsInitialValueIndex.get(recno).get(vtp);
		if (conflictID != null) {
			retval.add(new ConflictOptionPair(conflictID, -1, ConflictTypeCode.INITIAL));
		}

		for (ConflictOptionPair conflict : retval) {
			ArrayList<Update> conflictOptions = conflictsForRecno.get(conflict.conflict);
			final int numOptions = conflictOptions.size();
			for (int j = 0; j < numOptions; ++j) {
				Update conflictUpdate = conflictOptions.get(j);
				if (conflictUpdate.equalsOnValues(u) || subset(conflictUpdate.getTids(), u.getTids()) ||
						subset(u.getTids(), conflictUpdate.getTids())) {
					if (conflict.type == ConflictTypeCode.INITIAL && (! (u.getInitialTid() != null &&
							u.getInitialTid().equals(conflictUpdate.getInitialTid()) &&
							u.getInitialVal().equals(conflictUpdate.getInitialVal())))) {
						continue;
					}
					conflict.option = j;
				}
			}

			if (conflict.option == -1) {
				conflictOptions.add(u.duplicate());
				conflict.option = conflictOptions.size() - 1;
			}
		}
		return retval;
	}

	/**
	 * Add a new conflict involving the supplied update.
	 * 
	 * @param recno			The peer's reconciliation number
	 * @param u				The update
	 * @param initial		The type code for the conflict
	 * @return				The id of the conflict
	 */
	private ConflictOptionPair addConflict(int recno, Update u, ConflictTypeCode type) {
		if (type == ConflictTypeCode.INITIAL && (! (u.isInsertion() || u.isNull()))) {
			throw new IllegalArgumentException("Attempt to create initial value conflict with non-insertion update");
		}

		if (type == ConflictTypeCode.KEY && (u.isDeletion())) {
			throw new IllegalArgumentException("Attempt to create key value conflict with deletion update");
		}

		if (type == ConflictTypeCode.UPDATE && (u.isInsertion())) {
			throw new IllegalArgumentException("Attempt to create update conflict with insertion update");
		}

		ArrayList<ArrayList<Update>> conflictsForRecno = conflicts.get(recno);
		ArrayList<Update> newConflict = new ArrayList<Update>();

		newConflict.add(u.duplicate());
		conflictsForRecno.add(newConflict);
		int conflict = conflictsForRecno.size() - 1;

		if (type == ConflictTypeCode.UPDATE) {
			conflictsOldValueIndex.get(recno).put(u.getOldVal().duplicate(), conflict);
		} else if (type == ConflictTypeCode.KEY) {
			conflictsNewKeyIndex.get(recno).put(u.getNewVal().getKeySubtuple(), conflict);
		} else if (type == ConflictTypeCode.INITIAL) {
			conflictsInitialValueIndex.get(recno).put(new ValueTidPair(u.getInitialVal().duplicate(), u.getInitialTid().duplicate()), conflict);
		}

		return new ConflictOptionPair(conflict, 0, type);
	}


	/**
	 * Mark a conflict-causing transaction as deferred. This function does not
	 * add the potential use set to the dirty values.
	 * 
	 * @param recno			The peer's reconciliation number
	 * @param tid			The ID and originating peer of the txn
	 * @param cop			The ID of the conflict that caused the txn to be deferred
	 */
	private void deferTxn(int recno, TxnPeerID tpi, ConflictOptionPair cop) {
		TxnPeerID tpiCopy = tpi.duplicate();
		ConflictOptionPair copCopy = cop.duplicate();
		HashSet<ConflictOptionPair> conflictsForTxn = deferred.get(recno).get(tpi);
		if (conflictsForTxn == null) {
			conflictsForTxn = new HashSet<ConflictOptionPair>();
			deferred.get(recno).put(tpiCopy, conflictsForTxn);
		}
		HashSet<TxnPeerID> txnsForConflict = conflictTxns.get(recno).get(cop);
		if (txnsForConflict == null) {
			txnsForConflict = new HashSet<TxnPeerID>();
			conflictTxns.get(recno).put(copCopy, txnsForConflict);
		}
		txnsForConflict.add(tpiCopy);
		conflictsForTxn.add(copCopy);
	}

	private enum Status { ACCEPT, DEFERCONFLICT, REJECT, UNKNOWN, DEFER, REJECTCONFLICT }
	// ACCEPT = accept transaction
	// DEFERCONFLICT = deferred due to conflict with same value,
	// REJECT = reject due to conflict with state or unsatisfiable antecedents
	// UNKNOWN = not decided yet,
	// DEFER = deferred due to dirty value in potential use set
	// REJECTCONFLICT = rejected due to conflict with higher priority txn

	// Note that one a transaction is marked REJECT or DEFER, it can never be accepted; those marked
	// DEFERCONFLICT or REJECTCONFLICT might

	private static boolean subset(Set<TxnPeerID> putativeSubset, Set<TxnPeerID> putativeSuperset) {
		for (TxnPeerID tpi : putativeSubset) {
			if (! putativeSuperset.contains(tpi)) {
				return false;
			}
		}

		return true;
	}

	private static class ConflictDb {
		static class Record {
			TxnPeerID txn1;
			TxnPeerID txn2;
			Update u1;
			Update u2;
			Record(TxnPeerID txn1, TxnPeerID txn2, Update u1, Update u2) {
				this.txn1 = txn1;
				this.txn2 = txn2;
				this.u1 = u1;
				this.u2 = u2;
			}
		}
		private HashMap<TxnPeerID,List<Record>> index;
		ConflictDb() {
			index = new HashMap<TxnPeerID,List<Record>>();
		}
		void addConflict(TxnPeerID txn1, TxnPeerID txn2, Update u1, Update u2) {
			Record r = new Record(txn1, txn2, u1, u2);
			if (index.get(txn1) == null) {
				index.put(txn1, new ArrayList<Record>());
			}
			if (index.get(txn2) == null) {
				index.put(txn2, new ArrayList<Record>());
			}
			index.get(txn1).add(r);
			index.get(txn2).add(r);
		}
		List<Record> getConflicts(TxnPeerID txn) {
			List<Record> retval = index.get(txn);
			return (retval == null) ? new ArrayList<Record>(0) : retval;
		}
		List<TxnPeerID> getConflictingTxns(TxnPeerID txn) {
			ArrayList<TxnPeerID> retval = new ArrayList<TxnPeerID>();

			for (Record r : getConflicts(txn)) {
				if (r.txn1.equals(txn)) {
					retval.add(r.txn2);
				} else {
					retval.add(r.txn1);
				}
			}

			return retval;
		}
		void reset() {
			index.clear();
		}
	}

	private void findConflicts(Map<TxnPeerID,List<Update>> txns, ConflictDb conflictDb) throws DbException {
		final int numTxns = txns.size();
		// Use indices so we only check each pair of txns once
		ArrayList<TxnPeerID> txnIndex = new ArrayList<TxnPeerID>(numTxns);
		txnIndex.addAll(txns.keySet());
		// Record conflicts
		for (int i = 0; i < numTxns; ++i) {
			TxnPeerID txn_i = txnIndex.get(i);
			for (Update u_i : txns.get(txn_i)) {
				for (int j = 0; j < i; ++j) {
					TxnPeerID txn_j = txnIndex.get(j);
					for (Update u_j : txns.get(txn_j)) {
						if (u_i.conflicts(u_j) && (! subset(u_i.getTids(), u_j.getTids())) && (! subset(u_j.getTids(), u_i.getTids()))) {
							conflictDb.addConflict(txn_i,txn_j,u_i,u_j);
						}
					}
				}
			}
		}
	}

	private List<Update> findNeededUpdates(List<Update> transaction,
			Map<TupleTpi,TupleTpi> initialValueUpdates, Map<TxnPeerID,Set<TxnPeerID>> isDescendantOf) throws DbException {
		List<Update> neededUpdates = new ArrayList<Update>(transaction.size());

		for (Update u : transaction) {
			TxnPeerID tpi = u.getLastTid();
			TupleTpi initialTT = new TupleTpi(u.getInitialVal(), (u.isInsertion() || u.isNull()) ? u.getInitialTid() : null);
			TupleTpi tt = initialValueUpdates.get(initialTT);
			if (tt == null) {
				neededUpdates.add(u);
			} else {
				TxnPeerID alreadyTpi = tt.tpi;
				boolean isOlder = false;
				boolean isNewer = false;
				try {
					if (isDescendantOf.get(tpi).contains(alreadyTpi)) {
						isNewer = true;
					}
				} catch (NullPointerException npe) {
				}
				try {
					if (isDescendantOf.get(alreadyTpi).contains(tpi)) {
						isOlder = true;
					}
				} catch (NullPointerException npe) {
				}
				if (tt.tpi.equals(u.getLastTid())) {
					// We can safely ignore the update
					isOlder = true;
				}

				if (! (isOlder ^ isNewer)) {
					throw new DbException("Couldn't establish precedence relationship between " + tpi + " and " + alreadyTpi);
				}

				// If isOlder, then we don't have to do anything since the
				// newer update has already been applied
				if (isNewer) {
					Update uu = new Update(tt.t, u.getNewVal());
					uu.addTid(u.getLastTid());
					uu.setInitialVal(u.getInitialVal());
					uu.setInitialTid(u.getInitialTid());
					neededUpdates.add(uu);
				}
			}
		}

		return neededUpdates;
	}

	private static class TupleTpi {
		Tuple t;
		TxnPeerID tpi;
		TupleTpi(Tuple t, TxnPeerID tpi) {
			this.t = t;
			this.tpi = tpi;
		}
		public boolean equals(Object o) {
			if (! (o instanceof TupleTpi)) {
				return false;
			}
			TupleTpi tt = (TupleTpi) o;

			if (! t.equals(tt.t)) {
				return false;
			}

			if (tpi == null) {
				return (tt.tpi == null);
			} else {
				return (tpi.equals(tt.tpi));
			}
		}
		public int hashCode() {
			int retval = 37 * t.hashCode();
			if (tpi != null) {
				retval += tpi.hashCode();
			}
			return retval;
		}
	}

	/**
	 * Determine and apply the updates from other peers a reconciling peer
	 * needs to apply.
	 * 
	 * @param recno				the peer's reconciliation number
	 * @param trustedTxns		the transactions to consider
	 * @param decisions			where to put the decisions that this method makes
	 * @throws DbException
	 * @throws InconsistentUpdates
	 */
	private long applyReconciliationUpdates(int recno, Map<Integer,? extends Collection<TxnChain>> trustedTxns, Collection<Decision> decisions)
	throws DbException, InconsistentUpdates {
		long startTime = 0;
		if (benchmark != null) {
			startTime = System.nanoTime();
		}
		clearStateForReconcile(recno);

		// The transaction priorities, sorted in decreasing order
		List<Integer> prios = new ArrayList<Integer>(trustedTxns.size());
		prios.addAll(trustedTxns.keySet());
		Collections.sort(prios, Collections.reverseOrder());

		// Compute a mapping from transaction IDs to priority
		// and from transaction IDs to chain
		Map<TxnPeerID,Integer> txnPrios = new HashMap<TxnPeerID,Integer>();
		Map<TxnPeerID,TxnChain> txnChain = new HashMap<TxnPeerID,TxnChain>();
		for (Map.Entry<Integer, ? extends Collection<TxnChain>> prio : trustedTxns.entrySet()) {
			for (TxnChain tc : prio.getValue()) {
				txnPrios.put(tc.getHead(), prio.getKey());
				txnChain.put(tc.getHead(), tc);
			}
		}

		ArrayList<Update> delta = deltas.get(recno);
		if (delta == null) {
			delta = new ArrayList<Update>();
		}

		// What we decide to do with each transaction
		Map<TxnPeerID,Status> status = new HashMap<TxnPeerID,Status>();

		// Flattened versions of non-rejected transactions
		Map<TxnPeerID,List<Update>> txnContents = new HashMap<TxnPeerID,List<Update>>();

		txnIterate:
			for (TxnPeerID tpi : txnChain.keySet()) {
				status.put(tpi,Status.UNKNOWN);
				TxnChain tc = txnChain.get(tpi);
				List<Update> currTxnContents = tc.isFlattened() ? tc.getContents() : flatten(tc.getContents());

				for (Update u : currTxnContents) {
					// Dirty value touched by update
					if (isDirty(recno,u)) {
						status.put(tpi,Status.DEFER);
						txnContents.put(tpi, currTxnContents);
						continue txnIterate;
					}
				}

				// TODO: Should be able to check this before dirty values,
				// and reject unless the conflict is on a dirty value
				if (conflictsWithState(recno, currTxnContents)) {
					status.put(tpi,Status.REJECT);
					continue;
				}

				for (Update u : currTxnContents) {
					for (Update d: delta) { 
						// Check for conflicts with already applied updates from this
						// reconciliation
						if ((! u.isPrevTid(d.getLastTid())) && u.conflicts(d)) {
							status.put(tpi,Status.REJECT);
							continue txnIterate;
						}
					}
				}

				txnContents.put(tpi, currTxnContents);
			}

		// Records of fully trusted transactions that conflict with other
		// fully trusted transactions, and the updates on which they conflict
		ConflictDb conflictDb = new ConflictDb();

		findConflicts(txnContents, conflictDb);

		// Determine (tentatively) what to accept, what to defer, and what to reject
		for (int prio : prios) {
			// Find same priority conflicting transactions, and reject and defer those that must
			// be because of higher priority accepted or deferred transactions
			Map<TxnPeerID,Set<TxnPeerID>> samePrioConflicts = new HashMap<TxnPeerID,Set<TxnPeerID>>();
			for (TxnChain tc : trustedTxns.get(prio)) {
				TxnPeerID tpi = tc.getHead();
				if (status.get(tpi) == Status.UNKNOWN) {
					status.put(tpi, Status.ACCEPT);
				} else {
					continue;
				}
				samePrioConflicts.put(tpi, new HashSet<TxnPeerID>());
				List<TxnPeerID> conflicts = conflictDb.getConflictingTxns(tpi);
				boolean foundHigherAcceptedTxn = false;
				boolean foundHigherDeferredTxn = false;
				for (TxnPeerID txn : conflicts) {
					int txnPrio = txnPrios.get(txn);
					Status txnStatus = status.get(txn);
					if (txnPrio > prio) {
						if (txnStatus == Status.ACCEPT) {
							foundHigherAcceptedTxn = true;
						} else if (txnStatus == Status.DEFERCONFLICT) {
							foundHigherDeferredTxn = true;
						}
					} else if (txnPrio == prio) {
						samePrioConflicts.get(tpi).add(txn);
					}
				}
				if (foundHigherAcceptedTxn) {
					status.put(tpi,Status.REJECTCONFLICT);
				} else if (foundHigherDeferredTxn) {
					status.put(tpi,Status.DEFERCONFLICT);
				}
			}
			// If a transaction isn't going to be rejected because of a
			// conflict with a higher priority transaction, and it conflicts
			// with another transaction that also isn't going to be rejected,
			// defer it and all of the non-rejected transactions it conflicts with
			for (TxnChain tc : trustedTxns.get(prio)) {
				TxnPeerID tpi = tc.getHead();
				if (status.get(tpi) != Status.ACCEPT) {
					continue;
				}
				boolean foundConflict = false;
				for (TxnPeerID txn : samePrioConflicts.get(tpi)) {
					Status s = status.get(txn);
					if (s == Status.DEFER || s == Status.DEFERCONFLICT || s == Status.ACCEPT) {
						foundConflict = true;
					}
				}
				if (! foundConflict) {
					continue;
				}
				status.put(tpi, Status.DEFERCONFLICT);
			}
		}

		// Record of what the current values for the initial values are
		// In the key, a null initial TPI means that the value was present in the database
		// at the start of reconciliation
		HashMap<TupleTpi,TupleTpi> initialValueUpdates = new HashMap<TupleTpi,TupleTpi>();

		// Computed relationships between transaction chains
		// TODO: This gives relationships from trusted transactions to preceding ones, is that enough?
		HashMap<TxnPeerID,Set<TxnPeerID>> isDescendantOf = new HashMap<TxnPeerID,Set<TxnPeerID>>();
		for (Map.Entry<TxnPeerID,TxnChain> me : txnChain.entrySet()) {
			Set<TxnPeerID> antecedents = new HashSet<TxnPeerID>();
			isDescendantOf.put(me.getKey(), antecedents);
			for (TxnPeerID tpi : me.getValue().getComponents()) {
				if (! tpi.equals(me.getKey())) {
					antecedents.add(tpi);
				}
			}
		}


		Set<TxnPeerID> alreadyAcceptedTxns = new HashSet<TxnPeerID>();

		// Accept and reject transactions based on our decisions.
		// Deferred ones get dealt with later.
		for (TxnPeerID tpi : txnChain.keySet()) {
			Status txnStatus = status.get(tpi);
			if (txnStatus == Status.ACCEPT && (! alreadyAcceptedTxns.contains(tpi))) {
				// Chain is not subsumed by an already accepted one
				List<Update> neededUpdates = findNeededUpdates(txnContents.get(tpi), initialValueUpdates, isDescendantOf);

				List<Update> updatesToApply = new ArrayList<Update>(neededUpdates.size());

				for (Update u : neededUpdates) {
					// This works for null (placeholder) updates, which is important
					TupleTpi tt = new TupleTpi(u.getInitialVal(), (u.isInsertion() || u.isNull()) ? u.getInitialTid() : null);
					initialValueUpdates.put(tt, new TupleTpi(u.getNewVal(), u.getLastTid()));
					if (! u.isNull()) {
						updatesToApply.add(u);
					}
				}

				state.applyTransaction(recno, updatesToApply);
				delta.addAll(updatesToApply);

				// Record the component transactions as accepted
				for (TxnPeerID tpid : txnChain.get(tpi).getComponents()) {
					if (! alreadyAcceptedTxns.contains(tpid)) {
						alreadyAcceptedTxns.add(tpid);
						decisions.add(new Decision(tpid, recno, true));
					}
				}
			} else if (txnStatus == Status.REJECT || txnStatus == Status.REJECTCONFLICT) {
				decisions.add(new Decision(tpi, recno, false));
			} else if (txnStatus == Status.DEFERCONFLICT || txnStatus == Status.DEFER) {
			} else if (txnStatus == Status.UNKNOWN) {
				throw new DbException("Status of txn " + tpi + " is UNKNOWN at end of reconciliation");
			}
		}

		// Merge the new updates into the delta
		delta = flatten(delta);
		deltas.put(recno,delta);


		Map<TxnPeerID,List<Update>> deferredTxns = new HashMap<TxnPeerID,List<Update>>();

		// Determine the needed updates for the deferred transactions, and add
		// those updates to the dirty set
		for (TxnPeerID tpi : txnChain.keySet()) {
			if (status.get(tpi) != Status.DEFER && status.get(tpi) != Status.DEFERCONFLICT) {
				continue;
			}
			// Determine the updates we actually need to apply

			List<Update> neededUpdates = findNeededUpdates(txnContents.get(tpi), initialValueUpdates, isDescendantOf);
			for (Update u : neededUpdates) {
				markDirty(recno,u);
			}
			deferredTxns.put(tpi, neededUpdates);
		}


		conflictDb.reset();

		findConflicts(deferredTxns, conflictDb);

		for (TxnPeerID tpi : deferredTxns.keySet()) {
			for (Update u : deferredTxns.get(tpi)) {
				// Find all of the already created conflicts that this update
				// participates in. There can be at most one of each kind
				List<ConflictOptionPair> cops = identifyConflict(recno,u);
				ConflictOptionPair initialConflict = null;
				ConflictOptionPair updateConflict = null;
				ConflictOptionPair keyConflict = null;
				for (ConflictOptionPair cop : cops) {
					if (cop.type == ConflictTypeCode.INITIAL) {
						initialConflict = cop;
					} else if (cop.type == ConflictTypeCode.KEY) {
						keyConflict = cop;
					} else if (cop.type == ConflictTypeCode.UPDATE) {
						updateConflict = cop;
					}
				}

				// Add new conflicts for the kinds of conflicts that this update
				// has with later transactions but not with earlier ones
				List<ConflictDb.Record> conflicts = conflictDb.getConflicts(tpi);
				for (ConflictDb.Record r : conflicts) {
					if (initialConflict != null && updateConflict != null && keyConflict != null) {
						break;
					}

					Update thisUpdate;
					Update otherUpdate;
					if (r.txn1.equals(tpi)) {
						thisUpdate = r.u1;
						otherUpdate = r.u2;
					} else {
						thisUpdate = r.u2;
						otherUpdate = r.u1;
					}
					if (! u.equals(thisUpdate)) {
						continue;
					}
					ConflictTypeCode ctc = ConflictType.getConflictType(thisUpdate,otherUpdate);
					if (initialConflict == null && ctc == ConflictTypeCode.INITIAL) {
						initialConflict = addConflict(recno,thisUpdate,ctc);
					} else if (updateConflict == null && ctc == ConflictTypeCode.UPDATE) {
						updateConflict = addConflict(recno,thisUpdate,ctc);
					} else if (keyConflict == null && ctc == ConflictTypeCode.KEY) {
						keyConflict = addConflict(recno,thisUpdate,ctc);
					}
				}

				// Defer because of all possible conflicts
				if (initialConflict != null) {
					deferTxn(recno, tpi, initialConflict);
				}
				if (updateConflict != null) {
					deferTxn(recno, tpi, updateConflict);
				}
				if (keyConflict != null) {
					deferTxn(recno, tpi, keyConflict);
				}
			}
		}

		long endTime = 0;
		if (benchmark != null) {
			endTime = System.nanoTime();
		}

		return endTime - startTime;
	}
	
	/**
	 * Added ZI for integration reasons
	 * @deprecated
	 * 
	 * @param recno
	 * @param txn
	 * @param updatesToApply
	 * @throws SSException 
	 * @throws USException 
	 */
	public void applySingleTrans(int recno, TxnPeerID txn, List<Update> updatesToApply) throws SSException, USException {
		state.applyTransaction(recno, updatesToApply);
		
		Collection<Decision> decisions = new ArrayList<Decision>();

		// Record the component transactions as accepted
		decisions.add(new Decision(txn, recno, true));
		updateStore.recordTxnDecisions(decisions);
	}
	
	public synchronized int getRecNo() throws DbException {
		if (updateStore == null) {
			throw new IllegalStateException("Attempt to reconcile without first calling setDoneInitializing");
		}

		int usCurrentRecno = updateStore.getCurrentRecno();
		int ssCurrentRecno = state.getCurrentRecno();
		if (usCurrentRecno != ssCurrentRecno) {
			throw new DbException("Update store and state store are not consistent. Update store current recno: " + usCurrentRecno + ", state store current recno: " + ssCurrentRecno + "." );
		}
		updateStore.recordReconcile(false);
		
		return state.getCurrentRecno();
	}
	
	public synchronized void setRecDone() throws DbException {
		state.advanceRecno();
		
		if (updateStore.getCurrentRecno() != state.getCurrentRecno()) {
			throw new DbException("Update store and state store are not consistent");
		}
	}

	/**
	 * Reconcile this peer with the shared database
	 * 
	 * @return			The reconciliation number that the peer just performed
	 * @throws DbException
	 */
	public synchronized int reconcile() throws DbException {
		/*
		if (updateStore == null) {
			throw new IllegalStateException("Attempt to reconcile without first calling setDoneInitializing");
		}

		if (updateStore.getCurrentRecno() != state.getCurrentRecno()) {
			throw new DbException("Update store and state store are not consistent");
		}
		updateStore.recordReconcile(false);
		final int recno = state.getCurrentRecno();*/
		final int recno = getRecNo();


		ArrayList<Decision> decisions = new ArrayList<Decision>();
		if (! tc.empty()) {
			Map<Integer,List<TxnChain>> trustedTxns = new HashMap<Integer,List<TxnChain>>();
			Set<TxnPeerID> mustReject = new HashSet<TxnPeerID>();

			updateStore.getReconciliationData(recno, new HashSet<TxnPeerID>(publishedTids), trustedTxns, mustReject);

			// Record that we reject transactions with rejected antecedents
			for (TxnPeerID tpi : mustReject) {
				decisions.add(new Decision(tpi, recno, false));
			}

			long reconTime = applyReconciliationUpdates(recno, trustedTxns, decisions);
			if (benchmark != null) {
				benchmark.reconcile += reconTime;
			}

		}
		for (TxnPeerID tpi : publishedTids) {
			decisions.add(new Decision(tpi, recno, true));
		}
		publishedTids.clear();
		updateStore.recordTxnDecisions(decisions);

		/*
		state.advanceRecno();

		if (updateStore.getCurrentRecno() != state.getCurrentRecno()) {
			throw new DbException("Update store and state store are not consistent");
		}*/
		setRecDone();

		return recno;
	}

	/* (non-Javadoc)
	 * @see edu.upenn.cis.orchestra.reconciliation.Db#publish()
	 */
	public synchronized void publish() throws USException, SSException {
		if (updateStore == null) {
			throw new IllegalStateException("Attempt to publish without first calling setDoneInitializing");
		}


		updateStore.publish(unpublishedTransactions);
		unpublishedTransactions.clear();
		
		publishedTids.addAll(unpublishedTids);
		unpublishedTids.clear();


	}

	/**
	 * Determine if applying the supplied transaction at the specified reconciliation would
	 * cause a key violation.
	 * 
	 * Note that this implementation assumes that each transaction has already been
	 * flattened, since it doesn't check for conflicts caused by interactions of the
	 * various updates in each transaction. It also doesn't check for conflicts between
	 * the different transactions, since applyReconciliationUpdates does that.
	 * 
	 * @param recno
	 * @param txn
	 * @return <code>true</code> if one of the updates conflicts with the database
	 * state, <code>false</code> if none of them does
	 * @throws SSException
	 */
	private boolean conflictsWithState(int recno, List<Update> txn) throws SSException {

		for (Update u : txn) {
			if (u.isNull()) {
				continue;
			}
			if (u.isInsertion() || u.isUpdate()) {
				Tuple val = state.getTupleWithKey(recno, u.getNewVal());

				if (val != null && val.equals(u.getNewVal())) {
					// The value we're inserting or modifying is already in
					// the database
					continue;
				} else if (u.isUpdate() && val != null && u.getOldVal().equals(val)) {
					// We're modifying the value already in the database and not
					// changing the key
					continue;
				}

				if (val != null) {
					// We have a key violation caused by inserting the new value
					return true;
				}

				// Here, if val != null, val fully equals u.newVal.
				if (u.isUpdate() && (val == null)) {
					// Update has not already been applied, but cannot be
					Tuple oldVal = state.getTupleWithKey(recno, u.getOldVal());
					if (oldVal == null || (! oldVal.equals(u.getOldVal()))) {
						return true;
					}
				}
			}
		}
		return false;
	}

	/**
	 * Get the current state of the viewing peer. The returned map allows
	 * lookup by key because of the way equality and hashing is defined on
	 * tuples.
	 * 
	 * @return			A map containing, for each relation name, the set
	 * 					of keys of the current state of that relation.
	 * @throws SSException
	 */
	public synchronized Map<String,TupleSet> getState() throws SSException {
		HashMap<String,TupleSet> retval = new HashMap<String,TupleSet>();

		for (String s : schema.getRelationNames()) {
			retval.put(s, state.getState(s));
		}

		return retval;
	}


	/**
	 * Get all of the conflicts and the transactions taking part in them currently present
	 * for the viewing peer during its reconciliations.
	 * 
	 * @return				A mapping from recno to a list of conflicts to a list of sets
	 * 						containing the transactions participating in that option. For
	 * 						example, getConflicts().get(0).get(1).get(3) gets the set of
	 * 						transactions in option 3 of conflict 1 from reconciliation 0.
	 */
	public synchronized Map<Integer,List<List<Set<TxnPeerID>>>> getConflicts() {
		Map<Integer,List<List<Set<TxnPeerID>>>> retval = new HashMap<Integer,List<List<Set<TxnPeerID>>>>();

		for (int recno : conflictTxns.keySet()) {
			if (conflictTxns.get(recno).isEmpty()) {
				continue;
			}
			retval.put(recno, new ArrayList<List<Set<TxnPeerID>>>());
			List<List<Set<TxnPeerID>>> retvalForRecno = retval.get(recno);
			HashMap<ConflictOptionPair,HashSet<TxnPeerID>> conflictsForRecno = conflictTxns.get(recno);
			for (ConflictOptionPair cop : conflictsForRecno.keySet()) {
				while (retvalForRecno.size() <= cop.conflict) {
					retvalForRecno.add(new ArrayList<Set<TxnPeerID>>());
				}
				List<Set<TxnPeerID>> retvalForRecnoAndConflict = retvalForRecno.get(cop.conflict);
				while (retvalForRecnoAndConflict.size() <= cop.option) {
					retvalForRecnoAndConflict.add(new HashSet<TxnPeerID>());
				}
				Set<TxnPeerID> txnSet = retvalForRecnoAndConflict.get(cop.option);
				for (TxnPeerID tpi : conflictsForRecno.get(cop)) {
					txnSet.add(tpi.duplicate());
				}
			}
		}

		return retval;
	}

	/**
	 * Resolve the specified conflicts by accepting the specified options.
	 * 
	 * The <code>conflictResolution</code> argument is a mapping from reconciliation
	 * numbers to mappings from conflict numbers to option numbers to accept for that
	 * conflict. If the option to accept is null, then all options for that conflict are
	 * rejected.
	 * 
	 * @param conflictResolutions	The mapping from reconciliation numbers to mappings from
	 * 								conflict numbers to option numbers to reject, as described
	 * 								above
	 * @throws DbException
	 */
	public synchronized void resolveConflicts(Map<Integer,Map<Integer,Integer>> conflictResolutions) throws DbException {

		// Determine where we need to start re-running applyReconciliationUpdates
		int maxRecno = getCurrentRecno() - 1;
		int minRecno = maxRecno;

		List<Decision> tds = new ArrayList<Decision>();

		for (int recno : conflictResolutions.keySet()) {
			if (recno < 0 || recno > maxRecno) {
				throw new IllegalArgumentException("Reconciliation number must be >= 0 and have already happened");
			} else if (recno > maxRecno) {
				throw new UnknownRecno(recno, maxRecno);
			} else if (recno < minRecno) {
				minRecno = recno;
			}

			Map<Integer,Integer> conflictResolutionsForRecno = conflictResolutions.get(recno);
			HashMap<ConflictOptionPair,HashSet<TxnPeerID>> conflictTxnsForRecno = conflictTxns.get(recno);
			for (ConflictOptionPair cop : conflictTxnsForRecno.keySet()) {
				if (conflictResolutionsForRecno.containsKey(cop.conflict)) {
					Integer option = conflictResolutionsForRecno.get(cop.conflict);
					if (option == null || ((int) option) != cop.option) {
						for (TxnPeerID tpi : conflictTxnsForRecno.get(cop)) {
							tds.add(new Decision(tpi, recno, false));
						}
					}
				}
			}
		}
		if (updateStore.getCurrentRecno() != state.getCurrentRecno()) {
			throw new DbException("Update store and state store are not consistent");
		}
		updateStore.recordReconcile(true);
		updateStore.recordTxnDecisions(tds);
		state.advanceRecno();

		for (int recno = minRecno; recno <= maxRecno; ++recno) {
			tds.clear();
			Map<Integer,List<TxnChain>> trustedTxns = new HashMap<Integer,List<TxnChain>>();
			Set<TxnPeerID> mustReject = new HashSet<TxnPeerID>();
			updateStore.getReconciliationData(recno, publishedTids, trustedTxns, mustReject);			
			for (TxnPeerID tpi : mustReject) {
				tds.add(new Decision(tpi, recno, false));
			}
			long reconTime = applyReconciliationUpdates(recno, trustedTxns, tds);
			updateStore.recordReconcile(true);
			updateStore.recordTxnDecisions(tds);
			state.advanceRecno();
			if (benchmark != null) {
				benchmark.resolveConflicts += reconTime;
			}
		}
		if (updateStore.getCurrentRecno() != state.getCurrentRecno()) {
			throw new DbException("Update store and state store are not consistent");
		}
	}

	/**
	 * Get all the values that are dirty at this point in time.
	 * 
	 * @return				The set of tuples that are dirty
	 * @throws SSException
	 */
	public synchronized TupleSet getDirtyValues() throws SSException {
		final int recno = getCurrentRecno();
		Map<String,TupleSet> state = getState();
		
		TupleSet retval = new TupleSet();
		
		for (TupleSet valuesForRelation : state.values()) {
			for (Tuple st : valuesForRelation) {
				if (isDirty(recno, st)) {
					retval.add(st);
				}
			}
		}
		
		return retval;
	}

	public synchronized Tuple getValueForKey(int recno, Tuple key) throws StateStore.SSException {
		return state.getTupleWithKey(recno, key);
	}

	private Benchmark benchmark = null;

	public void setBenchmark(Benchmark b) throws USException {
		benchmark = b;
		if (updateStore != null) {
			updateStore.setBenchmark(b);
		}
	}

	public Benchmark getBenchmark() {
		return benchmark;
	}



	@Override
	public synchronized ResultIterator<Decision> getDecisions() throws USException {
		return updateStore.getDecisions();
	}

	@Override
	public synchronized List<Decision> getDecisions(int recno) throws USException {
		return updateStore.getDecisions(recno);
	}

	@Override
	public synchronized ResultIterator<ReconciliationEpoch> getReconciliations() throws USException {
		return updateStore.getReconciliations();
	}


	@Override
	public synchronized ResultIterator<Tuple> getRelationContents(String relation) throws StateStore.SSException {
		return state.getStateIterator(relation);
	}


	@Override
	public synchronized ResultIterator<Update> getPublishedUpdatesForRelation(String relname) throws USException {
		return updateStore.getPublishedUpdatesForRelation(relname);
	}


	@Override
	public synchronized List<Update> getTransaction(TxnPeerID txn) throws USException {
		return updateStore.getTransaction(txn);
	}

	@Override
	public synchronized ResultIterator<TxnPeerID> getTransactionsForReconciliation(int recno) throws USException {
		return updateStore.getTransactionsForReconciliation(recno);
	}


	@Override
	public synchronized void reset() throws DbException {
		state.reset();
		updateStore.reset();
		replayPreviousReconciliations();
	}
	
	private void replayPreviousReconciliations() throws DbException {
		int lastReconciliation = updateStore.getCurrentRecno() - 1;
		for (int recno = StateStore.FIRST_RECNO; recno <= lastReconciliation; ++recno) {
			final Map<TxnPeerID,List<Update>> acceptedTxns = updateStore.getTransactionsAcceptedAtRecno(recno);
			
			
			Map<Integer,List<TxnChain>> trusted = new HashMap<Integer,List<TxnChain>>();
			Set<TxnPeerID> mustReject = new HashSet<TxnPeerID>();
			Set<TxnPeerID> empty = Collections.emptySet();
			updateStore.getReconciliationData(recno, empty, trusted, mustReject);
			
			TransactionSource ts = new TransactionSource() {
				public List<Update> getTxn(TxnPeerID tpi) {
					return acceptedTxns.get(tpi);
				}
			};
			
			// add the accepted transactions to the starts of the transaction chains etc.
			for (List<TxnChain> tcs : trusted.values()) {
				for (TxnChain tc : tcs) {
					tc.replaceTailWithAvailableTxns(ts);
				}
			}

			
			TransactionDecisions td = new TransactionDecisions() {
				public boolean hasAcceptedTxn(TxnPeerID tpi) {
					// Only add to transaction chain transactions
					// that were accepted during this reconciliation
					if (acceptedTxns.containsKey(tpi)) {
						return false;
					} else {
						return true;
					}
				}
				

				public boolean hasRejectedTxn(TxnPeerID tpi) {
					return false;
				}
			};
			
			for (TxnPeerID tpi : acceptedTxns.keySet()) {
				int prio;
				try {
					prio = tc.getTxnPriority(acceptedTxns.get(tpi));
				} catch (CompareMismatch e) {
					throw new DbException(e);
				} 
				if (prio <= 0) {
					continue;
				}
				List<TxnChain> trustedForPrio = trusted.get(prio);
				if (trustedForPrio == null) {
					trustedForPrio = new ArrayList<TxnChain>();
					trusted.put(prio, trustedForPrio);
				}
				// Add transaction chains for all trusted transactions
				// to the list of transactions to consider
				trustedForPrio.add(new TxnChain(tpi, ts, td));
			}

			Set<Decision> decisions = new HashSet<Decision>();
			this.applyReconciliationUpdates(recno, trusted, decisions);
			List<Decision> recordedDecisions = updateStore.getDecisions(recno);
			Set<Decision> recordedDecisionsSet = new HashSet<Decision>();
			for (Decision d : recordedDecisions) {
				if (d.accepted) {
					recordedDecisionsSet.add(d);
				}
			}
			if (! decisions.equals(recordedDecisionsSet)) {
				throw new DbException("Replaying previous reconciliation gave different result: expected decisions " + recordedDecisionsSet + " but got " + decisions);
			}
			state.advanceRecno();
		}
	}
}


