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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import edu.upenn.cis.orchestra.reconciliation.StateStore.SSException;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore.USException;


/**
 * Abstract class to represent a connection to the Orchestra system
 * @author Nick Taylor
 */
public abstract class Db {
	protected final Schema schema;
	protected final AbstractPeerID id;
	
	// Trust conditions object
	protected final TrustConditions tc;
		
	/**
	 * Class to represent an attempt to represent use an invalid peer ID.
	 * 
	 * @author netaylor
	 */
	public static class IllegalPeer extends DbException {
		private static final long serialVersionUID = 1L;

		public IllegalPeer(AbstractPeerID whichPeer, String why) {
			super("Attempt to refer to peer " + whichPeer + ": " + why);
		}
	}
	
	/**
	 * Class to represent an attempt to refer to a reconciliation number
	 * larger than the last one currently recorded for the specified peer
	 * 
	 * @author Nick Taylor
	 *
	 */
	public static class UnknownRecno extends DbException {
		private static final long serialVersionUID = 1L;
		
		/**
		 * @param recno		The number of the requested reconciliation
		 */
		public UnknownRecno(int recno, int lastRecno) {
			super("Attempt to refer to reconciliation " + recno +
					", when last reconciliation for that peer is " + lastRecno);
		}
	}
	
	public static class InconsistentUpdates extends DbException {
		private static final long serialVersionUID = 1L;
		
		public InconsistentUpdates(Update u, Tuple currVal) {
			super("Attempt to apply update " + u + "to value " + currVal);
		}
	}
	
	/**
	 * Gets the priority of a specific update
	 * 
	 * @param u		The update in question
	 * @return		The priority of the update, or 0 if it is not trusted
	 * @throws CompareMismatch
	 */
	public int getUpdatePriority(Update u) throws CompareMismatch {
		return tc.getUpdatePriority(u);
	}

	public Db(TrustConditions tc, Schema schema) {
		if (! schema.isFinished()) {
			throw new IllegalArgumentException("Schema " + schema.getSchemaId() + " must be finished before creating Db object");
		}
		this.schema = schema;
		this.tc = tc.duplicate();
		id = tc.getOwner();
	}
	
	/**
	 * Gives a string representation of the current table of trust conditions.
	 * 
	 * @return				The string value of the table.
	 */
	public String getTrustConditions() throws DbException {
		return tc.toString(schema);
	}

	/**
	 * Inserts a transaction of updates from the viewing peer in the shared database. The ID
	 * of the transaction is automatically computed. The state of the viewing peer's
	 * database is updated as well.
	 *
	 * @param updates The sequence of updates to add to the database.
	 * @return The ID that the transaction was given
	 * @throws DbException
	 */
	abstract public TxnPeerID addTransaction(List<Update> updates) throws DbException;

	/**
	 * Get all of the conflicts and the transactions taking part in them currently present
	 * for the viewing peer during its reconciliations.
	 * 
	 * @return				A mapping from recno to a list of conflicts to a list of sets
	 * 						containing the transactions participating in that option. For
	 * 						example, getConflicts().get(0).get(1).get(3) gets the set of
	 * 						transactions in option 3 of conflict 1 from reconciliation 0.
	 * @throws DbException
	 */
	abstract public Map<Integer,List<List<Set<TxnPeerID>>>> getConflicts() throws DbException;
	
	/**
	 * Determine the number of the last reconciliation that the viewing peer performed.
	 * 
	 * @return				The reconciliation number of its most recent
	 * 						reconciliation, or 0 if the peer has not performed
	 * 						any reconciliations yet. 
	 * @throws DbException
	 */
	abstract public int getCurrentRecno() throws DbException;

	/**
	 * Get the current state of the viewing peer. The returned map allows
	 * lookup by key because of the way equality and hashing is defined on
	 * tuples.
	 * 
	 * @return			A map containing, for each relation name, the set
	 * 					of keys of the current state of that relation.
	 * @throws SSException
	 */
	abstract public Map<String,TupleSet> getState() throws SSException;
	
	/**
	 * Get the value associated with a particular key after the specified
	 * reconciliation of the viewing peer.
	 * 
	 * @param recno		The reconciliation after which to retrieve the value
	 * @param key		They key of the value to be retrieved
	 * @return			The tuple with that key, or <code>null</code> if no
	 * 					such tuple exists.
	 * @throws SSException
	 */
	abstract public Tuple getValueForKey(int recno, Tuple key) throws SSException;
	
	/**
	 * Get an iterator over a peer's table for a particular reconciliation
	 * @param relation	The name of the table to retrieve
	 * 
	 * @return			The iterator, which must be closed by the caller
	 * @throws SSException
	 */
	abstract public ResultIterator<Tuple> getRelationContents(String relation) throws SSException;

	/**
	 * Get an iterator over all of a peer's decisions. They are
	 * given in increasing order by reconciliation number, with
	 * acceptances preceeding rejections.
	 * 
	 * @return		The iterator, which must be closed by the caller
	 * @throws USException
	 */
	abstract public ResultIterator<Decision> getDecisions() throws USException;
	
	/**
	 * Get a peer's decisions for particular reconciliation. Acceptances
	 * preceed rejections.
	 * 
	 * @param recno			The reconciliation of interest
	 * @return				The decisions from that reconciliation
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
	 * @param relname		The relation for which to retrieve the updates
	 * @return				The iterator, which must be closed by the caller
	 * @throws USException
	 */
	abstract public ResultIterator<Update> getPublishedUpdatesForRelation(String relname) throws USException;

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
	 * Retrieve a published transaction from the update store.
	 * 
	 * @param txn		The ID of the transaction to retrieve
	 * @return			The contents of the transaction
	 * @throws USException
	 */
	abstract public List<Update> getTransaction(TxnPeerID txn) throws USException;
	
	/**
	 * Reconcile the viewing peer with the shared database
	 * 
	 * @return 				The number of the reconciliation just performed
	 * @throws DbException
	 */
	abstract public int reconcile() throws DbException;

	/**
	 * Publish the viewing peer's pending updates to the shared database
	 * 
	 * @throws SSException, USException
	 */
	abstract public void publish() throws SSException, USException;
	
	/**
	 * Resolve the specified conflicts by accepting the specified options.
	 * 
	 * The <code>conflictResolution</code> argument is a mapping from reconciliation
	 * numbers to mappings from conflict numbers to an option numbers to accept for that
	 * conflict. If the option to accept is null, then all options for that conflict are
	 * rejected.
	 * 
	 * @param conflictResolutions	The mapping from reconciliation numbers to mappings from
	 * 								conflict numbers to option numbers to reject, as described
	 * 								above
	 * @throws DbException
	 */
	abstract public void resolveConflicts(Map<Integer,Map<Integer,Integer>> conflictResolutions)
	 throws DbException;
	/**
	 * Determine if the viewing peer has accepted a transaction
	 * 
	 * @param txn			The ID of the transaction and of the peer that inserted it
	 * @return 				<code>true</code> if the viewing peer has accepted it,
	 * 						<code>false</code> otherwise.
	 * @throws USException
	 */
	abstract public boolean hasAcceptedTxn(TxnPeerID txn) throws USException;

	/**
	 * Determine if the viewing peer has rejected a transaction.
	 * 
	 * @param txn			The ID of the transaction and of the peer that inserted it
	 * @return 				<code>true</code> if the viewing peer has accepted it,
	 * 						<code>false</code> otherwise.
	 * @throws USException
	 */
	abstract public boolean hasRejectedTxn(TxnPeerID txn) throws USException;

	/**
	 * Get all the values that are dirty for the viewing peer at this point in time.
	 * 
	 * @return				The set of tuples that are dirty
	 * @throws DbException
	 */
	abstract public TupleSet getDirtyValues() throws DbException;
	
	/**
	 * Disconnect from the shared database. Local state is maintained.
	 * 
	 * @throws DbException
	 */
	abstract public void disconnect() throws DbException;
	
	/**
	 * Reconnect to the shared database. It is assumed that the
	 * this participant has not modified that shared database between
	 * disconnect and reconnect
	 * 
	 * @throws DbException
	 */
	abstract public void reconnect() throws DbException;
	
	abstract public boolean isConnected();
	
	abstract public void setBenchmark(Benchmark b) throws DbException;
	
	abstract public Benchmark getBenchmark();
	
	
	/**
	 * Get the ID associated with this connection to the database
	 * 
	 * @return The ID
	 */
	public AbstractPeerID getID() {
		return id;
	}
	
	public Schema getSchema() {
		return schema;
	}
	
	/**
	 * Reset all local state (i.e. unpublished transactions, etc.) and bring the
	 * local database handle to a state that is consistent with the global state.
	 * 
	 * @throws DbException
	 */
	public abstract void reset() throws DbException;
	
	/**
	 * Reset all local state (i.e. unpublished transactions, etc.) and if {@code replay == true} bring the
	 * local database handle to a state that is consistent with the global state.
	 * @param replay 
	 * 
	 * @throws DbException
	 */
	public abstract void reset(boolean replay) throws DbException;
	
	public static class InvalidUpdate extends DbException {
		private static final long serialVersionUID = 1L;
		InvalidUpdate(Update u) {
			super("Update " + u + " contains tuples from two different tables");
		}
	}
	
	/**
	 * Flattens a list of updates into the supplied lists.
	 * In the output, deletions precede modifications, which in turn precede insertions.
	 * 
	 * @param updates The updates to process
	 * @return The list of flattened updates, sorted as specified
	 * @throws InconsistentUpdates
	 * @throws InvalidUpdate 
	 */
	public static ArrayList<Update> flatten(List<Update> updates) throws InconsistentUpdates, InvalidUpdate
	{
		List<Map<Subtuple,Update>> hms = new ArrayList<Map<Subtuple,Update>>();
		ArrayList<Update> flattenedUpdates = new ArrayList<Update>();
		
		for (Update u : updates) {
			if (u.isUpdate() && ! u.getOldVal().getRelationName().equals(u.getNewVal().getRelationName())) {
				throw new InvalidUpdate(u);
			}
			int relnum = u.getRelationID();
			while (relnum >= hms.size()) {
				hms.add(new HashMap<Subtuple,Update>());
			}
			Map<Subtuple,Update> hm = hms.get(relnum);

			Subtuple key = (u.isInsertion()) ? u.getNewVal().getKeySubtuple() : u.getOldVal().getKeySubtuple();
			Update flattened = hm.get(key);
			if (flattened == null) {
				if (u.isDeletion()) {
					// These go straight into the output, since we can't combine them
					// with other operations
					flattenedUpdates.add(u.duplicate());
				} else {
					hm.put(key, u.duplicate());
				}
			} else if (flattened.getNewVal() != null && u.getNewVal() != null &&
					flattened.getNewVal().equals(u.getNewVal())) {
				// Deal with insertions of the same value from different updates
				flattened.addTids(u.getTids());
				for (TxnPeerID tpi : u.getPrevTids()) {
					flattened.addPrevTid(tpi);
				}
			} else if (! ((flattened.getNewVal() == null && u.getOldVal() == null) ||
						flattened.getNewVal() != null && flattened.getNewVal().equals(u.getOldVal()))){
					throw new InconsistentUpdates(u.duplicate(), flattened.getNewVal().duplicate());
			} else if (flattened.isInsertion() && u.isDeletion()) {
				// Put placeholder into flattened update sequence (which preserves initial value and tid and is used during reconciliation)
				flattened.setNewVal(null);
				flattened.addTidsFromUpdate(u);
				flattenedUpdates.add(flattened);
				hm.remove(key);
			} else if (flattened.isInsertion() && u.isUpdate()) {
				flattened.setNewVal(u.getNewVal().duplicate());
				flattened.addTidsFromUpdate(u);
			} else if (flattened.isUpdate() && u.isDeletion()) {
				flattened.setNewVal(null);	// i.e. null
				flattened.addTidsFromUpdate(u);
				flattenedUpdates.add(flattened);
				hm.remove(key);
			} else if (flattened.isUpdate() && u.isUpdate()) {
				flattened.setNewVal(u.getNewVal().duplicate());
				flattened.addTidsFromUpdate(u);
 			} else {
				throw new RuntimeException("Error in Db.flatten");
			}
			
			if (u.isUpdate() && flattened != null) {
				// Deal with changed key values
				hm.remove(key);
				key = flattened.getNewVal().getKeySubtuple();
				hm.put(key, flattened);
			}
		}
		
		for (Map<Subtuple,Update> hm : hms) {
			flattenedUpdates.addAll(hm.values());
		}
		
		// Update implements compareTo so deletions precede modifications,
		// which precede insertion
		java.util.Collections.sort(flattenedUpdates);
		
		return flattenedUpdates;
	}
	
	// These two are temporary calls so we can do update exchange with a rec
	// number WITHOUT calling Db.reconcile()

	/** @deprecated */
	public abstract int getRecNo() throws DbException;
	/** @deprecated */
	public abstract void setRecDone() throws DbException;
	
}