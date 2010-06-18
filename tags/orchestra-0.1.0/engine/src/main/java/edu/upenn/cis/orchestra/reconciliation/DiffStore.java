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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.ByteBufferReader;
import edu.upenn.cis.orchestra.datamodel.ByteBufferWriter;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.datamodel.Update;
import edu.upenn.cis.orchestra.datamodel.iterators.ResultIterator;


public abstract class DiffStore extends StateStore {
	private AbstractPeerID pid;
	private int currRecno;
	private int lastTid;
	private int firstRecno;

	// The schema for this store
//	Schema schema;
	protected ISchemaIDBinding schMap;

	protected static class UnknownTable extends SSException {
		private static final long serialVersionUID = 1L;
		String tableName;
		UnknownTable(String which) {
			super("Attempt to apply update to unknown table " + which);
			tableName = which;
		}
	}
	protected static class BadRecno extends SSException {
		private static final long serialVersionUID = 1L;
		int recno;
		BadRecno(int recno, int currRecno, int firstRecno) {
			super("Attempt to accept transaction at recno " + recno +
					" when first recno is " + firstRecno + " and current recno is " + currRecno);
			this.recno = recno;
		}
	}
	protected static class StoreEntry {
		Tuple value;
		Set<TxnPeerID> antecedents;

		StoreEntry(Tuple v) {
			value = v;
			antecedents = new HashSet<TxnPeerID>();
		}

		StoreEntry(Tuple v, Set<TxnPeerID> antecedents) {
			this(v);
			this.antecedents.addAll(antecedents);
		}

		byte[] getBytes() {
			ByteBufferWriter bbw = new ByteBufferWriter();

			bbw.addToBuffer(value);
			bbw.addToBuffer(antecedents.size());
			for (TxnPeerID tpi : antecedents) {
				bbw.addToBuffer(tpi);
			}

			return bbw.getByteArray();
		}

		static StoreEntry fromBytes(ISchemaIDBinding s, byte[] bytes, int offset, int length) throws SSException {
			ByteBufferReader bbr = new ByteBufferReader(s, bytes, offset, length);

			Tuple value = bbr.readTuple();
			int numTids = bbr.readInt();

			StoreEntry retval = new StoreEntry(value);

			for (int i = 0; i < numTids; ++i) {
				retval.antecedents.add(bbr.readTxnPeerID());
			}

			if (! bbr.hasFinished()) {
				throw new SSException("Data remaining in buffer after reading StoreEntry");
			}

			return retval;
		}
	}

	DiffStore(AbstractPeerID pid, ISchemaIDBinding schema, int lastTid) {
		/*
		if (! schema.isFinished()) {
			throw new IllegalArgumentException("Must create a DiffStore over a finished schema");
		}*/
		this.pid = pid.duplicate();
		this.schMap = schema;
		this.lastTid = lastTid;
		resetLocalState();
	}

	protected int getRecno() {
		return currRecno;
	}

	final public void clearStateBefore(int recno) throws SSException {
		clearStateBeforeImpl(recno);
		firstRecno = recno;
	}

	abstract void clearStateBeforeImpl(int recno) throws SSException;

	final public boolean containsTuple(int recno, Tuple t) throws SSException {
		Tuple tt = getTupleWithKey(recno,t);
		return (tt != null) && tt.equals(t);
	}


	public Tuple getTupleWithKey(int recno, Tuple t) throws SSException {
		if (recno < firstRecno || recno >  currRecno) {
			throw new BadRecno(recno, firstRecno, currRecno);
		}
		Tuple val = null;
		StoreEntry currVal = getStoreEntry(t);
		if (currVal != null) {
			val = currVal.value;
		}

		if (recno < currRecno) {
			List<Update> updatesForKey = getUpdateList(t, recno + 1);
			for (int i = updatesForKey.size() - 1; i >= 0; --i) {
				Update u = updatesForKey.get(i);
				if (u.getNewVal().sameKey(t)) {
					if (u.getOldVal() == null || ! u.getOldVal().sameKey(t)) {
						val = null;
					} else {
						val = u.getOldVal();
					}
				}
			}

		}

		if (val == null) {
			return null;
		} else {
			return val.duplicate();
		}
	}

	final public TxnPeerID prepareTransaction(List<Update> txn) throws SSException {
		TxnPeerID tid = new TxnPeerID(++lastTid, pid);

		for (Update u : txn) {
			if (u.getLastTid() != null || (! u.getPrevTids().isEmpty())) {
				throw new SSException("Attempt to prepare already prepared update: " + u);
			}
			if (u.isDeletion() || u.isUpdate()) {
				StoreEntry se = getStoreEntry(u.getOldVal());
				if (se == null || (! se.value.equals(u.getOldVal()))) {
					throw new SSException("Cannot prepare update " + u + " since original value " + u.getOldVal() + " is not in the database");
				}
				for (TxnPeerID tpi : se.antecedents) {
					u.addPrevTid(tpi);
				}
			}
			u.addTid(tid);
		}

		return tid;
	}


	public void applyTransaction(int recno, List<Update> txn) throws SSException {
		if (recno < firstRecno || recno >  currRecno) {
			throw new BadRecno(recno, firstRecno, currRecno);
		}
		for (Update u : txn) {
			boolean isAdditionalTid = false;
			StoreEntry oldValEntry = null;
			StoreEntry newValEntry = null;
			if (u.getOldVal() != null) {
				oldValEntry = getStoreEntry(u.getOldVal());
				if (recno != currRecno) {
					List<Update> thisKeyUpdates = getUpdateList(u.getOldVal(), recno + 1);
					if (thisKeyUpdates.size() != 0) {
						throw new UpdateError(recno, u, "Update being accepted at reconciliation " + recno + " would be modified by later update " + thisKeyUpdates.get(0));
					}
				}
				if (oldValEntry != null && oldValEntry.value.equals(u.getOldVal())) {
					setStoreEntry(u.getOldVal(), null);
				}
			}
			if (u.getNewVal() != null) {
				newValEntry = getStoreEntry(u.getNewVal());
				if (newValEntry != null) {
					if (u.getNewVal().equals(newValEntry.value)) {
						// An additional insertion/update to an already present value
						isAdditionalTid = true;
					} else {
						// Key violation
						throw new UpdateError(recno, u, "State already contains tuple with same key: " + newValEntry.value);
					}
				}
				if (recno != currRecno) {
					List<Update> thisKeyUpdates = getUpdateList(u.getNewVal(), recno + 1);
					if (thisKeyUpdates != null) {
						if (thisKeyUpdates.size() != 0) {
							throw new UpdateError(recno, u, "Update being accepted at reconciliation " + recno + " would be modified by later update " + thisKeyUpdates.get(0));
						}
					}
				}
				if (isAdditionalTid) {
					newValEntry.antecedents.addAll(u.getTids());
					setStoreEntry(u.getNewVal(), newValEntry);
				} else {
					setStoreEntry(u.getNewVal(), new StoreEntry(u.getNewVal(), u.getTids()));
				}
			}
			addToUpdateList(recno, u);
		}
	}


	final public int getCurrentRecno() {
		return currRecno;
	}

	final public void advanceRecno() throws SSException, Db.InconsistentUpdates {
		++currRecno;
		recnoHasAdvanced();
	}


	/**
	 * Get the store entry for the specified key
	 * 
	 * @param t					The key of the tuple to retrieve the entry for
	 * @return					The store entry for the supplied key
	 * @throws SSException
	 */
	abstract StoreEntry getStoreEntry(Tuple t) throws SSException;

	/**
	 * Set the store entry for the specified key 
	 * 
	 * @param t					The key of the tuple to set the entry for
	 * @param se				The value to set the entry for, or <code>null</code>
	 * 							to delete it.
	 * @throws SSException
	 */
	abstract void setStoreEntry(Tuple t, StoreEntry se) throws SSException;

	/**
	 * Append an update to the update list for the specified key
	 * for the specified reconciliation (which may be the current reconciliation).
	 * The update list should be flattened if necessary.
	 * 
	 * @param recno			The reconciliation during which to add the update
	 * @param u				The update to add to the update list
	 * @throws SSException
	 */
	abstract void addToUpdateList(int recno, Update u) throws SSException;

	/**
	 * Get the concatenation of the update lists (in causal order) for the specified
	 * key from the specified start reconciliation to the current reconciliation
	 * (inclusive). The updates for each reconciliation are flattened together.
	 * Information from the Update object other than old and new values is not
	 * retrieved.
	 * 
	 * @param t					The key of interest
	 * @param startRecno		The reconciliation at which to start
	 * @return
	 * @throws SSException
	 */
	abstract List<Update> getUpdateList(Tuple t, int startRecno) throws SSException;

	/**
	 * Perform any processing needed after a reconciliation has occurred. It should only be
	 * called once for each reconciliation.
	 * 
	 * @throws SSException
	 */
	abstract void recnoHasAdvanced() throws SSException;

	public ResultIterator<Tuple> getState(int recno, String relname) throws SSException {
		return null;
	}

	private void resetLocalState() {
		firstRecno = FIRST_RECNO;
		currRecno = firstRecno;
	}
	
	public void reset() throws SSException {
		resetLocalState();
		resetDiffStore();
	}
	
	protected abstract void resetDiffStore() throws SSException;
}
