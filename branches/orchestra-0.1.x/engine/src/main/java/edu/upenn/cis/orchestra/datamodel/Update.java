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
package edu.upenn.cis.orchestra.datamodel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.upenn.cis.orchestra.reconciliation.ConflictType;
import edu.upenn.cis.orchestra.reconciliation.DbException;
import edu.upenn.cis.orchestra.reconciliation.ISchemaIDBinding;


/**
 * Class to hold an update to the database
 * 
 * @author Nick Taylor
 */
public class Update implements Comparable<Update>, java.io.Serializable {
	private static final long serialVersionUID = 1L;
	private Tuple oldVal;
	private Tuple newVal;
	// IDs of the transactions that contributed to this update, in
	// the order they would be applied
	private ArrayList<TxnPeerID> tids;
	private HashSet<TxnPeerID> tidSet;
	private HashSet<TxnPeerID> prevTids;

	// The TID of the first update (considered during this reconciliation)
	// to modify the tuple
	private TxnPeerID initialTid;
	// The value first used by that update
	private Tuple initialVal;
	

	Tuple changedFromTo;

	/**
	 * Initialize a new <code>Update</code> object.
	 * 
	 * @param originalVal The original value of the tuple, or
	 * 		<code>null</code> for an insertion.
	 * @param updatedVal The new value of the tuple, or
	 * 		<code>null</code> for a deletion.
	 */
	public Update(Tuple originalVal, Tuple updatedVal) {
		if (originalVal == null || originalVal.isNull()) {
			oldVal = null;
		} else {
			if (originalVal.isReadOnly()) {
				oldVal = originalVal;
			} else {
				oldVal = originalVal.duplicate();
				oldVal.setReadOnly();
			}
		}
		if (updatedVal == null || updatedVal.isNull()) {
			newVal = null;
		} else {
			if (updatedVal.isReadOnly()) {
				newVal = updatedVal;
			} else {
				newVal = updatedVal.duplicate();
				newVal.setReadOnly();
			}
		}
		if (oldVal == null && newVal != null) {
			initialVal = newVal;
		} else if (oldVal != null) {
			initialVal = oldVal;
		} else {
			initialVal = null;
		}
		changedFromTo = null;
		tids = new ArrayList<TxnPeerID>();
		tidSet = new HashSet<TxnPeerID>();
		prevTids = new HashSet<TxnPeerID>();
		initialTid = null;
	}
	
	/**
	 * Special entry for holding an association between a
	 * "split" replace:  when we break it into a separate
	 * deletion and insertion, we need to preserve what the
	 * "other half" was.  We do that based solely on its key.
	 * 
	 * @param fromTo
	 */
	public void setChangedFromTo(Tuple fromTo) {
		if (fromTo != null)
			changedFromTo = fromTo.duplicate();
		else
			changedFromTo = null;
	}
	
	/**
	 * Get this special association.
	 * 
	 * @return
	 */
	public Tuple getFromTo() {
		return changedFromTo;
	}
	
	public Integer getRelationID() {
		if (isUpdate()) {
			if (newVal.getRelationID() == oldVal.getRelationID()) {
				return newVal.getRelationID();
			} else {
				throw new RuntimeException("Update contains tuples from different relations");
			}
		}
		if (newVal != null)
			return newVal.getRelationID();
		else if (oldVal != null)
			return oldVal.getRelationID();
		else if (initialVal != null)
			return initialVal.getRelationID();
		else
			return null;
	}
	
	public String getRelationName() {
		if (newVal != null) {
			return newVal.getRelationName();
		} else if (oldVal != null) {
			return oldVal.getRelationName();
		} else if (initialVal != null) {
			return initialVal.getRelationName();
		} else {
			return null;
		}
	}

	/**
	 * Adds a transaction ID and the ID of the peer that originated that
	 * transaction to this update.
	 * 
	 * @param tid The transaction ID
	 * @param peerID The ID of the originating peer
	 */
	public void addTid(int tid, AbstractPeerID peerID) {
		TxnPeerID tpid = new TxnPeerID(tid, peerID.duplicate()); 
		tids.add(tpid);
		tidSet.add(tpid);
		if (initialTid == null) {
			initialTid = tpid;
		}
	}
	
	public void addTidsFromUpdate(Update u) {
		for (TxnPeerID tpi : u.tids) {
			addTid(tpi);
		}
	}
	
	/**
	 * Adds a transaction ID and the ID of the peer that originated that
	 * transaction to this update.
	 * 
	 * @param tpi The transaction ID and the ID of the originating peer
	 */
	public void addTid(TxnPeerID tpi) {
		addTid(tpi.getTid(), tpi.getPeerID());
	}

	/**
	 * Add a list of (peer ID, txn ID) pairs to the list of transactions that
	 * contributed to this update.
	 * 
	 * @param tids				The list to add
	 */
	public void addTids(Iterable<TxnPeerID> tids) {
		for (TxnPeerID tpi : tids) {
			addTid(tpi);
		}
		
	}
	
	public void addPrevTid(TxnPeerID tpi) {
		prevTids.add(tpi);
	}
	
	/**
	 * Get a copy of the set of antecedent transactions for this update.
	 * 
	 * @return The (immutable) set of antecedent transactions.
	 */
	public Set<TxnPeerID> getPrevTids() {
		return Collections.unmodifiableSet(prevTids);
	}
	
	/**
	 * Determine if a transaction is an antecedent of this update
	 * 
	 * @param	tpi
	 * @return	<code>true</code> if <code>tpi</code> is an antecedent of this update,
	 * 			<code>false</code> if it is not
	 */
	public boolean isPrevTid(TxnPeerID tpi) {
		return prevTids.contains(tpi);
	}
	
	/**
	 * Gets the transaction IDs and the corresponding originating peer IDs
	 * associated with this update. The are in order.
	 * 
	 * @param tids The list to add the transaction ID/peer ID pairs for this update to
	 */
	public void getTids(List<TxnPeerID> tids) {
		for (TxnPeerID tpi : this.tids) {
			tids.add(tpi.duplicate());
		}
	}
	
	/**
	 * Get the set of transaction IDs associated with this update
	 * 
	 * @return The set of transaction IDs
	 */
	public Set<TxnPeerID> getTids() {
		return Collections.unmodifiableSet(tidSet);
	}
	
	/**
	 * Computes a hash code based on the key of the tuple.
	 * Only works for insert or delete, since replacement
	 * actually has two keys.
	 * 
	 * @return XOR of the keys of the relations
	 */
	public int hashCode() {
		if (isInsertion()) {
			return newVal.hashCode();
		
		} else if (isDeletion() || isUpdate()) {
			return oldVal.hashCode();
		} else
			throw new RuntimeException("Can't get hash code on replace!");
	}
			
	/**
	 * Get a copy of the last txn/peer ID pair for this update
	 * 
	 * @return A copy of the pair
	 */
	public TxnPeerID getLastTid() {
		if (tids.isEmpty()) {
			return null;
		} else {
			return tids.get(tids.size() - 1);
		}
	}
	
    /**
     * Create a deep copy of this <code>Update</code>
     * 
     * @return		The deep copy of <code>this</code>
     */
	public Update duplicate() {
		Update u = new Update(oldVal, newVal);
		for (TxnPeerID tpi: tids) {
			u.addTid(tpi);
		}
		for (TxnPeerID tpi : prevTids) {
			u.addPrevTid(tpi);
		}
		u.initialTid = (initialTid == null) ? null : initialTid.duplicate();
		u.initialVal = (initialVal == null) ? null : initialVal.duplicate();
		return u;
	}
    
	/**
	 * Checks to see if this update encodes an insertion.
	 * 
	 * @return <code>true</code> if it does,
	 * 		<code>false</code> otherwise.
	 */
	public boolean isInsertion() {
		return (oldVal == null && newVal != null);
	}
	
	/**
	 * Checks to see if this update encodes a deletion.
	 * 
	 * @return <code>true</code> if it does,
	 * 		<code>false</code> otherwise.
	 */
	public boolean isDeletion() {
		return (oldVal != null && newVal == null);
	}
	
	/**
	 * Checks to see if this update encodes a modification.
	 * 
	 * @return <code>true</code> if it does,
	 * 		<code>false</code> otherwise.
	 */
	public boolean isUpdate() {
		return (oldVal != null && newVal != null);
	}
	
	/**
	 * Checks to see if this update is a placeholder, recording
	 * the presence of a value inserted and then deleted
	 * 
	 * @return <code>true</code> if it is, <code>false</code> if it is not
	 */
	public boolean isNull() {
		return (oldVal == null && newVal == null);
	}
	
	/**
	 * Determine if two updates conflict because they make different
	 * modifications to the same initial value
	 * 
	 * @param u		The update to check against
	 * @return		<code>true</code> if they do conflict for this reason,
	 * 				false if they do not (though they may still conflict for
	 * 				some other reason)
	 * @throws DbException 
	 */
	public boolean conflictsInitial(Update u) throws DbException {
		return (ConflictType.getConflictType(this,u) == ConflictType.ConflictTypeCode.INITIAL);
	}
	
	/**
	 * Determine if an update conflicts with this update.
	 * 
	 * @param u the Update to check against
	 * @return <code>true</code> if they conflict, <code>false</code> if they do not
	 * @throws DbException 
	 */
	public boolean conflicts(Update u) throws DbException {
		return (ConflictType.getConflictType(this,u) != null);
	}
	
	/**
	 * Compares two updates, for use in sorting a list so deletions precede
	 * modifications, which precede insertions.
	 * 
	 * @param u The update to compate <code>this</code> with.
	 * @return -1, 0, or 1, depending on the values of <code>this</code>
	 * 		and <code>u</code>
	 */
	public int compareTo(Update u) {
			if (isDeletion()) {
				if (u.isDeletion()) {
					return 0;
				} else {
					return -1;
				}
			} else if (isUpdate()) {
				if (u.isDeletion()) {
					return 1;
				} else if (u.isUpdate()) {
					return 0;
				} else if (u.isInsertion()) {
					return -1;
				}
			} else if (isInsertion()) {
				if (u.isInsertion()) {
					return 0;
				} else {
					return 1;
				}
			}
			// Just to satisfy the compiler, if we call
			// checkValid on input before sorting this
			// is not needed.
			return 0;
	}
	
	public String toString() {
		StringBuffer retval = new StringBuffer();
		if (oldVal == null) {
			retval.append("+");
		} else {
			retval.append(oldVal.toString());
		}
		if (oldVal != null && newVal != null)
			retval.append(" -> ");
		
		if (newVal == null) {
			retval.insert(0, "-");
		} else {
			retval.append(newVal.toString());
		}
		
		if (! tids.isEmpty()) {
			retval.append(" ");
			retval.append(tids.toString());
		}
		if (! prevTids.isEmpty()) {
			retval.append(" /" + getPrevTids().toString());
		}
		return retval.toString();
	}
	
	public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		Update u = (Update) o;
		if (getFromTo() == null && u.getFromTo() != null)
			return false;
		if (getFromTo() != null && u.getFromTo() == null)
			return false;
		if (getFromTo() != null && !getFromTo().equals(u.getFromTo()))
			return false;
		
		boolean tidsEqual = u.tids.equals(tids);
		boolean prevTidEqual = (prevTids == null) ? (u.prevTids == null) :
			prevTids.equals(u.prevTids);
		boolean initialValuesEqual = (initialVal == null) ? (u.initialVal == null) :
			initialVal.equals(u.initialVal);
		boolean initialTxnEqual = (initialTid == null) ? (u.initialTid == null) :
			initialTid.equals(u.initialTid);
		
		//System.out.println(this + " =? " + u + ": " + (oldEquals && newEquals && tidsEqual && prevTidEqual));
		return tidsEqual && prevTidEqual && initialValuesEqual && initialTxnEqual &&
			equalsOnValues(u);
	}

	/**
	 * Determine if the old and new values for two updates are the same
	 * 
	 * @param u		The update to compare with
	 * @return		<code>true</code> if they are equal, <code>false</code> if not
	 */
	public boolean equalsOnValues(Update u) {
		boolean oldEquals = (oldVal == null) ? (u.oldVal == null) :
			oldVal.equals(u.oldVal);
		boolean newEquals = (newVal == null) ? (u.newVal == null) :
			newVal.equals(u.newVal);
		
		return oldEquals && newEquals;
	}

	public Tuple getOldVal() {
		return oldVal;
	}

	public void setNewVal(Tuple newVal) {
		if (newVal == null || newVal.isNull()) {
			this.newVal = null;
		} else {
			if (newVal.isReadOnly()) {
				this.newVal = newVal;
			} else {
				this.newVal = newVal.duplicate();
				this.newVal.setReadOnly();
			}
		}
	}

	public Tuple getNewVal() {
		return newVal;
	}

	public TxnPeerID getInitialTid() {
		return initialTid;
	}
	
	public void setInitialTid(TxnPeerID tpi) {
		initialTid = tpi.duplicate();
	}

	public Tuple getInitialVal() {
		return initialVal;
	}
	
	public void setInitialVal(Tuple t) {
		initialVal = t.duplicate();
	}
	
	public enum SerializationLevel {
		VALUES_ONLY (1),
		VALUES_AND_TIDS (2),
		ALL (3);
	
		final int code;
		SerializationLevel(int c) {
			code = c;
		}
		
	}
	
	/**
	 * Determine the byte array that represents this Update object
	 * 
	 * @param sl				A <code>SerializationLevel</code> representing how much
	 * 							data is to be written. Options are: VALUES_ONLY (only
	 * 							write old and new values), VALUES_AND_TIDS (write values,
	 * 							component tids, prev tid), ALL (write values, component
	 * 							tids, prev tid, initial tid and value, changedFromTo,
	 * 							and bestPriority)
	 * @return					The byte array
	 */
	public byte[] getBytes(SerializationLevel sl) {
		/* Order of serialization is:
		 * 
		 * 4 bytes		Serialization level (from SerializationLevel.code)
		 * 4 bytes		Length of old value, or 0 if not present
		 * ? bytes		Old value
		 * 4 bytes		Length of new value, or 0 if not present
		 * ? bytes		New value
		 * ------------ VALUES_ONLY stops here
		 * 4 bytes		# of component tids to read (in order)
		 * 		For each tid to read:
		 * 				4 bytes		Length of tid
		 * 				? bytes		tid
		 * 4 bytes		# of previous tids
		 * 		For each tid to read:
		 * 				4 bytes		Length of tid
		 * 				? bytes		tid
		 * ? bytes		Previous tid
		 * ------------ VALUES_AND_TIDS stops here
		 * 4 bytes		Length of initial tid, or 0 if not present
		 * ? bytes		Initial tid
		 * 4 bytes		Length of initial value, or 0 if not present
		 * ? bytes		Initial value
		 * 4 bytes		Length of changedFromTo, or 0 if not present
		 * ? bytes		changedFromTo
		 * 4 bytes 		bestPriority
		 */

		ByteBufferWriter bb = new ByteBufferWriter();
		
		bb.addToBuffer(sl.code);
		bb.addToBuffer(oldVal);
		bb.addToBuffer(newVal);
		
		if (sl == SerializationLevel.VALUES_ONLY) {
			return bb.getByteArray();
		}
		

		bb.addToBuffer(tids.size());
		for (TxnPeerID tpi : tids) {
			bb.addToBuffer(tpi);
		}
		bb.addToBuffer(prevTids.size());
		for (TxnPeerID tpi : prevTids) {
			bb.addToBuffer(tpi);
		}
		
		if (sl == SerializationLevel.VALUES_AND_TIDS) {
			return bb.getByteArray();
		}
		
		bb.addToBuffer(initialTid);
		bb.addToBuffer(initialVal);
		bb.addToBuffer(changedFromTo);
		
		return bb.getByteArray();
		
	}
	
	public static Update fromBytes(ISchemaIDBinding s, byte[] bytes, int offset, int length) {
		ByteBufferReader bbr = new ByteBufferReader(s, bytes, offset, length);
		int slCode = bbr.readInt();
		SerializationLevel sl;
		if (slCode == SerializationLevel.VALUES_ONLY.code) {
			sl = SerializationLevel.VALUES_ONLY;
		} else if (slCode == SerializationLevel.VALUES_AND_TIDS.code) {
			sl = SerializationLevel.VALUES_AND_TIDS;
		} else {
			sl = SerializationLevel.ALL;
		}
		
		Tuple oldVal = bbr.readTuple();
		Tuple newVal = bbr.readTuple();
		
		if (oldVal == null && newVal == null)
			System.err.println("Unexpected: null update");
		
		Update u = new Update(oldVal, newVal);
		
		if (sl == SerializationLevel.VALUES_ONLY) {
			return u;
		}
		
		for (int numTids = bbr.readInt(); numTids > 0; --numTids) {
			u.addTid(bbr.readTxnPeerID());
		}
		
		int numPrevTid = bbr.readInt();
		for (int i = 0; i < numPrevTid; ++i) {
			u.addPrevTid(bbr.readTxnPeerID());
		}
		
		if (sl == SerializationLevel.VALUES_AND_TIDS) {
			return u;
		}
		
		u.initialTid = bbr.readTxnPeerID();
		u.initialVal = bbr.readTuple();
		u.changedFromTo = bbr.readTuple();
		
		if (! bbr.hasFinished()) {
			throw new RuntimeException("Data remaining in specified section of byte array after reading Update");
		}
		
		return u;
	}
}