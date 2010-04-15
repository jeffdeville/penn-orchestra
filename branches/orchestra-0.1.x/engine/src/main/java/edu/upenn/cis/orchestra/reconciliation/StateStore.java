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

import java.util.List;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.sleepycat.je.DatabaseException;

import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.TupleSet;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.datamodel.Update;
import edu.upenn.cis.orchestra.datamodel.iterators.IteratorException;
import edu.upenn.cis.orchestra.datamodel.iterators.ResultIterator;
import edu.upenn.cis.orchestra.util.XMLParseException;

/**
 * A class to store the state of a specific peer's database
 * over many reconciliations
 * 
 * @author Nick Taylor
 *
 */
public abstract class StateStore {
	public static final int FIRST_RECNO = 0;
	public interface Factory {
		public StateStore getStateStore(AbstractPeerID pid, ISchemaIDBinding s, int lastTid) throws SSException;
		public void serialize(Document doc, Element store);

		/**
		 * Shuts down the state store factory. This factory's
		 * {@getStateStore(...)} method should not be
		 * called once this method is called.
		 * 
		 * @throws SSException
		 */
		public void shutdown() throws SSException;
	}
	public static class SSException extends DbException {
		private static final long serialVersionUID = 1L;

		public SSException() {
			super();
		}

		public SSException(String arg0) {
			super(arg0);
		}

		public SSException(String arg0, Throwable arg1) {
			super(arg0, arg1);
		}

		public SSException(Throwable arg0) {
			super(arg0);
		}

	}

	public static class UpdateError extends SSException {
		private static final long serialVersionUID = 1L;
		public int recno;
		public Update u;
		public String why;

		public UpdateError(int recno, Update u, String why) {
			super("Couldn't apply " + u + " during reconciliation " + recno +": " + why);
			this.recno = recno;
			this.u = u;
			this.why = why;
		}
	}

	abstract void close() throws SSException;
	
	abstract void reopen() throws SSException;

	/**
	 * Clear all of the state information prior to the specified reconciliation
	 * 
	 * @param recno			The first reconciliation for which state information
	 * 						is still needed
	 * @throws SSException
	 */
	abstract void clearStateBefore(int recno) throws SSException;

	/**
	 * Determine if the state contains the specified
	 * tuple immediately after the specified reconciliation.
	 * 
	 * @param recno			The reconciliation during which to check
	 * @param t				The tuple to check for
	 * @return				<code>true</code> if the tuple is present,
	 * 						<code>false</code> otherwise
	 * @throws SSException
	 */
	abstract boolean containsTuple(int recno, Tuple t) throws SSException;

	/**
	 * Gets the tuple with the specified key, if it exists immediately after
	 * the specified reconciliation
	 * 
	 * @param recno		The reconciliation during which to check
	 * @param t			The tuple whose key to check for
	 * @return			The matching tuple if it exists, or
	 * 					<code>null</code> if it does not
	 * @throws SSException
	 */
	abstract Tuple getTupleWithKey(int recno, Tuple t) throws SSException;

	/**
	 * Prepare a new (as of yet unpublished) transaction from the peer
	 * that owns the state store. The updates in the transaction
	 * <code>txn</code> have their TID and previous TID set. The
	 * transaction must already have been flattened.
	 * 
	 * @param txn		The transaction to apply; it will be modified
	 * 					so the component updates have their TIDs and
	 * 					previous TIDs set.
	 * @return			The TID assigned to this transaction
	 * @throws SSException
	 */
	abstract TxnPeerID prepareTransaction(List<Update> txn) throws SSException;

	/**
	 * Apply a transaction (prepared by this peer or published by another peer)
	 * to this peer's state
	 * 
	 * @param recno		The reconciliation during which to apply the transaction
	 * @param txn		The transaction to apply
	 * @throws SSException
	 */
	abstract void applyTransaction(int recno, List<Update> txn) throws SSException;

	/**
	 * Get the current reconciliation recorded in this state store
	 * 
	 * @return			The number of the last reconciliation
	 * @throws SSException
	 */
	abstract int getCurrentRecno() throws SSException;

	/**
	 * Add a new reconciliation, with the same content as the final state
	 * of the current reconciliation 
	 * 
	 * @throws SSException
	 */
	abstract void advanceRecno() throws SSException, Db.InconsistentUpdates;

	/**
	 * Get the current state of the specified relation
	 * 
	 * @param relname	The name of the relation of interest
	 * @return			The state as a map, this allows lookup by key
	 * @throws SSException
	 */
	final TupleSet getState(String relname) throws SSException {
		ResultIterator<Tuple> stateIterator = getStateIterator(relname);
		TupleSet retval = new TupleSet();
		try {
			while (stateIterator.hasNext()) {
				retval.add(stateIterator.next());
			}
		} catch (IteratorException e) {
			throw new SSException(e.getMessage(), e.getCause());
		}
		return retval;
	}

	/**
	 * Get the current state of the specified relation
	 * 
	 * @param relname	The name of the relation of interest
	 * @return				An iterator, which must be closed by the caller
	 * @throws SSException
	 */
	abstract ResultIterator<Tuple> getStateIterator(String relname) throws SSException;

	static public StateStore.Factory deserialize(Element store) throws XMLParseException {
		String type = store.getAttribute("type");
		if (type.compareToIgnoreCase("hash") == 0) {
			return HashTableStore.Factory.deserialize(store);
		} else if (type.compareToIgnoreCase("bdb") == 0) {
			try {
				return BerkeleyDBStore.Factory.deserialize(store);
			} catch (DatabaseException e) {
				throw new XMLParseException(e.getMessage(), e.getCause());
			}
		}
		throw new XMLParseException("Unrecognized state store type \'" + type + "\'", store);
	}
	
	/**
	 * Clears all state from the state store and sets the current reconciliation
	 * number to FIRST_RECNO
	 * 
	 * @throws SSException
	 */
	public abstract void reset() throws SSException;
}
