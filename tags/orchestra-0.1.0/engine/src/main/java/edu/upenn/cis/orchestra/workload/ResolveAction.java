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
package edu.upenn.cis.orchestra.workload;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import edu.upenn.cis.orchestra.datamodel.IntPeerID;
import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.reconciliation.Db;

public class ResolveAction extends WorkloadAction {
	private static final long serialVersionUID = 1L;
	Map<Integer,Set<TxnPeerPair>> toAccept = null;
	/**
	 * Create an action that corresponds to resolving one or more conflicts. Conflicts
	 * will be resolved by accepting the options that correspond to the specifed
	 * transactions.
	 * 
	 * @param peer			Integer ID for the peer that is resolving the conflicts
	 * @throws Exception
	 */
	public ResolveAction(int peer) throws Exception {
		super(peer);
		toAccept = new HashMap<Integer,Set<TxnPeerPair>>();
	}

	public void acceptTxn(int recno, int txnPeer, int txnID) {
		if (toAccept.get(recno) == null) {
			toAccept.put(recno, new HashSet<TxnPeerPair>());
		}
		toAccept.get(recno).add(new TxnPeerPair(txnID, txnPeer));
	}
	
	protected void doAction(Map<Integer,Db> dbs, LockManagerClient lmc) throws Exception {
		Map<Integer,Map<Integer,Integer>> conflictResolutions =
			new HashMap<Integer,Map<Integer,Integer>>();
		
		Map<Integer,List<List<Set<TxnPeerID>>>> conflicts = dbs.get(peer).getConflicts();
		for (int recno : conflicts.keySet()) {
			Map<Integer,Integer> res = new HashMap<Integer,Integer>();
			Set<TxnPeerID> txns = new HashSet<TxnPeerID>();
			Set<TxnPeerID> remainingTxns = new HashSet<TxnPeerID>();
			if (toAccept.get(recno) != null) {
				for (TxnPeerPair tpp : toAccept.get(recno)) {
					TxnPeerID tpi = new TxnPeerID(tpp.txn, new IntPeerID(tpp.peer)); 
					txns.add(tpi);
					remainingTxns.add(tpi);
				}
			}
			List<List<Set<TxnPeerID>>> conflictsForRecno = conflicts.get(recno);
			final int numConflicts = conflictsForRecno.size();
			for (int conflict = 0; conflict < numConflicts; ++conflict) {
				final List<Set<TxnPeerID>> conflictOptions = conflictsForRecno.get(conflict);
				final int numOptions = conflictOptions.size();
				int chosenOption = -1;
				for (int option = 0; option < numOptions; ++option) {
					Set<TxnPeerID> optionTxns = conflictOptions.get(option);
					boolean foundAcceptedTxn = false;
					for (TxnPeerID tpi : txns) {
						if (optionTxns.contains(tpi)) {
							foundAcceptedTxn = true;
							break;
						}
					}
					if (foundAcceptedTxn) {
						if (chosenOption != -1) {
							System.err.println("Found two matching options for conflict");
							chosenOption = -1;
							break;
						}
						chosenOption = option;
					}
				}
				if (chosenOption != -1) {
					remainingTxns.removeAll(conflictOptions.get(chosenOption));
					res.put(conflict, chosenOption);
				} else {
					res.put(conflict, null);
				}
			}
			
			// Check for conflicts that we missed
			for (TxnPeerID tpi : remainingTxns) {
				throw new ConflictNotFound(new IntPeerID(peer), recno,tpi);
			}
			
			conflictResolutions.put(recno, res);
		}
		dbs.get(peer).resolveConflicts(conflictResolutions);
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		
		sb.append("Peer " + peer + ": accept options");
		
		Set<Integer> recnos = toAccept.keySet();
		List<Integer> recnosSorted = new Vector<Integer>();
		recnosSorted.addAll(recnos);
		Collections.sort(recnosSorted);

		for (int recno : recnosSorted) {
			Set<TxnPeerPair> tpps = toAccept.get(recno);
			
			for (TxnPeerPair tpp : tpps) {
				sb.append(" r" + recno + "p" + tpp.peer + "t" + tpp.txn);
			}
		}
		
		return sb.toString();
	}
	
	protected static class TxnPeerPair implements Serializable {
		private static final long serialVersionUID = 1L;
		int txn;
		int peer;
		TxnPeerPair(int txn, int peer) {
			this.txn = txn;
			this.peer = peer;
		}
		
		public boolean equals(Object o) {
			if (o == null || o.getClass() != this.getClass())
				return false;
			TxnPeerPair tpp = (TxnPeerPair) o;
			return (txn == tpp.txn && peer == tpp.peer);
		}
		
		public int hashCode() {
			return txn * 37 + peer;
		}
	}
	
	static public class ConflictNotFound extends Exception {
		private static final long serialVersionUID = 1L;
		protected ConflictNotFound(AbstractPeerID pid, int recno, TxnPeerID tpi) {
			super("Could not find conflict option for txn ID " + tpi + " during peer "
					+ pid + "'s reconciliation no. " + recno);
		}
	}
	
	static public class MultipleOptionsForConflict extends Exception {
		private static final long serialVersionUID = 1L;
		protected MultipleOptionsForConflict(AbstractPeerID peer, int recno, int conflict) {
			super("Trying to accept multiple options for conflict " + conflict + " during peer " +
					peer + " reconciliation no. " + recno);
		}
	}
}
