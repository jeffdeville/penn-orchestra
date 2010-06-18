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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import edu.upenn.cis.orchestra.datamodel.IntPeerID;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.reconciliation.Db;

class WorkloadPeer {
	Db db;
	Random random;
	int peerNum;
	double reconcileProb;
	double resolveProb;
	double updateProb;
	int txnSize;
	TupleGenerator valueGenerator;
	
	/**
	 * Create a new random updater or browser
	 * 
	 * @param peerNum			The peer number of this updates
	 * @param db				The db it will update
	 * @param random			The <code>Random</code> object to use as a source of randomness
	 * @param reconcileProb		Probability that this peer reconcilines during a given cycle
	 * @param resolveProb		Probability that this resolves a conflict during a given cycle 
	 * @param updateProb		Probability of performing a transaction
	 * @param txnSize			The size of a transaction for this peer
	 * @param valGen			The object that will be used to create new tuples
	 */
	public WorkloadPeer(int peerNum, Db db, Random random,
			double reconcileProb, double resolveProb, double updateProb,
			int txnSize, TupleGenerator valGen) throws Exception {
		this.peerNum = peerNum;
		this.db = db;
		this.random = random;
		this.reconcileProb = reconcileProb;
		this.resolveProb = resolveProb;
		this.updateProb = updateProb;
		this.txnSize = txnSize;
		valueGenerator = valGen;
	}

	/**
	 * Randomly choose an action on the database supplied to this
	 * peer and return a WorkloadAction representing it. Note that
	 * this does not apply the WorkloadAction!
	 * 
	 * @return			The <code>WorkloadAction</code> representing what the peer did
	 */
	WorkloadAction chooseAction() throws Exception {
		WorkloadAction wa = null;
		double rand = random.nextDouble();
		if (rand < reconcileProb) {
			wa = new ReconcileAction(peerNum);
		} else if (rand < (reconcileProb + resolveProb)) {
			boolean foundConflict = false;
			Map<Integer,List<List<Set<TxnPeerID>>>> deferred = db.getConflicts();
			ResolveAction ra = new ResolveAction(peerNum);
			
			// For each conflict, choose one option and accept
			// all transactions from it. WorkloadAction will sort out
			// what to do in the case of ambiguity
			for (int recno : deferred.keySet()) {
				List<List<Set<TxnPeerID>>> conflicts = deferred.get(recno);
				
				Set<TxnPeerID> allTxns = new HashSet<TxnPeerID>();
				Set<TxnPeerID> rejectedTxns = new HashSet<TxnPeerID>();
				
				for (List<Set<TxnPeerID>> options : conflicts) {
					foundConflict = true;

					final int numOptions = options.size();

					List<Integer> possibleTxns = new ArrayList<Integer>();
					
					for (int i = 0; i < numOptions; ++i) {
						Set<TxnPeerID> option = options.get(i);
						allTxns.addAll(option);
						boolean possible = true;
						for (TxnPeerID tpi : rejectedTxns) {
							if (option.contains(tpi)) {
								possible = false;
								break;
							}
						}
						if (possible) {
							possibleTxns.add(i);
						}
					}
					
					int chosen = -1;
					if (! possibleTxns.isEmpty()) {
						chosen = possibleTxns.get(random.nextInt(possibleTxns.size()));
					}
					
					for (int i = 0; i < numOptions; ++i) {
						if (i != chosen) {
							rejectedTxns.addAll(options.get(i));
						}
					}
				}

				for (TxnPeerID tpi : allTxns) {
					if (rejectedTxns.contains(tpi)) {
						continue;
					}
					ra.acceptTxn(recno, ((IntPeerID) tpi.getPeerID()).getID(), tpi.getTid());
				}
			}
			if (foundConflict) {
				wa = ra;
			}
		} else if (rand < (reconcileProb + resolveProb + updateProb)) {
			Set<Tuple> dirty = db.getDirtyValues();
			// Values modified by this transaction
			Set<Tuple> already = new HashSet<Tuple>();
			TransactionAction ta = new TransactionAction(peerNum);
			
			int currRecno = db.getCurrentRecno();
			
			for (int i = 0; i < txnSize; ++i) {
				Tuple t = valueGenerator.getTuple();
				if (already.contains(t) || dirty.contains(t)) {
					continue;
				}
				already.add(t);
				
				Tuple curr = db.getValueForKey(currRecno, t);
				
				if (curr == null) {
					// Insertion
					ta.addInsertion(t);
					for (Tuple supp : valueGenerator.getSupportingTuples(t)) {
						ta.addInsertion(supp);
					}
				} else if (! curr.equals(t)) {
					ta.addUpdate(curr, t);
				}
			}
			if (! already.isEmpty()) {
				// If txn has size 0 for whatever reason, don't return
				// a TransactionAction
				wa = ta;
			}
		}
		return wa;
	}
}
