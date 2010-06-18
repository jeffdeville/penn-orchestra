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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import edu.upenn.cis.orchestra.datamodel.ByteBufferReader;
import edu.upenn.cis.orchestra.datamodel.ByteBufferWriter;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.datamodel.Update;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore.TransactionSource;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore.USException;


public class TxnChain {
	private List<Update> txnContents;
	private TxnPeerID head;
	private Set<TxnPeerID> tail;
	private Set<TxnPeerID> components;
	private boolean isFlattened = false;

	// Note that only one of the following two data members needs to be serialized,
	// since they contain the same data.
	// Mapping from dependent txn to set of antecedent transactions
	private Map<TxnPeerID, Set<TxnPeerID>> antecedents;
	// Mapping from antecedent txn to set of dependent transactions
	private Map<TxnPeerID, Set<TxnPeerID>> dependents;

	
	/**
	 * Create a new <code>TxnChain</code> object
	 * 
	 * @param contents			The flattened transaction chain
	 * @param head				The root transaction (i.e. the trusted one)
	 * @param tail				Transactions with no antecedents included in this
	 * 							object (i.e. ones with no antecedents at all or with no
	 * 							included antecedents)
	 * @param hasGraph			<code>true</code> if this object should create a dependency
	 * 							graph 
	 */
	public TxnChain(List<Update> contents, TxnPeerID head, Set<TxnPeerID> tail, boolean hasGraph) {
		txnContents = contents;
		this.head = head;
		this.tail = new HashSet<TxnPeerID>();
		this.tail.addAll(tail);
		this.components = new HashSet<TxnPeerID>();
		recomputeComponents();
		if (hasGraph) {
			antecedents = new HashMap<TxnPeerID, Set<TxnPeerID>>();
			dependents = new HashMap<TxnPeerID, Set<TxnPeerID>>();
		}
	}
	
	/**
	 * Create a new <code>TxnChain</code> object with empty contents and tail
	 * 
	 * @param head
	 */
	public TxnChain(TxnPeerID head, boolean hasGraph) {
		this(null, head, new HashSet<TxnPeerID>(), hasGraph);		
	}

	public void addToTail(TxnPeerID tailMember) {
		tail.add(tailMember);
	}
	
	public void setContents(List<Update> contents) {
		this.txnContents = contents;
		recomputeComponents();
	}
	
	public List<Update> getContents() {
		return txnContents;
	}

	public TxnPeerID getHead() {
		return head;
	}

	public Set<TxnPeerID> getTail() {
		return Collections.unmodifiableSet(tail);
	}
	
	public boolean isFlattened() {
		return isFlattened;
	}
	
	public void setFlattened(boolean isFlattened) {
		this.isFlattened = isFlattened;
	}
	
	public Set<TxnPeerID> getComponents() {
		return Collections.unmodifiableSet(components);
	}
	
	/**
	 * Record that an antecedent/dependent relationship is contained within the flattened
	 * transaction chain. 
	 * 
	 * @param antecedent		The antecedent transaction ID
	 * @param dependent			The dependent transaction ID
	 */
	public void addAntecedent(TxnPeerID antecedent, TxnPeerID dependent) {
		if (antecedents == null) {
			throw new IllegalStateException("Cannot call addAntecedent on a TxnChain object without dependency graph");
		}
		Set<TxnPeerID> antecedentSet = antecedents.get(dependent);
		if (antecedentSet == null) {
			antecedentSet = new HashSet<TxnPeerID>();
			antecedents.put(dependent, antecedentSet);
		}

		Set<TxnPeerID> dependentSet = dependents.get(antecedent);
		if (dependentSet == null) {
			dependentSet = new HashSet<TxnPeerID>();
			dependents.put(antecedent, dependentSet);
		}

		antecedentSet.add(antecedent);
		dependentSet.add(dependent);
	}
	
	/**
	 * Get the antecedents of a specified transaction (if it is
	 * included in this transaction chain)
	 * 
	 * @param dependent
	 * @return
	 */
	public Set<TxnPeerID> getAntecedents(TxnPeerID dependent) {
		return antecedents.get(dependent);
	}
	
	/**
	 * Get the dependents of a specified transaction that are in
	 * this transaction chain
	 * 
	 * @param antecedent
	 * @return
	 */
	public Set<TxnPeerID> getDependents(TxnPeerID antecedent) {
		return dependents.get(antecedent);
	}
	
	public static TxnChain fromBytes(ISchemaIDBinding s, byte[] bytes, int offset, int length) {
		ByteBufferReader bbr = new ByteBufferReader(s, bytes, offset, length);
		return new TxnChain(bbr);
	}
	
	public static TxnChain fromBytes(ISchemaIDBinding s, byte[] bytes) {
		return TxnChain.fromBytes(s, bytes, 0, bytes.length);
	}

	public byte[] getBytes(boolean writeUpdates) {
		ByteBufferWriter bbw = new ByteBufferWriter();
		writeBytes(writeUpdates, bbw);
		return bbw.getByteArray();
	}

	public TxnChain(ByteBufferReader bbr) {
		components = new HashSet<TxnPeerID>();
		boolean hasUpdates = bbr.readBoolean();
		if (hasUpdates) {
			txnContents = new ArrayList<Update>();
			int numUpdates = bbr.readInt();
			for (int i = 0; i < numUpdates; ++i) {
				txnContents.add(bbr.readUpdate());
			}			
		}
		tail = new HashSet<TxnPeerID>();
		head = bbr.readTxnPeerID();
		int tailSize = bbr.readInt();
		for (int i = 0; i < tailSize; ++i) {
			tail.add(bbr.readTxnPeerID());
		}
		boolean hasGraph = bbr.readBoolean();
		if (hasGraph) {
			antecedents = new HashMap<TxnPeerID, Set<TxnPeerID>>();
			dependents = new HashMap<TxnPeerID, Set<TxnPeerID>>();
			int numDependents = bbr.readInt();
			for (int i = 0; i < numDependents; ++i) {
				TxnPeerID dependent = bbr.readTxnPeerID();
				int numAntecedents = bbr.readInt();
				for (int j = 0; j < numAntecedents; ++j) {
					TxnPeerID antecedent = bbr.readTxnPeerID();
					addAntecedent(antecedent, dependent);
				}
			}
		}
		recomputeComponents();
	}
	
	public void writeBytes(boolean writeUpdates, ByteBufferWriter bbw) {
		bbw.addToBuffer(writeUpdates);
		if (writeUpdates) {
			bbw.addToBuffer(txnContents.size());
			for (Update u: txnContents) {
				bbw.addToBuffer(u, Update.SerializationLevel.ALL);
			}
		}
		bbw.addToBuffer(head);
		bbw.addToBuffer(tail.size());
		for (TxnPeerID tpi : tail) {
			bbw.addToBuffer(tpi);
		}
		if (antecedents == null) {
			bbw.addToBuffer(false);
		} else {
			bbw.addToBuffer(true);
			bbw.addToBuffer(antecedents.size());
			for (Map.Entry<TxnPeerID, Set<TxnPeerID>> e : antecedents.entrySet()) {
				bbw.addToBuffer(e.getKey());
				bbw.addToBuffer(e.getValue().size());
				for (TxnPeerID tpi : e.getValue()) {
					bbw.addToBuffer(tpi);
				}
			}
		}
	}
	
	public void recomputeComponents() {
		components.clear();
		if (txnContents != null) {
			for (Update u : txnContents) {
				components.addAll(u.getTids());
			}
		} else if (antecedents != null) {
			components.add(head);
			if (antecedents != null) {
				for (Set<TxnPeerID> tpis : antecedents.values()) {
					for (TxnPeerID tpi : tpis) {
						if (! tail.contains(tpi)) {
							components.add(tpi);
						}
					}
				}
			}
		}
	}

	/**
	 * Compute a TxnChain for the specified transaction for the specified
	 * @param ts		A way to retrieve the needed transactions
	 * @param td		A way to determine which transactions have already
	 * 					been accepted or rejected
	 * @param txn		The transaction to retrieve the antecedents for
	 * @throws USException 
	 */
	public TxnChain(TxnPeerID head, TransactionSource ts, TransactionDecisions td) throws USException {
		this(head,true);
	
		// Make sure we don't add the same antecedent transaction more than once
		HashSet<TxnPeerID> alreadyAddedTxns = new HashSet<TxnPeerID>();
		alreadyAddedTxns.add(head);
	
		HashMap<TxnPeerID,Set<TxnPeerID>> needs = new HashMap<TxnPeerID,Set<TxnPeerID>>();
	
		// The transactions we're already considering
		Queue<TxnPeerID> txnsList = new LinkedList<TxnPeerID>();
		txnsList.add(head);
	
		TxnPeerID tid;
		while ((tid = txnsList.poll()) != null) {
			List<Update> txn = ts.getTxn(tid);
			if (txn == null) {
				throw new USException("Txn ID " + tid + " not found in transaction source");
			}
			for (Update u : txn) {
				for (TxnPeerID prevTid : u.getPrevTids()) {
					addAntecedent(prevTid, tid);
					if (td.hasAcceptedTxn(prevTid)) {
						addToTail(prevTid);
					} else if (td.hasRejectedTxn(prevTid)) {
						throw new UpdateStore.AlreadyRejectedAntecedent(prevTid);
					} else {
						Set<TxnPeerID> needed = needs.get(tid);
						if (needed == null) {
							needed = new HashSet<TxnPeerID>();
							needs.put(tid, needed);
						}
						needed.add(prevTid);
						if (! alreadyAddedTxns.contains(prevTid)) {
							alreadyAddedTxns.add(prevTid);
							txnsList.add(prevTid);
						}
					}
				}
			}
		}
	
		ArrayList<TxnPeerID> canApplyTxns = new ArrayList<TxnPeerID>();
		for (TxnPeerID tpid : alreadyAddedTxns) {
			Set<TxnPeerID> needed = needs.get(tpid); 
			if (needed == null || needed.isEmpty()) {
				canApplyTxns.add(tpid);
			}
		}
	
		setContents(new ArrayList<Update>());
	
		// When adding a transaction to the set of applied transactions doesn't
		// let us apply anything else, we're done!
		while (! canApplyTxns.isEmpty()) {
			ArrayList<TxnPeerID> emptyTxns = canApplyTxns;
			canApplyTxns = new ArrayList<TxnPeerID>();
	
			for (TxnPeerID tpid : emptyTxns) {
				needs.remove(tpid);
				getContents().addAll(ts.getTxn(tpid));
				Set<TxnPeerID> dependents = getDependents(tpid);
				if (dependents != null) {
					for (TxnPeerID needingTxn : dependents) {
						Set<TxnPeerID> needed = needs.get(needingTxn);
						needed.remove(tpid);
						if (needed.isEmpty()) {
							canApplyTxns.add(needingTxn);
						}
					}
				}
			}
		}
		recomputeComponents();
	}
	
	public void replaceTailWithAvailableTxns(TransactionSource ts) throws USException {
		Set<TxnPeerID> toAdd = new HashSet<TxnPeerID>();
		Set<TxnPeerID> toConsider = new HashSet<TxnPeerID>(tail);
		
		Map<TxnPeerID,Set<TxnPeerID>> needs = new HashMap<TxnPeerID,Set<TxnPeerID>>(),
			needed = new HashMap<TxnPeerID,Set<TxnPeerID>>();
		
		Set<TxnPeerID> newTail = new HashSet<TxnPeerID>();

		Set<TxnPeerID> empty = Collections.emptySet();
		for (TxnPeerID tpi : tail) {
			needed.put(tpi, empty);
		}
		
		
		while (toConsider.size() > 0) {
			HashSet<TxnPeerID> newToConsider = new HashSet<TxnPeerID>();
			for (TxnPeerID tpi : toConsider) {
				List<Update> txn = ts.getTxn(tpi);
				if (txn == null) {
					newTail.add(tpi);
					if (needed.get(tpi) != null) {
						for (TxnPeerID succ : needed.get(tpi)) {
							needs.get(succ).remove(tpi);
							if (antecedents != null) {
								addAntecedent(tpi, succ);
							}
						}
					}
					needed.remove(tpi);
					continue;
				}
				toAdd.add(tpi);
				Set<TxnPeerID> txnNeeds = needs.get(tpi);
				if (txnNeeds == null) {
					txnNeeds = new HashSet<TxnPeerID>();
					needs.put(tpi, txnNeeds);
				}
				for (Update u : txn) {
					for (TxnPeerID ante : u.getPrevTids()) {
						txnNeeds.add(ante);
						Set<TxnPeerID> txnNeeded = needed.get(ante);
						if (txnNeeded == null) {
							txnNeeded = new HashSet<TxnPeerID>();
							needed.put(ante, txnNeeded);
						}
						txnNeeded.add(tpi);
						if (! toAdd.contains(ante)) {
							newToConsider.add(ante);
						}
					}
				}
			}
			toConsider = newToConsider;
		}
		
		List<Update> newTxnContents = new ArrayList<Update>();
		
		Set<TxnPeerID> canApplyTxns = new HashSet<TxnPeerID>();
		for (TxnPeerID tpi : toAdd) {
			if (needs.get(tpi).isEmpty()) {
				canApplyTxns.add(tpi);
			}
		}
		
		while (canApplyTxns.size() > 0) {
			
			for (TxnPeerID tpi : canApplyTxns) {
				newTxnContents.addAll(ts.getTxn(tpi));
				
				for (TxnPeerID succ : needed.get(tpi)) {
					needs.get(succ).remove(tpi);
					if (antecedents != null) {
						addAntecedent(tpi, succ);
					}
				}
			}
			toAdd.removeAll(canApplyTxns);
			canApplyTxns.clear();
			
			for (TxnPeerID tpi : toAdd) {
				if (needs.get(tpi).isEmpty()) {
					canApplyTxns.add(tpi);
				}
			}
		}
		
		if (! toAdd.isEmpty()) {
			throw new USException("Error replacing txn chain tail");
		}
		
		newTxnContents.addAll(txnContents);
		txnContents = newTxnContents;
		tail = newTail;
		recomputeComponents();
	}
}
