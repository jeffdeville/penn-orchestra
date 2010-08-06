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

import java.util.Map;
import java.util.Vector;

import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.Update;
import edu.upenn.cis.orchestra.reconciliation.Db;


public class TransactionAction extends WorkloadAction {
	private static final long serialVersionUID = 1L;
	private Vector<Tuple> oldVals;
	private Vector<Tuple> newVals;
	
	public TransactionAction(int peer) {
		super(peer);
		oldVals = new Vector<Tuple>();
		newVals = new Vector<Tuple>();
	}

	protected void doAction(Map<Integer,Db> dbs, LockManagerClient lmc) throws Exception {
		int numVals = oldVals.size();
		Vector<Update> txn = new Vector<Update>();
		for (int i = 0; i < numVals; ++i) {
			txn.add(new Update(oldVals.get(i), newVals.get(i)));
		}
		
		dbs.get(peer).addTransaction(txn);
	}
	
	protected void addInsertion(Tuple newVal) {
		oldVals.add(null);
		newVals.add(newVal);
	}
	
	protected void addUpdate(Tuple oldVal, Tuple newVal) {
		oldVals.add(oldVal);
		newVals.add(newVal);
	}
	
	protected void addDeletion(Tuple oldVal) {
		oldVals.add(oldVal);
		newVals.add(null);
	}
	
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("Peer " + peer + ": add transaction [");
		
		int numOps = oldVals.size();
		for (int i = 0; i < numOps; ++i) {
			Tuple oldVal = oldVals.get(i);
			Tuple newVal = newVals.get(i);
			sb.append(oldVal == null ? "-" : oldVal);
			sb.append(" => ");
			sb.append(newVal == null ? "-" : newVal);
			if (i != numOps -1) {
				sb.append(", ");
			}
		}
		
		sb.append("]");
		
		return sb.toString();
	}
}
