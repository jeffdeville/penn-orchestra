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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.datamodel.Update;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore.USException;

public class USDump implements Serializable {
	private static final long serialVersionUID = 1L;
	private Map<AbstractPeerID,Schema> schemas;
	private ISchemaIDBinding _binding;
	private Map<TxnPeerID,Integer> txnEpochs = new HashMap<TxnPeerID,Integer>();
	private Map<TxnPeerID,List<Update>> txns = new HashMap<TxnPeerID,List<Update>>();
	private Map<AbstractPeerID,List<Decision>> decisions = new HashMap<AbstractPeerID,List<Decision>>();
	private Map<AbstractPeerID,Map<Integer,Integer>> recnoEpochs = new HashMap<AbstractPeerID,Map<Integer,Integer>>();

	public USDump(ISchemaIDBinding bindings, Map<AbstractPeerID,Schema> schemas) {
		if (schemas.isEmpty()) {
			throw new IllegalArgumentException("Dump must contain at least one schema");
		}
		this._binding = bindings;
		this.schemas = new HashMap<AbstractPeerID,Schema>(schemas);
	}

	public void addTxnEpoch(TxnPeerID txn, int epoch) {
		txnEpochs.put(txn, epoch);
	}
	public void addTxn(TxnPeerID tid, List<Update> txn) {
		txns.put(tid, txn);
	}
	public void addTxns(Map<TxnPeerID,? extends List<Update>> txns) {
		this.txns.putAll(txns);
	}
	public void addDecision(AbstractPeerID pid, Decision d) {
		List<Decision> decsForPeer = decisions.get(pid);
		if (decsForPeer == null) {
			decsForPeer = new ArrayList<Decision>();
			decisions.put(pid, decsForPeer);
		}
		decsForPeer.add(d);
	}
	public void addDecision(AbstractPeerID pid, TxnPeerID tid, int recno, boolean accept) {
		addDecision(pid, new Decision(tid,recno,accept));
	}
	public void addRecnoEpoch(AbstractPeerID pid, int recno, int epoch) {
		Map<Integer,Integer> recnoEpochsForPeer = recnoEpochs.get(pid);
		if (recnoEpochsForPeer == null) {
			recnoEpochsForPeer = new HashMap<Integer,Integer>();
			recnoEpochs.put(pid, recnoEpochsForPeer);
		}
		recnoEpochsForPeer.put(recno,epoch);
	}
	public Map<AbstractPeerID,Schema> getSchemas() {
		return Collections.unmodifiableMap(schemas);
	}
	public Iterator<TxnPeerID> getTids() {
		return Collections.unmodifiableMap(txns).keySet().iterator();
	}
	public int getTxnEpoch(TxnPeerID tpi) {
		Integer epoch = txnEpochs.get(tpi);
		if (epoch != null) {
			return epoch;
		} else {
			throw new IllegalArgumentException("Nothing is known about txn " + tpi);
		}
	}
	public List<Update> getTxnContents(TxnPeerID tpi) {
		List<Update> txn = txns.get(tpi);
		if (txn != null) {
			return txn;
		} else {
			throw new IllegalArgumentException("Nothing is known about txn " + tpi);
		}
	}
	public Set<AbstractPeerID> getPeers() {
		HashSet<AbstractPeerID> retval = new HashSet<AbstractPeerID>();
		retval.addAll(decisions.keySet());
		retval.addAll(recnoEpochs.keySet());
		return retval;
	}
	public Iterator<Decision> getPeerDecisions(AbstractPeerID pid) {
		List<Decision> decsForPeer = decisions.get(pid);
		if (decsForPeer == null) {
			decsForPeer = Collections.emptyList();
		}
		return Collections.unmodifiableList(decsForPeer).iterator();
	}
	public Iterator<USDump.RecnoEpoch> getPeerRecons(AbstractPeerID pid) {
		Map<Integer,Integer> reconsForPeer = recnoEpochs.get(pid);
		if (reconsForPeer == null) {
			reconsForPeer = Collections.emptyMap();
		}
		final Iterator<Map.Entry<Integer, Integer>> it = reconsForPeer.entrySet().iterator();

		return new Iterator<USDump.RecnoEpoch>() {
			public boolean hasNext() {
				return it.hasNext();
			}

			public USDump.RecnoEpoch next() {
				Map.Entry<Integer, Integer> val = it.next();
				return new RecnoEpoch(val.getKey(), val.getValue());
			}

			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}
	public static class RecnoEpoch {
		public final int recno;
		public final int epoch;
		RecnoEpoch(int recno, int epoch) {
			this.recno = recno;
			this.epoch = epoch;
		}
	}
	private void writeObject(java.io.ObjectOutputStream out)
	throws IOException {
//		out.writeObject(schemas);
		out.writeObject(txnEpochs);
		out.writeObject(decisions);
		out.writeObject(recnoEpochs);
		
		out.writeInt(txns.size());
		for (Map.Entry<TxnPeerID,List<Update>> me : txns.entrySet()) {
			TxnPeerID tid = me.getKey();
			List<Update> txn = me.getValue();
			byte[] bytes = tid.getBytes();
			out.writeInt(bytes.length);
			out.write(bytes);
			out.writeInt(txn.size());
			for (Update u : txn) {
				bytes = u.getBytes(Update.SerializationLevel.VALUES_AND_TIDS);
				out.writeInt(bytes.length);
				out.write(bytes);
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	private void readObject(java.io.ObjectInputStream in)
	throws IOException, ClassNotFoundException {
//		schemas = (Map) in.readObject();
		txnEpochs = (Map) in.readObject();
		decisions = (Map) in.readObject();
		recnoEpochs = (Map) in.readObject();
		
		int numTxns = in.readInt();
		
		txns = new HashMap<TxnPeerID,List<Update>>(numTxns);
		
		while (numTxns > 0) {
			byte[] tidBytes = new byte[in.readInt()];
			in.read(tidBytes);
			TxnPeerID tid = TxnPeerID.fromBytes(tidBytes);
			int numUpdates = in.readInt();
			List<Update> txn = new ArrayList<Update>(numUpdates);
			txns.put(tid, txn);
//			Schema s = schemas.get(tid.getPeerID());
			Schema s;
			try {
				s = _binding.getSchema(tid.getPeerID());
			} catch (USException ue) {
				throw new ClassNotFoundException();
			}
			while (numUpdates > 0) {
				byte[] updateBytes = new byte[in.readInt()];
				in.read(updateBytes);
				Update u = Update.fromBytes(_binding, updateBytes, 0, updateBytes.length);
				txn.add(u);
				--numUpdates;
			}
			--numTxns;
		}
	}

}