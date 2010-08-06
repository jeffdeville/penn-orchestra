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
package edu.upenn.cis.orchestra.reconciliation.bdbstore;

import java.io.Serializable;

import edu.upenn.cis.orchestra.datamodel.ByteBufferReader;
import edu.upenn.cis.orchestra.datamodel.ByteBufferWriter;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.reconciliation.ISchemaIDBinding;
import edu.upenn.cis.orchestra.reconciliation.TxnChain;

class ReconciliationData implements Serializable {
	private static final long serialVersionUID = 1L;
	
	transient ByteBufferWriter bbw;
	transient ByteBufferReader bbr;
//	transient Schema s;
	transient ISchemaIDBinding _map;
	private byte[] data;
	
	ReconciliationData() {
		bbw = new ByteBufferWriter();
	}
	
	void finish() {
		data = bbw.getByteArray();
		bbw = null;
	}
	
	void writeEntry(TxnPeerID tpi, int prio, TxnChain tc) {
		bbw.addToBuffer(tpi);
		bbw.addToBuffer(prio);
		if (tc == null) {
			bbw.addToBuffer((byte[]) null);
		} else {
			bbw.addToBuffer(tc.getBytes(true));
		}
	}
	
	static class Entry {
		TxnPeerID tpi;
		int prio;
		TxnChain tc;
		Entry(TxnPeerID tpi, int prio, TxnChain tc) {
			this.tpi = tpi;
			this.prio = prio;
			this.tc = tc;
		}
	}
	
	void beginReading(ISchemaIDBinding s) {
		this._map = s;
		bbr = new ByteBufferReader(s, data);
//		this.s = s;
	}
	
	ReconciliationData.Entry readEntry() {
		if (bbr.hasFinished()) {
			return null;
		}
		
		TxnPeerID tpi = bbr.readTxnPeerID();
		int prio = bbr.readInt();
		byte[] chain = bbr.readByteArray();
		TxnChain tc;
		if (chain == null || chain.length == 0) {
			tc = null;
		} else {
//			tc = TxnChain.fromBytes(s, chain, 0, chain.length);
			tc = TxnChain.fromBytes(_map, chain, 0, chain.length);
		}
		return new Entry(tpi, prio, tc);
	}
	
}