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
import java.util.List;

import edu.upenn.cis.orchestra.datamodel.ByteBufferReader;
import edu.upenn.cis.orchestra.datamodel.ByteBufferWriter;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.datamodel.Update;

class PublishMsg implements Serializable {
	private static final long serialVersionUID = 1L;
	private byte[] txns;
	
	transient ByteBufferReader txnsReader;
	
	PublishMsg(List<List<Update>> txns) {
		ByteBufferWriter bbw = new ByteBufferWriter();
		ByteBufferWriter bbw2 = new ByteBufferWriter();
		for (List<Update> txn : txns) {
			bbw2.clear();
			for (Update u : txn) {
				bbw2.addToBuffer(u, Update.SerializationLevel.VALUES_AND_TIDS);
			}
			bbw.addToBuffer(txn.get(0).getLastTid());
			bbw.addToBuffer(bbw2.getByteArray());
		}
		this.txns = bbw.getByteArray();
	}
	
	void startReading() {
		txnsReader = new ByteBufferReader(null, txns);
	}
	
	static class TxnToPublish {
		TxnPeerID tpi;
		byte[] contents;
	}
	
	TxnToPublish readTxn() {
		if (txnsReader.hasFinished()) {
			return null;
		}
		
		TxnToPublish ttp = new TxnToPublish();
		ttp.tpi = txnsReader.readTxnPeerID();
		ttp.contents = txnsReader.readByteArray();
		
		return ttp;
	}	
}
