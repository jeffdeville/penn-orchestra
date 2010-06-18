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
import edu.upenn.cis.orchestra.reconciliation.Decision;

class RecordDecisions implements Serializable {
	private static final long serialVersionUID = 1L;
	private byte[] decisions;
	
	transient ByteBufferReader bbr;
	
	RecordDecisions(Iterable<Decision> decisions) {
		ByteBufferWriter bbw = new ByteBufferWriter();
		for (Decision decision : decisions) {
			bbw.addToBuffer(decision.tpi);
			bbw.addToBuffer(decision.recno);
			bbw.addToBuffer(decision.accepted);
		}
		this.decisions = bbw.getByteArray();
	}

	void startReading() {
		bbr = new ByteBufferReader(null, decisions);
	}
	
	Decision readDecision() {
		if (bbr.hasFinished()) {
			return null;
		}
		TxnPeerID tpi = bbr.readTxnPeerID();
		int recno = bbr.readInt();
		boolean accept = bbr.readBoolean();
		
		return new Decision(tpi, recno, accept);
	}
}