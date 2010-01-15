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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import edu.upenn.cis.orchestra.datamodel.ByteBufferReader;
import edu.upenn.cis.orchestra.datamodel.ByteBufferWriter;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;

class GetTxns implements Serializable {
	private static final long serialVersionUID = 1L;
	private final byte tpis[];
	
	GetTxns(Collection<TxnPeerID> tpis) {
		ByteBufferWriter bbw = new ByteBufferWriter();
		for (TxnPeerID tpi : tpis) {
			bbw.addToBuffer(tpi);
		}
		this.tpis = bbw.getByteArray();
	}
	
	List<TxnPeerID> getTpis() {
		ArrayList<TxnPeerID> retval = new ArrayList<TxnPeerID>();
		
		ByteBufferReader bbr = new ByteBufferReader(tpis);
		while (! bbr.hasFinished()) {
			retval.add(bbr.readTxnPeerID());
		}
		
		return retval;
	}
}
