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
import java.io.IOException;

import edu.upenn.cis.orchestra.datamodel.TxnPeerID;

class GetStatusMsg implements Serializable {
	private static final long serialVersionUID = 1L;
	TxnPeerID tpi;
	
	GetStatusMsg(TxnPeerID tpi) {
		this.tpi = tpi;
	}

	private void writeObject(java.io.ObjectOutputStream out) throws IOException {
		byte[] tpiBytes = tpi.getBytes();
		out.writeInt(tpiBytes.length);
		out.write(tpiBytes);
	}

	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
		int tpiLength = in.readInt();
		byte[] tpiBytes = new byte[tpiLength];
		in.read(tpiBytes);
		tpi = TxnPeerID.fromBytes(tpiBytes, 0, tpiBytes.length);
	}
}