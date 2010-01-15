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
package edu.upenn.cis.orchestra.datamodel;

import java.io.IOException;


/**
 * Class to hold a pair of a transactionID and a peer ID, which uniquely
 * identify a specific transaction for this database.
 * 
 * @author Nick Taylor
 */
public class TxnPeerID implements java.io.Serializable {
	private static final long serialVersionUID = 1L;
	private int tid;
	private AbstractPeerID peerID;

	/**
	 * Create a new transaction/peer ID pair.
	 * 
	 * @param tid The transaction ID
	 * @param peerID The peer ID
	 * @throws PeerOutOfRange If the peer ID is invalid
	 */
	public TxnPeerID(int tid, AbstractPeerID peerID) {

		if (tid < 0) {
			throw new IllegalArgumentException("tid must be >= 0");
		}
		this.tid = tid;
		this.peerID = peerID.duplicate();
	}

	private TxnPeerID(AbstractPeerID peerID, int tid) {
		this.tid = tid;
		this.peerID = peerID;
	}

	/**
	 * Duplicate an existing transaction/peer ID pair
	 * 
	 * @param tpi				The pair to duplicate
	 * @throws PeerOutOfRange	If the peer ID is invalid
	 */
	public TxnPeerID(TxnPeerID tpi) {

		this.tid = tpi.tid;
		this.peerID = tpi.peerID.duplicate();
	}

	/**
	 * Gets the transaction ID for this pair.
	 * 
	 * @return	The transaction ID
	 */
	public int getTid() {
		return tid;
	}

	/**
	 * Gets the peer ID for this pair
	 * 
	 * @return	The peer ID
	 */
	public AbstractPeerID getPeerID() {
		return peerID;
	}

	public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		TxnPeerID tpi = (TxnPeerID) o;
		return ((tpi.tid == tid) && (tpi.peerID.equals(peerID))); 
	}
	public int hashCode() {
		return tid + 37 * peerID.hashCode();
	}
	public String toString() {
		//return "(TID=" + tid + ",PID=" + peerID + ")";
		return Integer.toString(tid) + peerID.toString();
	}
	public byte[] getBytes() {
		// Serialization holds: Txn ID (4 bytes), length of peer ID in bytes (4 bytes), peer ID (? bytes)
		byte[] peerBytes = peerID.getBytes();
		byte[] txnBytes = IntType.getBytes(tid);
		byte[] retval = new byte[txnBytes.length + IntType.bytesPerInt + peerBytes.length];
		int retvalPos = 0;
		for (int i = 0; i <txnBytes.length; ++i) {
			retval[retvalPos++] = txnBytes[i];
		}
		byte[] peerBytesLength = IntType.getBytes(peerBytes.length);
		for (int i = 0; i < peerBytesLength.length; ++i) {
			retval[retvalPos++] = peerBytesLength[i];
		}
		for (int i = 0; i < peerBytes.length; ++i) {
			retval[retvalPos++] = peerBytes[i];
		}
		return retval;
	}

	public static TxnPeerID fromBytes(byte[] bytes, int offset, int length) {
		final int bytesPerInt = IntType.bytesPerInt;
		int txnID = IntType.getValFromBytes(bytes, offset);
		offset += bytesPerInt;
		int peerIdLength = IntType.getValFromBytes(bytes, offset);
		offset += bytesPerInt;
		if (peerIdLength + 2 * bytesPerInt != length) {
			throw new RuntimeException("Length specified by byte stream does not batch length from argument");
		}
		AbstractPeerID pi = AbstractPeerID.fromBytes(bytes, offset, peerIdLength);
		return new TxnPeerID(txnID, pi);
	}

	public static TxnPeerID fromBytes(byte[] bytes) {
		return fromBytes(bytes, 0, bytes.length);
	}

	public TxnPeerID duplicate() {
		// Since TxnPeerID is immutable (as long as PeerID is),
		// don't duplicate unless peerID.duplicate() creates a new
		// object
		AbstractPeerID copy = peerID.duplicate();
		if (copy == peerID) {
			return this;
		} else {
			return new TxnPeerID(copy, tid);
		}
	}

	private void writeObject(java.io.ObjectOutputStream out) throws IOException {
		byte[] pidBytes = peerID.getBytes();
		out.writeInt(tid);
		out.writeInt(pidBytes.length);
		out.write(pidBytes);
	}

	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
		tid = in.readInt();
		int length = in.readInt();
		byte[] bytes = new byte[length];
		in.readFully(bytes);
		peerID = AbstractPeerID.fromBytes(bytes);
	}
}