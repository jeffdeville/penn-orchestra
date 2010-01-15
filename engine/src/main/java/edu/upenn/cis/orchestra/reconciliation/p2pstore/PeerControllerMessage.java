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
package edu.upenn.cis.orchestra.reconciliation.p2pstore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import rice.p2p.commonapi.Id;
import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.ByteBufferReader;
import edu.upenn.cis.orchestra.datamodel.ByteBufferWriter;
import edu.upenn.cis.orchestra.datamodel.TrustConditions;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.reconciliation.ISchemaIDBinding;
import edu.upenn.cis.orchestra.reconciliation.PidAndRecno;

/**
 * Class representing a message sent to or from the peer controller
 * 
 * @author Nick Taylor
 *
 */
abstract class PeerControllerMessage extends P2PMessage {
	static final long serialVersionUID = 1L;
	
	PeerControllerMessage(Id dest) {
		super(dest);
	}
	
	PeerControllerMessage(PeerControllerMessage origRequest) {
		super(origRequest);
	}
}

/**
 * Record the results of a peer's reconciliation. If the reconciliation
 * included new transactions (i.e. it is not just doing conflict resolution),
 * then epoch should be set. The reply should be ReplySuccess, or ReplyFailure
 * if the reconciliation has already been published.
 * 
 * @author netaylor
 *
 */
class RecordReconciliation extends PeerControllerMessage {
	static final long serialVersionUID = 1L;
	AbstractPeerID pid;
	int recno;
	List<TxnPeerID> accepted;
	List<TxnPeerID> rejected;
	int epoch;
	
	RecordReconciliation(AbstractPeerID pid, int recno, List<TxnPeerID> accepted, List<TxnPeerID> rejected,
			int epoch, Id dest) {
		super(dest);
		this.pid = pid;
		this.recno = recno;
		this.epoch = epoch;
		
		this.accepted = new ArrayList<TxnPeerID>(accepted.size());
		this.accepted.addAll(accepted);
		this.rejected = new ArrayList<TxnPeerID>(rejected.size());
		this.rejected.addAll(rejected);
	}
		
	private void writeObject(java.io.ObjectOutputStream out) throws IOException {
		ByteBufferWriter bbw = new ByteBufferWriter();
		bbw.addToBuffer(pid);
		bbw.addToBuffer(recno);
		bbw.addToBuffer(epoch);
		bbw.addToBuffer(accepted.size());
		for (TxnPeerID tpi : accepted) {
			bbw.addToBuffer(tpi);
		}
		bbw.addToBuffer(rejected.size());
		for (TxnPeerID tpi : rejected) {
			bbw.addToBuffer(tpi);
		}
		byte[] data = bbw.getByteArray();
		out.writeInt(data.length);
		out.write(data);
	}

	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
		int length = in.readInt();
		byte[] data = new byte[length];
		in.read(data);
		ByteBufferReader bbr = new ByteBufferReader(null,data);
		pid = bbr.readPeerID();
		recno = bbr.readInt();
		epoch = bbr.readInt();
		int acceptedNum = bbr.readInt();
		accepted = new ArrayList<TxnPeerID>(acceptedNum);
		while (acceptedNum > 0) {
			accepted.add(bbr.readTxnPeerID());
			--acceptedNum;
		}
		int rejectedNum = bbr.readInt();
		rejected = new ArrayList<TxnPeerID>(rejectedNum);
		while (rejectedNum > 0) {
			rejected.add(bbr.readTxnPeerID());
			--rejectedNum;
		}
	}	
}

/**
 * Retrieve the epoch number that is associated with a particular
 * reconciliation. Should receive ReconciliationEpochIs or ReplyFailure
 * in response.
 * 
 * @author Nick Taylor
 *
 */
class RequestReconciliationEpoch extends PeerControllerMessage {
	static final long serialVersionUID = 1L;
	
	private final byte[] pid;
	private final int recno;
	private final int mostRecentRecno;
	
	RequestReconciliationEpoch(AbstractPeerID pid, int recno, int mostRecentRecno, Id dest) {
		super(dest);
		this.recno = recno;
		this.pid = pid.getBytes();
		this.mostRecentRecno = mostRecentRecno;
	}
	
	int getRecno() {
		return recno;
	}
	
	AbstractPeerID getPid() {
		return AbstractPeerID.fromBytes(pid);
	}
	
	int getMostRecentRecno() {
		return mostRecentRecno;
	}
}


/**
 * Reply to a RequestReconciliationEpoch message
 * 
 * @author Nick Taylor
 *
 */
class ReconciliationEpochIs extends PeerControllerMessage {
	static final long serialVersionUID = 1L;
	
	private final int epoch;
	
	ReconciliationEpochIs(int epoch, RequestReconciliationEpoch request) {
		super(request);
		this.epoch = epoch;
	}
	
	int getEpoch() {
		return epoch;
	}
}

/**
 * Request the trust conditions for a particular peer
 * 
 * @author netaylor
 *
 */
class RequestTrustConditions extends PeerControllerMessage {
	static final long serialVersionUID = 1L;
	private final byte[] pid;
	
	RequestTrustConditions(AbstractPeerID pid, Id dest) {
		super(dest);
		this.pid = pid.getBytes();
	}
	
	AbstractPeerID getID() {
		return AbstractPeerID.fromBytes(pid, 0, pid.length);
	}
	
	byte[] getPidBytes() {
		return pid;
	}
}

/**
 * Reply to a {@link RequestTrustConditions} message.
 * @author netaylor
 *
 */
class TrustConditionsAre extends PeerControllerMessage {
	static final long serialVersionUID = 1L;

	private final byte[] pid;
	private final byte[] trustConditions;
	
	TrustConditionsAre(byte[] pid, byte[] tc, RequestTrustConditions request) {
		super(request);
		this.pid = pid;
		trustConditions = tc;
	}
		
	TrustConditions getTrustConditions(ISchemaIDBinding s) {
		return new TrustConditions(trustConditions, s);
	}
	
	AbstractPeerID getPid() {
		return AbstractPeerID.fromBytes(pid);
	}
	
	byte[] getPidBytes() {
		return pid;
	}
	
	byte[] getTrustConditionsBytes() {
		return trustConditions;
	}
}

/**
 * Request a reconciliation record. Receives either {@link ReconciliationRecordIs} or
 * {@link ReplyFailure} as a response.
 * 
 * @author netaylor
 *
 */
class RequestReconciliationRecord extends PeerControllerMessage {
	private final byte[] pid;
	private final int recno;
	static final long serialVersionUID = 1L;
	
	RequestReconciliationRecord(AbstractPeerID pid, int recno, Id dest) {
		super(dest);
		this.pid = pid.getBytes();
		this.recno = recno;
	}
		
	PidAndRecno getPar() {
		return new PidAndRecno(AbstractPeerID.fromBytes(pid), recno);
	}
}

class ReconciliationRecordIs extends PeerControllerMessage {
	private static final long serialVersionUID = 1L;
	final byte[] reconciliationRecord;
	
	ReconciliationRecordIs(byte[] reconciliationRecord, RequestReconciliationRecord request) {
		super(request);
		this.reconciliationRecord = reconciliationRecord;
	}
}
