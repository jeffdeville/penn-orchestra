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

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Transaction;

import edu.upenn.cis.orchestra.datamodel.ByteBufferReader;
import edu.upenn.cis.orchestra.datamodel.ByteBufferWriter;
import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.reconciliation.PidAndRecno;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdRange;
import rice.p2p.commonapi.NodeHandle;

abstract class ReplicationControllerMessage extends P2PMessage {
	ReplicationControllerMessage(Id dest) {
		super(dest);
	}
	
	ReplicationControllerMessage(ReplicationControllerMessage origRequest) {
		super(origRequest);
	}
	
	ReplicationControllerMessage(Id dest, NodeHandle hint) {
		super(dest,hint);
	}
	
	ReplicationControllerMessage(NodeHandle dest) {
		super(dest);
	}
	
	ReplicationControllerMessage(boolean createNull) {
		super(createNull);
	}
	
}

/**
 * Determine if there is data for the specified Id in the overlay network
 * 
 * 
 * @author netaylor
 *
 */
class TryToFindId extends ReplicationControllerMessage {
	static final long serialVersionUID = 1L;
	Id id;
	TryToFindId(Id id) {
		super(id);
		this.id = id;
	}
}

/**
 * Reply to a TryToFindId message
 * 
 * @author netaylor
 *
 */
class IdFound extends ReplicationControllerMessage {
	static final long serialVersionUID = 1L;
	IdFound(TryToFindId origRequest) {
		super(origRequest);
	}
}

/**
 * Reply to a TryToFindId message
 * 
 * @author netaylor
 *
 */
class IdNotFound extends ReplicationControllerMessage {
	static final long serialVersionUID = 1L;
	IdNotFound(TryToFindId origRequest) {
		super(origRequest);
	}
}

/**
 * A message to request all of the data that hashes to a particular 
 * 
 * @author netaylor
 *
 */
class RequestDataForId extends ReplicationControllerMessage implements ClonableMessage {
	static final long serialVersionUID = 1L;
	private final Id id;
	RequestDataForId(Id id) {
		super(id);
		this.id = id;
	}
	RequestDataForId(Id id, NodeHandle nh, boolean isHint) {
		super(isHint ? id : null, nh);
		this.id = id;
	}
	
	Id getId() {
		return id;
	}
	
	public RequestDataForId cloneMessage(NodeHandle newDest) {
		return new RequestDataForId(id, newDest, false);
	}
}

/**
 * Informs a peer of data to insert into its controllers. The data may be new
 * data that is being distributed to its replica set, it may be data that the peer
 * should have but is missing due to nodes joining and leaving, or it may be sent
 * due to a request from the Pastry replication manager. This
 * message may be sent in response to a {@link RequestDataForId} message, but
 * it may also be sent unrequested.
 * 
 * @author netaylor
 *
 */
class DataToAdd extends ReplicationControllerMessage implements ClonableMessage {
	static final long serialVersionUID = 1L;
	
	final static private byte EPOCH = 1, PEER = 2, TXN = 3, RECON = 4;
	
	transient private ByteBufferWriter bbw = new ByteBufferWriter();
	private byte[] data = null;
	
	DataToAdd(Transaction t, ReplicationController.Entries entries, EpochController ec,
			PeerController pc, TransactionController tc, RequestDataForId origReq) throws DatabaseException {
		super(origReq);
		addEntries(t,entries,ec,pc,tc);
		finish();
	}
	
	DataToAdd(Transaction t, ReplicationController.Entries entries, EpochController ec,
			PeerController pc, TransactionController tc, Id dest) throws DatabaseException {
		super(dest);
		addEntries(t,entries,ec,pc,tc);
		finish();
	}

	DataToAdd(Transaction t, ReplicationController.Entries entries, EpochController ec,
			PeerController pc, TransactionController tc, NodeHandle dest) throws DatabaseException {
		super(dest);
		addEntries(t,entries,ec,pc,tc);
		finish();
	}
	
	void addEntries(Transaction t, ReplicationController.Entries entries,
			EpochController ec, PeerController pc,
			TransactionController tc) throws DatabaseException {
		for (int epoch : entries.epochs) {
			ec.getEpoch(t, epoch, this);
		}
		
		for (AbstractPeerID pid : entries.pids) {
			pc.getPeer(t, pid, this);
		}
		
		for (TxnPeerID tpi : entries.tids) {
			tc.getTransaction(t, tpi, this);
		}
	}
	
	DataToAdd(NodeHandle dest) {
		super(dest);
	}
	
	DataToAdd() {
		super(true);
	}
	
	void addEpoch(int epoch, byte[] txnIds) {
		bbw.addToBuffer(EPOCH);
		bbw.addToBuffer(epoch);
		bbw.addToBuffer(txnIds);
	}
	
	void addPeer(AbstractPeerID pid, byte[] trustConditions) {
		bbw.addToBuffer(PEER);
		bbw.addToBuffer(pid);
		bbw.addToBuffer(trustConditions);
	}
	
	void addReconciliation(PidAndRecno par, byte[] contents) {
		bbw.addToBuffer(RECON);
		bbw.addToBuffer(par);
		bbw.addToBuffer(contents);
	}
	
	void addTransaction(TxnPeerID tpi, byte[] contents) {
		bbw.addToBuffer(TXN);
		bbw.addToBuffer(tpi);
		bbw.addToBuffer(contents);
	}
		
	void finish() {
		data = bbw.getByteArray();
		bbw = null;
	}
	
	public DataToAdd cloneMessage(NodeHandle newDest) {
		DataToAdd dta = new DataToAdd(newDest);
		dta.data = data;
		return dta;
	}
		
	void decode(Transaction t, EpochController ec, PeerController pc, TransactionController tc) throws DatabaseException {
		ByteBufferReader bbr = new ByteBufferReader(null, data);
		while (! bbr.hasFinished()) {
			byte code = bbr.readByte();
			if (code == EPOCH) {
				int epoch = bbr.readInt();
				byte[] txnIds = bbr.readByteArray();
				ec.addEpoch(t, epoch, txnIds);
			} else if (code == PEER) {
				AbstractPeerID pid = bbr.readPeerID();
				byte[] trustConditions = bbr.readByteArray();
				pc.addPeer(t, pid, trustConditions);
			} else if (code == TXN) {
				TxnPeerID tpi = bbr.readTxnPeerID();
				byte[] contents = bbr.readByteArray();
				tc.addTransaction(t, tpi, contents);
			} else if (code == RECON) {
				PidAndRecno par = bbr.readPidAndRecno();
				byte[] contents = bbr.readByteArray();
				pc.addReconciliation(t, par, contents);
			} else {
				throw new RuntimeException("Error wile decoding DataToAdd");
			}
		}
	}
}

class GetRemoteRange extends ReplicationControllerMessage {
	private static final long serialVersionUID = 1L;

	GetRemoteRange(NodeHandle dest) {
		super(dest);
	}
}

class RemoteRangeIs extends ReplicationControllerMessage {
	private static final long serialVersionUID = 1L;
	final IdRange range;
	
	RemoteRangeIs(IdRange range, GetRemoteRange request) {
		super(request);
		this.range = range;
	}
}