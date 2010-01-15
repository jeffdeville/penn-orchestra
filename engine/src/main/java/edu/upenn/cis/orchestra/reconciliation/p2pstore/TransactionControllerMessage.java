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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import rice.p2p.commonapi.Id;
import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.ByteBufferReader;
import edu.upenn.cis.orchestra.datamodel.ByteBufferWriter;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.datamodel.Update;
import edu.upenn.cis.orchestra.reconciliation.ISchemaIDBinding;

/**
 * A message to or a reply sent from the transaction controller
 * 
 * @author Nick
 *
 */
abstract class TransactionControllerMessage extends P2PMessage {
	TransactionControllerMessage(Id destId) {
		super(destId);
	}
	
	TransactionControllerMessage(TransactionControllerMessage inReplyTo) {
		super(inReplyTo);
	}
}

/**
 * Message sent when publishing a transaction.
 * @author Nick
 *
 */
class PublishedTxn extends TransactionControllerMessage {
	private static final long serialVersionUID = 1L;
	// Updates (assumed to all be from same txn), at serialization
	// level VALUES_AND_TIDS
	final byte[] data;
	PublishedTxn(byte[] data, Id dest) {
		super(dest);
		this.data = data;
	}
	PublishedTxn(List<Update> txn, Id dest) {
		super(dest);
		ByteBufferWriter bbw = new ByteBufferWriter();
		for (Update u : txn) {
			bbw.addToBuffer(u, Update.SerializationLevel.VALUES_AND_TIDS);
		}
		data = bbw.getByteArray();
	}
}

/**
 * Requests that a transaction and its antecedents be sent to the specified peer, unless
 * that peer has already accepted or rejected them. The responses are
 * sent as a reply to the specified message. If withPrio is set, then the root
 * transaction's priority is returned and the antecedents are only requested if the
 * root transaction is trusted.
 * 
 * @author Nick
 *
 */
class RequestTxnForReconciliation extends TransactionControllerMessage {
	private static final long serialVersionUID = 1L;
	private final RequestTxnForReconciliation origMsg;
	private final TxnPeerID tid;
	private final AbstractPeerID pid;
	private final int recno;
	private final int mostRecentRecno;
	private final boolean withPrio;
	private final Set<TxnPeerID> acceptedButNotRecorded;
	
	RequestTxnForReconciliation(TxnPeerID tid, RequestTxnForReconciliation orig, Id dest) {
		super(dest);
		this.tid = tid;
		this.origMsg = orig;
		this.pid = orig.getPid();
		this.recno = orig.getRecno();
		this.mostRecentRecno = orig.getMostRecentRecno();
		withPrio = false;
		acceptedButNotRecorded = null;
	}
	
	RequestTxnForReconciliation(TxnPeerID tid, AbstractPeerID pid, int recno, int mostRecentRecno, Collection<TxnPeerID> acceptedButNotRecorded, boolean withPrio, Id dest) {
		super(dest);
		this.tid = tid;
		this.pid = pid;
		this.recno = recno;
		this.origMsg = this;
		this.mostRecentRecno = mostRecentRecno;
		this.withPrio = withPrio;
		this.acceptedButNotRecorded = new HashSet<TxnPeerID>(acceptedButNotRecorded);
	}
	
	TxnPeerID getTid() {
		return tid;
	}
	
	AbstractPeerID getPid() {
		return pid;
	}
	
	int getRecno() {
		return recno;
	}
	
	RequestTxnForReconciliation getOrigMsg() {
		return origMsg;
	}
	
	int getMostRecentRecno() {
		return mostRecentRecno;
	}
	
	boolean withPrio() {
		return withPrio;
	}
	
	boolean acceptedButNotRecorded(TxnPeerID tid) {
		return origMsg.acceptedButNotRecorded.contains(tid);
	}
}

/**
 * A reply to a {@link RequestTxnForReconciliation}
 * which indicates that the
 * specified transaction has already been accepted by the interested peer, or
 * if a reply to a message withPrio set to true, that the transaction is
 * not trusted.
 * 
 * @author Nick
 *
 */
class TxnNotNeeded extends TransactionControllerMessage {
	private static final long serialVersionUID = 1L;
	private final TxnPeerID tpi;
	TxnNotNeeded(TxnPeerID tpi, RequestTxnForReconciliation request) {
		super(request);
		this.tpi = tpi;
	}
	
	TxnPeerID getTpi() {
		return tpi;
	}
}

/**
 * A reply to a {@link RequestTxnForReconciliation}
 * which indicates that the transaction has already been
 * rejected
 * 
 * @author Nick
 *
 */
class TxnAlreadyRejected extends TransactionControllerMessage {
	private static final long serialVersionUID = 1L;
	private final TxnPeerID tpi;
	TxnAlreadyRejected(TxnPeerID tpi, RequestTxnForReconciliation request) {
		super(request);
		this.tpi = tpi;
	}
	
	
	TxnPeerID getTpi() {
		return tpi;
	}
	
}

/**
 * A reply to a {@link RequestTxnForReconciliation}
 * which indicates that an antecedent to the transaction has already been
 * rejected
 * 
 * @author Nick
 *
 */
class AntecedentTxnRejected extends TransactionControllerMessage {
	private static final long serialVersionUID = 1L;
	private final TxnPeerID tpi;
	AntecedentTxnRejected(TxnPeerID tpi, RequestTxnForReconciliation request) {
		super(request);
		this.tpi = tpi;
	}
	
	
	TxnPeerID getTpi() {
		return tpi;
	}	
}

/**
 * A reply to a {@link RequestTxnForReconciliation}
 * which gives the contents of the transaction and its priority, if requested.
 * 
 * @author Nick
 *
 */
class RetrievedTxn extends TransactionControllerMessage {
	private static final long serialVersionUID = 1L;
	private final int priority;	// Priority of transaction if in response to
								// a request withPrio, -1 otherwise
	private final byte[] txnContents;
	private final byte[] requestedAntecedents;
	
	RetrievedTxn(byte[] txnContents, Collection<TxnPeerID> requestedAntecedents, RequestTxnForReconciliation origRequest) {
		this(txnContents, requestedAntecedents, -1, origRequest);
	}
	
	RetrievedTxn(byte[] data, Collection<TxnPeerID> requestedAntecedents, int priority, RequestTxnForReconciliation origRequest) {
		super(origRequest);
		this.txnContents = data;
		this.priority = priority;
		ByteBufferWriter bbw = new ByteBufferWriter();
		for (TxnPeerID tid : requestedAntecedents) {
			bbw.addToBuffer(tid);
		}
		this.requestedAntecedents = bbw.getByteArray();
	}
	
	List<Update> getTxn(ISchemaIDBinding s) {
		ByteBufferReader bbr = new ByteBufferReader(s, txnContents);
		ArrayList<Update> txn = new ArrayList<Update>();
		while (! bbr.hasFinished()) {
			txn.add(bbr.readUpdate());
		}

		return txn;
	}
	
	int getTxnPriority() {
		return priority;
	}
	
	boolean hasPriority() {
		return priority != -1;
	}
	
	Collection<TxnPeerID> getRequestedAntecedents() {
		Collection<TxnPeerID> retval = new ArrayList<TxnPeerID>();
		ByteBufferReader bbr = new ByteBufferReader(null, requestedAntecedents);
		while (! bbr.hasFinished()) {
			retval.add(bbr.readTxnPeerID());
		}
		return retval;
	}
}

/**
 * Reply to {@link GetTxnForReconciliation} or {@link GetTxnForReconciliationWithPrio}
 * or (@link RequestTxn}
 * which indicates that the transaction could not be retrieved for the specified
 * reason.
 * 
 * @author netaylor
 *
 */
class CouldNotRetrieveTxn extends TransactionControllerMessage {
	private static final long serialVersionUID = 1L;
	private final TxnPeerID tid;
	private final String why;

	CouldNotRetrieveTxn(TxnPeerID tpi, String why, RequestTxnForReconciliation origRequest) {
		super(origRequest);
		tid = tpi;
		this.why = why;
	}

	CouldNotRetrieveTxn(TxnPeerID tpi, String why, RequestTxn origRequest) {
		super(origRequest);
		tid = tpi;
		this.why = why;
	}

	TxnPeerID getTpi() {
		return tid;
	}
	
	String getReason() {
		return why;
	}

	public String toString() {
		return "Could not retrieve transaction " + getTpi() + ": " + why;
	}
}

/**
 * Request the contents of a published transaction
 * 
 * @author netaylor
 *
 */
class RequestTxn extends TransactionControllerMessage {
	private static final long serialVersionUID = 1L;
	private final TxnPeerID tid;
	
	RequestTxn(TxnPeerID tid, Id dest) {
		super(dest);
		this.tid = tid;
	}
	
	public String toString() {
		return "RequestTxn(" + tid + ")";
	}
	
	TxnPeerID getTid() {
		return tid;
	}
}

/**
 * Reply to a {@link RequestTxn} that gives the contents
 * of a published transaction.
 * 
 * @author netaylor
 *
 */
class PublishedTxnIs extends TransactionControllerMessage {
	private static final long serialVersionUID = 1L;
	
	private final byte[] txnContents;
	
	PublishedTxnIs(byte[] txnContents, RequestTxn origRequest) {
		super(origRequest);
		this.txnContents = txnContents;
	}
	List<Update> getTxn(ISchemaIDBinding s) {
		ByteBufferReader bbr = new ByteBufferReader(s, txnContents);
		ArrayList<Update> txn = new ArrayList<Update>();
		while (! bbr.hasFinished()) {
			txn.add(bbr.readUpdate());
		}

		return txn;
	}
}
