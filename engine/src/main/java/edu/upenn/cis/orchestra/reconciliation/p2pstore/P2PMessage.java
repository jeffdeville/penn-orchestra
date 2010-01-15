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

import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.NodeHandle;

abstract class P2PMessage implements Message {
	private static final long serialVersionUID = 1L;
	// Node that is sending the message. Do not set this explicitly,
	// it is set when the message is sent.
	private NodeHandle from = null;
	// Used to generate message IDs
	static long nextMessageId = Long.MIN_VALUE + 1;
	
	// Id assigned to the message
	private final long messageId = getNextMessageId();
	// Message is a reply to the specified message id (or Long.MIN_VALUE if it is not a reply)
	public static final long NOT_REPLY = Long.MIN_VALUE;
	private final long inReplyTo;
	
	/*
	 * The message's destination. If both the Id and the NodeHandle are non-null,
	 * then the NodeHandle is used as a routing hint. If both are null, the message
	 * cannot be sent.
	 */
	private final Id destId;
	private final NodeHandle destNH;

	P2PMessage(Id destId) {
		this(destId,null);
	}
	
	P2PMessage(NodeHandle destNH) {
		this(null,destNH);
	}
	
	P2PMessage(Id id, NodeHandle nh) {
		this.destId = id;
		this.destNH = nh;
		inReplyTo = NOT_REPLY;
		if (destId == null && destNH == null) {
			throw new IllegalArgumentException("Cannot create a message with neither a destination Id nor a destination NodeHandle");
		}
	}
	
	P2PMessage(boolean allowNulls) {
		this.destId = null;
		this.destNH = null;
		this.inReplyTo = NOT_REPLY;
	}
	
	P2PMessage(P2PMessage inReplyTo) {
		this.destId = null;
		this.destNH = inReplyTo.from;
		this.inReplyTo = inReplyTo.messageId;
	}
					
	/**
	 * Get the ID of this message
	 * 
	 * @return
	 */
	final long getMessageId() {
		return messageId;
	}
	
	/**
	 * Get the id of the message this message is a reply to.
	 * 
	 * @return		The id of the original message,
	 * 				or <code>Long.MIN_VALUE</code> if the message is not a reply
	 */
	final long getRequestMessageId() {
		return inReplyTo;
	}
			
	/**
	 * Get the next message ID for the sending node/JVM
	 * 
	 * @return An ID that is unique for the sending node
	 */
	private static synchronized long getNextMessageId() {
		return nextMessageId++;
	}
	
	public final int getPriority() {
		return Message.MEDIUM_HIGH_PRIORITY;
	}
	
	final boolean isReply() {
		return inReplyTo !=NOT_REPLY;
	}
	
	void send(Endpoint e) {
		this.from = e.getLocalNodeHandle();
		e.route(destId, this, destNH);
	}
}

class ReplySuccess extends P2PMessage {
	private static final long serialVersionUID = 1L;
	ReplySuccess(P2PMessage inReplyTo) {
		super(inReplyTo);
	}
}

class ReplyFailure extends P2PMessage {
	private static final long serialVersionUID = 1L;
	String msg;
	ReplyFailure(String msg, P2PMessage inReplyTo) {
		super(inReplyTo);
		this.msg = msg;
	}
		
	public String toString() {
		return "ReplyFailure (" + msg + ")";
	}
}

class ReplyException extends P2PMessage {
	static final long serialVersionUID = 1L;

	final String message;
	final Exception e;
	
	ReplyException(String message, P2PMessage inReplyTo) {
		this(message, null, inReplyTo);
	}
	
	ReplyException(String message, Exception e, P2PMessage inReplyTo) {
		super(inReplyTo);
		this.message = message;
		this.e = e;
	}
	
	public String toString() {
		return "ReplyException (" + message + ")" + (e == null ? "" : ": " + e);
	}
}

class ReplyTimeout extends P2PMessage {
	private static final long serialVersionUID = 1L;
	ReplyTimeout(P2PMessage inReplyTo) {
		super(inReplyTo);
	}
}

class CheckForTxn extends P2PMessage {
	private static final long serialVersionUID = 1L;
	final TxnPeerID toCheck;
	CheckForTxn(RequestTxnForReconciliation request, TxnPeerID toCheck) {
		super(request);
		this.toCheck = toCheck;
	}
}
