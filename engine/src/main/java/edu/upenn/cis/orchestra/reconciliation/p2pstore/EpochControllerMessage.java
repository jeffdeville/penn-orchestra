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

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import rice.p2p.commonapi.Id;

import edu.upenn.cis.orchestra.datamodel.ByteBufferReader;
import edu.upenn.cis.orchestra.datamodel.ByteBufferWriter;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;

/**
 * Superclass of all messages sent to, and replies sent from,
 * an EpochController
 * 
 * @author Nick
 *
 */
class EpochControllerMessage extends P2PMessage {
	EpochControllerMessage(Id destId) {
		super(destId);
	}
	
	EpochControllerMessage(EpochControllerMessage ecm) {
		super(ecm);
	}

	private static final long serialVersionUID = 1L;
}
	
/**
 * Request for the contents of an epoch
 * 
 * @author Nick
 *
 */
class RequestPublishedEpoch extends EpochControllerMessage {
	private static final long serialVersionUID = 1L;
	final int epoch;
	RequestPublishedEpoch(int epoch, Id dest) {
		super(dest);
		this.epoch = epoch;
	}
}

/**
 * A message sent from a publishing peer to an epoch controller
 * to give it the entire list of transactions for that epoch.
 * 
 * Also sent as a reply to {@link RequestPublishedEpoch} if the epoch exists
 * and is finished.
 * 
 * @author Nick
 *
 */
class PublishEpoch extends EpochControllerMessage {
	private static final long serialVersionUID = 1L;
	final int epoch;
	final byte[] tids;
	PublishEpoch(int epoch, byte[] tids, RequestPublishedEpoch rpe) {
		super(rpe);
		this.tids = tids;
		this.epoch = epoch;
	}
	PublishEpoch(int epoch, Collection<TxnPeerID> tids, Id dest) {
		super(dest);
		ByteBufferWriter bbw = new ByteBufferWriter();
		for (TxnPeerID tid : tids) {
			bbw.addToBuffer(tid);
		}
		this.tids = bbw.getByteArray();
		this.epoch = epoch;
	}
	List<TxnPeerID> getTids() {
		// Only need schema to deserialize updates and tuples
		ByteBufferReader bbr = new ByteBufferReader(null, tids, 0, tids.length);
		LinkedList<TxnPeerID> tids = new LinkedList<TxnPeerID>();
		
		while (! bbr.hasFinished()) {
			tids.add(bbr.readTxnPeerID());
		}
		
		return tids;
	}
}

/**
 * A reply to {@link PublishEpoch}, indicating that the epoch could not be published
 * because it already exists. It also helpfully includes the most recent epoch number
 * issued by this epoch controller.
 * 
 * @author netaylor
 *
 */
class CouldNotPublishEpoch extends EpochControllerMessage {
	private static final long serialVersionUID = 1L;
	final int lastEpoch;
	CouldNotPublishEpoch(int lastEpoch, PublishEpoch pe) {
		super(pe);
		this.lastEpoch = lastEpoch;
	}
}

/**
 * A reply to {@link RequestPublishedEpoch}, indicating that the epoch has not been
 * published.
 * 
 * @author netaylor
 *
 */
class EpochNotPublished extends EpochControllerMessage {
	private static final long serialVersionUID = 1L;	
	EpochNotPublished(RequestPublishedEpoch rpe) {
		super(rpe);
	}
}

class RequestMostRecentEpoch extends EpochControllerMessage {
	private static final long serialVersionUID = 1L;
	RequestMostRecentEpoch(Id dest) {
		super(dest);
	}
}

class MostRecentEpochIs extends EpochControllerMessage {
	private static final long serialVersionUID = 1L;
	final int mostRecentEpoch;
	
	MostRecentEpochIs(int mostRecentEpoch, RequestMostRecentEpoch request) {
		super(request);
		this.mostRecentEpoch = mostRecentEpoch;
	}
}