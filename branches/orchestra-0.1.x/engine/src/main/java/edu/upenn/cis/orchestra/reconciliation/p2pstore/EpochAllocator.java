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

import com.sleepycat.je.Transaction;

import rice.p2p.commonapi.Id;

class EpochAllocator {
	static final int FIRST_EPOCH = 0;
	// The last epoch this epoch allocator knows about; this may not be the last one
	// published, but the last epoch published cannot be less than this, making it a
	// good place to start trying to publish.
	private int lastEpoch;
	private P2PStore store;
	private Id epochAllocatorId;
	
	EpochAllocator(P2PStore store, Id epochAllocatorId) {
		this.store = store;
		this.lastEpoch = FIRST_EPOCH - 1;
		this.epochAllocatorId = epochAllocatorId;
	}
		
	synchronized P2PMessage processMessage(EpochAllocatorMessage eam, Transaction t, P2PStore.MessageProcessorThread processor) {
		if (eam instanceof RequestLastEpoch) {
			 return new LastEpochIs(lastEpoch, (RequestLastEpoch) eam);
		} else if (eam instanceof LastEpochIs) {
			LastEpochIs lea = (LastEpochIs) eam;
			if (lea.lastEpoch > lastEpoch) {
				lastEpoch = lea.lastEpoch;
			}
			return null;
		} else {
			return new ReplyException("Recevied unexpected Epoch Allocator message: " + eam, eam);
		}
	}
	
	synchronized void lastEpochIs(int epoch) {
		if (epoch > lastEpoch) {
			lastEpoch = epoch;
		}
	}
	
	int getLastEpoch() throws InterruptedException, UnexpectedReply {
		ReplyHolder<Integer> replies = new ReplyHolder<Integer>(1);
		P2PMessage request = new RequestLastEpoch(epochAllocatorId);
		store.sendMessageAwaitReply(request, new SimpleReplyContinuation<Integer>(1,replies), LastEpochIs.class);
		replies.waitUntilFinished();
		
		P2PMessage m = replies.getReply(1);
		if (m instanceof LastEpochIs) {
			LastEpochIs lei = (LastEpochIs) m;
			synchronized (this) {
				if (lei.lastEpoch > lastEpoch) {
					lastEpoch = lei.lastEpoch;
				}
				return lastEpoch;
			}
		} else {
			throw new UnexpectedReply(request,m);
		}
	}
	
	void shareLastEpoch() {
		int lastEpoch;
		synchronized (this) {
			lastEpoch = this.lastEpoch;
		}
		store.sendMessage(new LastEpochIs(lastEpoch, epochAllocatorId));
	}
}