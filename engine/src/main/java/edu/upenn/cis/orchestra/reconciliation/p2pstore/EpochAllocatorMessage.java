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

import rice.p2p.commonapi.Id;

/**
 * Superclass of all messages that get routed to the EpochAllocator
 * running at a particular peer, and replies that get sent back
 * from the epoch allocator
 * 
 * 
 * @author Nick
 *
 */
abstract class EpochAllocatorMessage extends P2PMessage {
	EpochAllocatorMessage(Id id) {
		super(id);
	}

	EpochAllocatorMessage(EpochAllocatorMessage m) {
		super(m);
	}

	private static final long serialVersionUID = 1L;
}

/**
 * A request for the last epoch that the epoch allocator knows about.
 * All epochs prior to this have been published. More recent epochs may
 * me published, and they may not be.
 * 
 * 
 * @author Nick
 *
 */
class RequestLastEpoch extends EpochAllocatorMessage {
	private static final long serialVersionUID = 1L;
	RequestLastEpoch(Id id) {
		super(id);
	}
}

/**
 * A reply to a {@link RequestLastEpoch} message, it tells what the
 * last epoch the epoch allocator knows about it. It is also sent by an
 * epoch controller when it grants an epoch.
 * 
 * @author Nick
 *
 */
class LastEpochIs extends EpochAllocatorMessage {
	private static final long serialVersionUID = 1L;
	final int lastEpoch;
	LastEpochIs(int lastEpoch, RequestLastEpoch rle) {
		super(rle);
		this.lastEpoch = lastEpoch;
	}
	LastEpochIs(int lastEpoch, Id dest) {
		super(dest);
		this.lastEpoch = lastEpoch;
	}
}