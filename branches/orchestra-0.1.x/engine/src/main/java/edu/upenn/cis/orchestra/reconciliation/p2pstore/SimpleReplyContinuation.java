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


import edu.upenn.cis.orchestra.reconciliation.p2pstore.P2PStore.MessageProcessorThread;


class SimpleReplyContinuation<T> implements ReplyContinuation {
	private final ReplyHolder<? super T> replies;
	private final T key;
	private boolean finished;
	
	SimpleReplyContinuation(T key, ReplyHolder<? super T> replies) {
		this.key = key;
		this.replies = replies;
		finished = false;
	}
	
	
	public boolean isFinished() {
		return finished;
	}

	public void processReply(P2PMessage m, MessageProcessorThread mpt) {
		replies.receiveReply(key, m);
		finished = true;
	}

}
