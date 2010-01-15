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

import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.Collections;

class ReplyHolder<T> implements Iterable<P2PMessage> {
	private int numRepliesRemaining;
	private Map<T,P2PMessage> replies;

	ReplyHolder(int numReplies) {
		numRepliesRemaining = numReplies;
		replies = new HashMap<T,P2PMessage>();
	}
	
	synchronized boolean receiveReply(T key, P2PMessage reply) {
		if (replies.containsKey(key)) {
			return false;
		}
		
		replies.put(key, reply);
		--numRepliesRemaining;
		
		if (numRepliesRemaining <= 0) {
			notifyAll();
		}
		
		return true;
	}
	
	void waitUntilFinished() throws InterruptedException {
		waitUntilFinished(null);
	}
	
	synchronized void waitUntilFinished(P2PStore.MessageProcessorThread processor) throws InterruptedException {
		if (processor != null) {
			processor.startAwaitingReply();
		}
		while (numRepliesRemaining > 0) {
			wait();
		}
		if (processor != null) {
			processor.stopAwaitingReply();
		}
	}
	
	synchronized int numRepliesRemaining() {
		return numRepliesRemaining;
	}
	
	synchronized P2PMessage getReply(T key) {
		return replies.get(key);
	}
	
	void reset(int numReplies) {
		replies.clear();
		numRepliesRemaining = numReplies;
	}

	public Iterator<P2PMessage> iterator() {
		return Collections.unmodifiableCollection(replies.values()).iterator();
	}
	
	public Set<T> getKeys() {
		return Collections.unmodifiableSet(replies.keySet());
	}
}
