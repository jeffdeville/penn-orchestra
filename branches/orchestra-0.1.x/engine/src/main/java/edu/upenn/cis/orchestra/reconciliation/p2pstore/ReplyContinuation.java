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

interface ReplyContinuation {
	/**
	 * Method to call when a reply comes in. If more retries are still possible,
	 * failures (as defined when the message was sent) will not be sent here, and the
	 * message will be resent automatically. May also receive a
	 * {@link ReplyTimeout} if the last retry times out.
	 * 
	 * @param m The message received in reply
	 * @param mpt TODO
	 */
	void processReply(P2PMessage m, MessageProcessorThread mpt);
	
	
	/**
	 * @return	True if the continuation is not needed anymore, false
	 * if additional replies are expected.
	 */
	boolean isFinished();
}