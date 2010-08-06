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
package edu.upenn.cis.orchestra.reconciliation;

import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore.USException;

public interface TransactionDecisions {
	/**
	 * Determine if the viewing peer has accepted a transaction
	 * 
	 * @param txn			The ID of the transaction and of the peer that inserted it
	 * @return 				<code>true</code> if the viewing peer has accepted it,
	 * 						<code>false</code> otherwise.
	 * @throws Exception
	 */
	boolean hasRejectedTxn(TxnPeerID tpi) throws USException;

	/**
	 * Determine if the viewing peer has rejected a transaction.
	 * 
	 * @param txn			The ID of the transaction and of the peer that inserted it
	 * @return 				<code>true</code> if the viewing peer has accepted it,
	 * 						<code>false</code> otherwise.
	 * @throws Exception
	 */
	boolean hasAcceptedTxn(TxnPeerID tpi) throws USException;
	
}