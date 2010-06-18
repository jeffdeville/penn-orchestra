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
package edu.upenn.cis.orchestra.gui.peers;

import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Schema;

/**
 * Interface for peer context when browsing
 * 
 * @author zives
 *
 */
public interface IPeerBrowsingContext {
	public Peer getCurrentPeer();
	public PeerTransactionsIntf getPeerTransactionsIntf ();
	public Schema getCurrentSchema();
	
	public void setCurrentSchema(Schema s);
	public void setCurrentPeer(Peer p, PeerTransactionsIntf peerTrans);
	
	public enum BrowseState {PEER_VIEW, MAPPING_VIEW, SCHEMA_VIEW, TRANS_VIEW, PROV_VIEW};
	
	public BrowseState getBrowseState();
	public void setBrowseState(BrowseState b);
	
	public void setRefreshTransactions();
	
	/**
	 * Should return the index of a pane within a given view: if there are multiple
	 * peer views, etc., it should be the 0th, 1st, etc.
	 * 
	 * @return
	 */
	public int getIndex();
	public void setIndex(int index);
}
