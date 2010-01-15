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

import javax.swing.JComponent;

import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Schema;

public interface PeersMgtPanelObserver 
{
	public abstract void peerWasSelected(PeersMgtPanel panel, Peer p, Schema s, PeerTransactionsIntf peerTransIntf);
	public abstract void peerContextMenu(PeersMgtPanel panel, Peer p, Schema s, PeerTransactionsIntf peerTransIntf, JComponent parent, int x, int y);
	public abstract void mappingWasSelected(PeersMgtPanel panel, Mapping m);
	public abstract void selectionIsEmpty(PeersMgtPanel panel);
}
