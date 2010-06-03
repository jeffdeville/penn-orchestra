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
package edu.upenn.cis.orchestra.gui;

import java.awt.BorderLayout;
import java.awt.CardLayout;

import javax.swing.JPanel;

import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.gui.peers.PeersMgtPanel;
import edu.upenn.cis.orchestra.gui.provenance.ProvenanceViewer;
import edu.upenn.cis.orchestra.gui.schemas.RelationDataEditorFactory;

public class MainPanel extends JPanel 
{
	public static final long serialVersionUID = 1L;
	
	
	private final CardLayout _cardLayout = new CardLayout ();
	private final String CARD_LAYOUT_PEERSMGT_INDEX = "PeersMgt";
	private final String CARD_LAYOUT_PROVVIEWER_INDEX = "ProvViewer";
	
	
	private final PeersMgtPanel _peersMgtPanel;
	
	private final JPanel _rightPanel;
	
	private final OrchestraSystem _system;
	
	private ProvenanceViewer _provViewer = null;
	
	
	public MainPanel(OrchestraSystem system, String catalogPath)
	{
		super (new BorderLayout ());
		
		_system = system;
		_peersMgtPanel = new PeersMgtPanel (system, catalogPath);
		
		_rightPanel = new JPanel (_cardLayout);
		_rightPanel.add(_peersMgtPanel, CARD_LAYOUT_PEERSMGT_INDEX);
		
		add (_rightPanel, BorderLayout.CENTER);
			
	}
	
	public OrchestraSystem getSystem()
	{
		return _system;
	}
	
	protected PeersMgtPanel getPeersMgtPanel ()
	{
		return _peersMgtPanel;
	}
	
	public void showPeerInformation(Peer p) 
	{
		_cardLayout.show(_rightPanel, CARD_LAYOUT_PEERSMGT_INDEX);
		_peersMgtPanel.showPeerInfo(p);		
	}
	
	public void showPeerSchema(Peer p, Schema s) 
	{
		_cardLayout.show(_rightPanel, CARD_LAYOUT_PEERSMGT_INDEX);
		_peersMgtPanel.showPeerSchema(p, s);
	}
	
	public void showPeersNetwork() {
		_cardLayout.show(_rightPanel, CARD_LAYOUT_PEERSMGT_INDEX);
		_peersMgtPanel.showPeerNetwork();		
	}
	
	public void showPeerMappings(Peer p) {
		_cardLayout.show(_rightPanel, CARD_LAYOUT_PEERSMGT_INDEX);
		_peersMgtPanel.showPeerMappings(p);
	}
	
//	public void showConsole() 
//	{
//		_peersMgtPanel.showConsole();
//	}

	public void showProvenanceViewer(Peer p, Schema s) 
	{
		_cardLayout.show(_rightPanel, CARD_LAYOUT_PEERSMGT_INDEX);
		_peersMgtPanel.showProvenanceViewer(p, s);
	}

	public void showProvenanceViewer(Peer p) 
	{
		_cardLayout.show(_rightPanel, CARD_LAYOUT_PEERSMGT_INDEX);
		_peersMgtPanel.showProvenanceViewer(p);
	}
	
	public void showTransactionViewer(Peer p)
	{
		_cardLayout.show(_rightPanel, CARD_LAYOUT_PEERSMGT_INDEX);
		_peersMgtPanel.showTransactionViewerTab(p);		
	}
	
	public void startStoreServer() throws Exception {
		_system.startStoreServer();
	}
	
	public void clearStoreServer() throws IllegalStateException {
		_system.clearStoreServer();
	}
	
	public boolean storeServerRunning() {
		return (_system.storeServerRunning());
	}
	
	public void stopStoreServer() throws Exception {
		_system.stopStoreServer();
	}
}
