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

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ComponentEvent;
import java.awt.event.ComponentListener;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.swing.BorderFactory;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.jgraph.event.GraphSelectionEvent;
import org.jgraph.event.GraphSelectionListener;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.gui.console.ConsolePanel;
import edu.upenn.cis.orchestra.gui.graphs.GraphPanel;
import edu.upenn.cis.orchestra.gui.peers.graph.IPeerMapping;
import edu.upenn.cis.orchestra.gui.peers.graph.PeerGraph;
import edu.upenn.cis.orchestra.gui.peers.graph.PeerVertex;
import edu.upenn.cis.orchestra.gui.proql.ProQLEditorPanel;
import edu.upenn.cis.orchestra.gui.query.QueryEditorPanel;
import edu.upenn.cis.orchestra.gui.schemas.RelationDataEditorFactory;

public class PeersMgtPanel extends JPanel implements IPeerBrowsingContext {
	
	public static final long serialVersionUID = 1L;
	
	private JSplitPane _splitPane;
	private boolean _splitPanePosSet = false;
	
	private final JPanel _peerPropertiesPanel = new JPanel (); 
	
	// Tabbed pane: will contain one tab for peers network and 
	// one tab for the local peer.
	private final JTabbedPane _tabbedPane = new JTabbedPane ();
	private final static String TAB_PEERSNETW_TITLE = "All Peers";
	private final static String TAB_PEER_TITLE = "Peer ";
	private final static String TAB_CONSOLE = "Console";
	
	private final static String TAB_QUERY = "Query";
	final static String TAB_PROQL = "Provenance";

	
	/** The name of contained {@code JTabbedPane} (for testibility). */
	public static final String PEERS_MGT_TABBED_PANE = "PeersMgtPanelTabbedPane";
	// List of peers for which the tabs have been initialized
	private Set<String> _knownPeers = new HashSet<String>(); 
	
	// Properties components to update when a schema is selected
	private final PeerInfoSidePanel _peerInfoPanel = new PeerInfoSidePanel();
	private final SchemaInfoSidePanel _schemaInfoPanel = new SchemaInfoSidePanel ();
	private final MappingInfoSidePanel _mappingInfoPanel = new MappingInfoSidePanel ();
	private PeerGraph _peerGraph;
	
	private final RelationDataEditorFactory _dataEditFactory;
	
	private final List<PeersMgtPanelObserver> _observers = 
							new ArrayList<PeersMgtPanelObserver> (3);
	
	private final OrchestraSystem _system;
	
//	private static final int FONT_SIZE = 12;
	
	private BrowseState _view;
	private int _index;
	private Peer _peer;
	private PeerTransactionsIntf _peerTrans;
	private Schema _schema;
	private ConsolePanel _console;
	private QueryEditorPanel _query;
	private ProQLEditorPanel _provquery;
	
	private String _catalogPath;

	
	//private boolean _graphSplitPaneWasValid = false;
	
	public PeersMgtPanel (OrchestraSystem peers, String catalogPath)
	{
		_dataEditFactory = new RelationDataEditorFactory (peers);
		_system = peers;
		_catalogPath = catalogPath;
		
		initFrame ();
		
		addListeners();
	}
	
	private void addListeners() {
		addComponentListener(new ComponentListener ()
		{
			public void componentResized(ComponentEvent evt) 
			{
				if (!_splitPanePosSet)
				{
					_splitPanePosSet = true;
					_splitPane.setDividerLocation(0.7);
				}
			}
			
			public void componentHidden(ComponentEvent evt) {}
			public void componentMoved(ComponentEvent evt) {}
			public void componentShown(ComponentEvent evt) {}
		});
	}
	
	
	public SchemaInfoSidePanel getSchemaInfoPanel() {
		return _schemaInfoPanel;
	}
	
//	public ConsolePanel getConsole() {
//		return _console;
//	}
//	
//	public QueryEditorPanel getQueryEditor() {
//		return _query;
//	}
	
//	public void showConsole() {
//		_tabbedPane.setSelectedComponent(_console);
//	}
	
//	public void showQueryEditor() {
//		_tabbedPane.setSelectedComponent(_query);
//	}
	
	private void initFrame ()
	{
		setLayout (new BorderLayout ());
		_tabbedPane.setName(PEERS_MGT_TABBED_PANE);
		// Add a tab for the peers network
		_tabbedPane.addTab(TAB_PEERSNETW_TITLE, createPeerNetworkPanel(_system));

		_tabbedPane.addTab(TAB_PEER_TITLE + _system.getLocalPeer().getId(), new JPanel ());

		
		//_console = new ConsolePanel(_system);
		//_tabbedPane.addTab(TAB_CONSOLE, _console);
		
		//_query = new QueryEditorPanel(_system);
		//_tabbedPane.addTab(TAB_QUERY, _query);
		
		if (Config.queryDatalog()) {
			_query = new QueryEditorPanel(_system);
			_tabbedPane.addTab(TAB_QUERY, _query);
		}

		if (Config.useProQL()) {
			_provquery = new ProQLEditorPanel(_system);
			_tabbedPane.addTab(TAB_PROQL, _provquery);
		}
		
		// Add a change listener to the tabbed pane so that we can initialize 
		// the peer panels when needed
		_tabbedPane.addChangeListener(new ChangeListener ()
			{
				public void stateChanged(ChangeEvent evt) 
				{
					String tabTitle = _tabbedPane.getTitleAt(_tabbedPane.getSelectedIndex());
					if (tabTitle.startsWith(TAB_PEER_TITLE))
					{
						String peerId = tabTitle.substring(TAB_PEER_TITLE.length(), tabTitle.length());
						showPeer (_system.getPeer(peerId));
					}
				}
			});
		
		add (_tabbedPane, BorderLayout.CENTER);
	
		//_peerGraph.applyLayout();
		
	}
	
	private JPanel createPeerNetworkPanel (OrchestraSystem system)
	{
		final JPanel peersNetwPanel = new JPanel (new BorderLayout ());

		// Prepare the split pane
		_splitPane = new JSplitPane ();
		_splitPane.setResizeWeight(1.0);
		
		// Create the peers graph
		createPeerGraph (system);

		// Create the graph scroll pane
		JScrollPane scrollPGraph = new JScrollPane (_peerGraph);
		_peerGraph.applyLayoutOnFirstReveal();
		
		JPanel graphPanel = new JPanel (new BorderLayout ());
		graphPanel.add (scrollPGraph, BorderLayout.CENTER);		
		
		_splitPane.setLeftComponent(graphPanel);
		
		graphPanel.addComponentListener(new ComponentListener ()
				{
					public void componentHidden(ComponentEvent arg0) {}
					public void componentMoved(ComponentEvent arg0) {}
					public void componentResized(ComponentEvent arg0) {
						_peerGraph.center();
					}
					public void componentShown(ComponentEvent arg0) {}
				});
		
		// Right panel
		final JPanel rightPanel = createRightPanel ();
		_splitPane.setRightComponent(rightPanel);
		
		peersNetwPanel.add(_splitPane, BorderLayout.CENTER);
		
		
		return peersNetwPanel;
	}
	
	
	private JPanel createRightPanel ()
	{
		final JPanel rightPanel = new JPanel (new GridBagLayout());
		
		final JPanel graphToolsPanel = new GraphPanel (_peerGraph);
		graphToolsPanel.setBorder (BorderFactory.createTitledBorder("View controls"));

		graphToolsPanel.setPreferredSize(new Dimension(180, 180));
		
		GridBagConstraints cst = new GridBagConstraints ();
		cst.gridx = 0;
		cst.gridy = 0;
		cst.weightx = 0.4;
		//cst.weighty = 0.3;
		cst.anchor = GridBagConstraints.FIRST_LINE_START;
		cst.fill = GridBagConstraints.HORIZONTAL;
		cst.insets = new Insets (0,0,10,0);
		graphToolsPanel.setMinimumSize(new Dimension(180, 180));
		//cst.weighty = 1;
		rightPanel.add (graphToolsPanel, cst);
		
		// Add the panel used to show selected /schemas properties
		initPeerPropertiesPanel ();
		cst = new GridBagConstraints ();
		cst.gridx = 0;
		cst.gridy = 1;
		cst.weightx = 0.4;
		cst.weighty = 1;
		cst.anchor = GridBagConstraints.FIRST_LINE_START;
		cst.fill = GridBagConstraints.BOTH;
		rightPanel.add (_peerPropertiesPanel, cst);
		
		return rightPanel;
	}

	private void createPeerGraph (final OrchestraSystem system)
	{
		// Create the graph
		_peerGraph = new PeerGraph (system);
		
		// Add a selection listener to update detailed information on 
		// peers/schemas/mappings according to the current selection
		_peerGraph.getSelectionModel().addGraphSelectionListener(new GraphSelectionListener ()
		{
			public void valueChanged(GraphSelectionEvent evt) 
			{
				if (_peerGraph.getSelectionCount()!=1) {
					cleanPeerPropertiesPanel ();
					_peerGraph.setSelectedPeer(null);
					_peerGraph.setSelectedMapping(null);
                	setCurrentPeerAndSchema(null, null);
					for (PeersMgtPanelObserver obs : _observers)
						obs.selectionIsEmpty(PeersMgtPanel.this);					
				} else {
					if (_peerGraph.getSelectionCount()==1 
								&& _peerGraph.getSelectionCell() instanceof PeerVertex)
					{
						Peer p = ((PeerVertex) _peerGraph.getSelectionCell()).getPeer();
						Schema s = ((PeerVertex) _peerGraph.getSelectionCell()).getSchema();
						updatePeerPropertiesPanel (p, s);
						_peerGraph.setSelectedPeer((PeerVertex)_peerGraph.getSelectionCell());
						_peerGraph.setSelectedMapping(null);
                    	setCurrentPeerAndSchema(p, s);
					} else if (_peerGraph.getSelectionCount()==1 
								&& _peerGraph.getSelectionCell() instanceof IPeerMapping)
					{
						Mapping m = ((IPeerMapping) _peerGraph.getSelectionCell()).getMapping();
						updateMappingInfoPanel (m);
						_peerGraph.setSelectedMapping((IPeerMapping)_peerGraph.getSelectionCell());
						_peerGraph.setSelectedPeer(null);
                    	setCurrentPeerAndSchema(null, null);
						for (PeersMgtPanelObserver obs : _observers)
							obs.mappingWasSelected(PeersMgtPanel.this, m);
					}
				}
			}
		});


		// Add a mouse listener to allow observers to change their state 
		// according to the item on which the user double-clicks.
		_peerGraph.getSpecializedGraphUI().addMouseListener(new MouseListener ()
		{
			public void mouseClicked(MouseEvent evt) {
                if (evt.getClickCount()==2 && evt.getButton() == MouseEvent.BUTTON1)
				{
                    Object obj = _peerGraph.getSelectionCell();
                    if (obj != null 
                		&& obj instanceof PeerVertex) 
                    {
                    	PeerVertex cell = (PeerVertex) _peerGraph.getSelectionCell();
                    	setCurrentPeerAndSchema(cell.getPeer(), cell.getSchema());
                    	if (system.isLocalPeer(cell.getPeer())){
                    		showPeerSchema (cell.getPeer(), cell.getSchema());
                    	}
                    }

                    if (obj != null 
                		&& obj instanceof IPeerMapping) 
                    {
                    	IPeerMapping cell = (IPeerMapping) _peerGraph.getSelectionCell();
                    	if (system.isLocalPeer(cell.getPeer())){
                    		showPeerMapping(cell.getPeer (), cell.getMapping());
                    	}
                    }
				} else if (evt.getButton() == MouseEvent.BUTTON2 || evt.getButton() == MouseEvent.BUTTON3) {
                    Object obj = _peerGraph.getSelectionCell();
                    if (obj != null 
                		&& obj instanceof PeerVertex) 
                    {
                    	PeerVertex cell = (PeerVertex) _peerGraph.getSelectionCell();
                    	setCurrentPeerAndSchema(cell.getPeer(), cell.getSchema());
                    	
                    	if (system.isLocalPeer(cell.getPeer())){
                    		for (PeersMgtPanelObserver obs : _observers)
                    			obs.peerContextMenu(PeersMgtPanel.this, cell.getPeer(), cell.getSchema(), getPeerDetailPanel(cell.getPeer()), PeersMgtPanel.this, evt.getX(), evt.getY());
                    	}
                    }
				} 

			}
			public void mouseEntered(MouseEvent evt) {}
			public void mouseExited(MouseEvent evt) {}
			public void mousePressed(MouseEvent evt) {}
			public void mouseReleased(MouseEvent evt) {}
		});
		
	}
	
	
	private void cleanPeerPropertiesPanel ()
	{
		_peerInfoPanel.setVisible(false);
		_schemaInfoPanel.setVisible(false);
		_mappingInfoPanel.setVisible(false);
	}
	
	private void initPeerPropertiesPanel ()
	{
		_peerPropertiesPanel.setLayout(new GridBagLayout());
		
		/*
		final JLabel lblTitle = new JLabel ("Properties");
		lblTitle.setFont(lblTitle.getFont().deriveFont(Font.BOLD));		
		GridBagConstraints cst = new GridBagConstraints ();
		cst.gridx = 0;
		cst.gridy = 0;
		cst.anchor = GridBagConstraints.FIRST_LINE_START;
		cst.weightx = 0.1;
		_peerPropertiesPanel.add (lblTitle, cst);*/
				
		GridBagConstraints cst = new GridBagConstraints ();
		cst.gridx = 0;
		cst.gridy = 1;
		cst.anchor = GridBagConstraints.FIRST_LINE_START;
		cst.fill = GridBagConstraints.BOTH;
		cst.weightx = 0.1;
		cst.weighty = 0.1; 
		_peerPropertiesPanel.add(_peerInfoPanel, cst);
		_peerPropertiesPanel.add (_mappingInfoPanel, cst);

		cst = new GridBagConstraints ();
		cst.gridx = 0;
		cst.gridy = 2;
		cst.anchor = GridBagConstraints.FIRST_LINE_START;
		cst.fill = GridBagConstraints.BOTH;
		cst.weightx = 0.1;
		cst.weighty = 0.1;
		_peerPropertiesPanel.add(_schemaInfoPanel, cst);
	}
	
	
	private void updatePeerPropertiesPanel (Peer p, Schema s)
	{
		_mappingInfoPanel.setVisible(false);
		_peerInfoPanel.setPeer(p);
		_peerInfoPanel.setVisible(true);		
		_schemaInfoPanel.setSchema(s);
		_schemaInfoPanel.setVisible(true);
	}
	
	private void updateMappingInfoPanel (Mapping m)
	{
		_peerInfoPanel.setVisible(false);
		_schemaInfoPanel.setVisible(false);
		
		_mappingInfoPanel.setMapping(m);
		_mappingInfoPanel.setVisible(true);
	}
	

	
	
	public void showPeer (final Peer peer)
	{
		if (!_knownPeers.contains(peer.getId()))
		{
			final PeerTabSetPanel panel = new PeerTabSetPanel (_system, peer, this, _dataEditFactory, _catalogPath);
			_tabbedPane.setComponentAt(_tabbedPane.indexOfTab(TAB_PEER_TITLE + peer.getId()), panel);
			_knownPeers.add(peer.getId());
		}
		//setBrowseState(BrowseState.PEER_VIEW);
		int panelIndx = _tabbedPane.indexOfTab(TAB_PEER_TITLE + peer.getId());
		_tabbedPane.setSelectedIndex(panelIndx);
		setCurrentPeer(peer, getPeerDetailPanel(peer));
		
		if (getBrowseState() == BrowseState.MAPPING_VIEW)
			getPeerDetailPanel(peer).showMappingsTab();
		else if (getBrowseState() == BrowseState.SCHEMA_VIEW)
			getPeerDetailPanel(peer).showSchema(getPeerSchema(getCurrentPeer()));
		else if (getBrowseState() == BrowseState.PROV_VIEW)
			getPeerDetailPanel(peer).showProvenanceViewerTab(getPeerSchema(peer));
		else if (getBrowseState() == BrowseState.TRANS_VIEW)
			getPeerDetailPanel(peer).showTransactionViewerTab();
		else
			getPeerDetailPanel(peer).showPeerInfoTab ();
	}

	private PeerTabSetPanel getPeerDetailPanel (Peer p)  
	{
		Component comp = _tabbedPane.getComponentAt(_tabbedPane.indexOfTab(TAB_PEER_TITLE + p.getId()));
		if (comp instanceof PeerTabSetPanel) 
			return (PeerTabSetPanel) _tabbedPane.getComponentAt(_tabbedPane.indexOfTab(TAB_PEER_TITLE + p.getId()));
		else
			return null;
	}
	
	public Schema getPeerSchema(Peer peer) {
		Schema schema;
		if (peer != null && getCurrentSchema() != null && peer.getSchemaIds().contains(getCurrentSchema().getSchemaId()))
			schema = getCurrentSchema();
		else
			schema = peer.getSchemas().iterator().next();
		

		return schema;
	}
	
	public void showPeerInfo (final Peer peer)
	{
		showPeer (peer);
		getPeerDetailPanel(peer).showPeerInfoTab ();
		setCurrentSchema(getPeerSchema(peer));
	}
	
	public void showPeerSchema (final Peer peer, final Schema schema)
	{
		showPeer(peer);
		getPeerDetailPanel(peer).showSchema(schema);
		setCurrentSchema(schema);
	}
	
	public void showPeerMappings (final Peer peer)
	{
		showPeer(peer);
		getPeerDetailPanel(peer).showMappingsTab();
		setCurrentSchema(getPeerSchema(peer));
	}
	
	public void showProvenanceViewer (final Peer peer)
	{
		showPeer(peer);
		getPeerDetailPanel(peer).showProvenanceViewerTab();
	}

	public void showProvenanceViewer (final Peer peer, final Schema s)
	{
		showPeer(peer);
		getPeerDetailPanel(peer).showProvenanceViewerTab(s);
	}

	public void showPeerMapping (final Peer peer, final Mapping mapping)
	{
		showPeer (peer);
		getPeerDetailPanel(peer).showMapping(mapping);
		setCurrentSchema(getPeerSchema(peer));
	}

	public void showPeerNetwork ()
	{
		_tabbedPane.setSelectedIndex(_tabbedPane.indexOfTab(TAB_PEERSNETW_TITLE));
	}
	
	public void showTransactionViewerTab (final Peer p)
	{
		showPeer(p);
		getPeerDetailPanel(p).showTransactionViewerTab();
	}
	
	public void addObserver (PeersMgtPanelObserver obs)
	{
		_observers.add(obs);
	}
	
	public void removeObserver (PeersMgtPanelObserver obs)
	{
		_observers.remove(obs);
	}


	public Peer getCurrentPeer() {
		return _peer;
	}
	public Schema getCurrentSchema() {
		return _schema;
	}
	
	public void setCurrentPeerAndSchema(Peer p, Schema s) {
		_peer = p;		
		_schema = s;
		for (PeersMgtPanelObserver obs : _observers)
			obs.peerWasSelected(this, getCurrentPeer(), getCurrentSchema(), getPeerTransactionsIntf());
	}
	
	public void setCurrentSchema(Schema s) {
		_schema = s;
		for (PeersMgtPanelObserver obs : _observers)
			obs.peerWasSelected(this, getCurrentPeer(), getCurrentSchema(), getPeerTransactionsIntf());
	}
	public void setCurrentPeer(Peer p, PeerTransactionsIntf peerTrans) {
		_peer = p;
		if (peerTrans != null)
			_peerTrans = peerTrans;
		for (PeersMgtPanelObserver obs : _observers)
			obs.peerWasSelected(this, p, getPeerSchema(p), getPeerTransactionsIntf());
	}
	
	public BrowseState getBrowseState() {
		return _view;
	}

	public void setBrowseState(BrowseState b) {
		_view = b;
	}
	
	public void mimic(PeersMgtPanel panel) {
		_tabbedPane.setSelectedIndex(panel._tabbedPane.getSelectedIndex());
//		setCurrentPeer(panel.getCurrentPeer(), panel.getPeerTransactionsIntf());
//		setCurrentSchema(panel.getCurrentSchema());
	}
	
	public int getIndex() {
		return _index;
	}
	public void setIndex(int index) {
		_index = index;
	}
	
	public PeerTransactionsIntf getPeerTransactionsIntf() {
		return _peerTrans;
	}
	
	public void setRefreshTransactions() {
		// If one of the transaction views has been updated, make sure we refresh ALL
		for (int i = 0; i < _tabbedPane.getTabCount(); i++)
			if (_tabbedPane.getTitleAt(i).startsWith(TAB_PEER_TITLE) && _tabbedPane.getComponentAt(i) instanceof PeerTabSetPanel)
				((PeerTabSetPanel)_tabbedPane.getComponentAt(i)).setRecomputeTransactions();
	}

	
}
