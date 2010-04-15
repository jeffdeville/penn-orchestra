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
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.event.ActionListener;
import java.awt.event.ComponentEvent;
import java.awt.event.ComponentListener;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.swing.BoxLayout;
import javax.swing.ButtonGroup;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.gui.mappings.graph.MappingGraph;
import edu.upenn.cis.orchestra.gui.provenance.ProvenanceViewer;
import edu.upenn.cis.orchestra.gui.schemas.RelationDataEditorFactory;
import edu.upenn.cis.orchestra.gui.schemas.RelationDataModel;
import edu.upenn.cis.orchestra.gui.schemas.SchemaMgtPanel;
import edu.upenn.cis.orchestra.gui.schemas.SchemaTransactionIntf;
import edu.upenn.cis.orchestra.gui.transactions.TransactionViewer;
import edu.upenn.cis.orchestra.gui.utils.VTextIcon;
import edu.upenn.cis.orchestra.reconciliation.DbException;

public class PeerTabSetPanel extends JPanel
implements PeerDetailInfoPanelObserver,
PeerTransactionsIntf
{
	public static final long serialVersionUID = 1L;

	private final String TAB_PEERINFO_TITLE = "Descriptions";
	private final String TAB_MAPPINGS_TITLE = "Mappings";
	private final String TAB_SCHEMA_TITLE = "Relations";//Schema ";
	private final String TAB_PROVVIEW_TITLE = "Provenance";
	private final String TAB_TRANSVIEWER_TITLE = "Transactions";

	private final OrchestraSystem _sys;
	private final Peer _p;	
	private final Set<String> _knownSchemas;

	private JPanel _topPanel;
	
	private boolean _splitPanePosSet;
	private MappingGraph _mappingGraph;
	private final Map<Mapping, JRadioButton> _mapMappBtn = new HashMap<Mapping, JRadioButton> ();

	private boolean _recomputeTransactions = false;

	final JTabbedPane _tabbedPane;

	private final IPeerBrowsingContext _context;
	private final RelationDataEditorFactory _dataEditFactory;
	
	private final String _catalogPath;

	public PeerTabSetPanel (final OrchestraSystem sys, final Peer p, IPeerBrowsingContext cx, RelationDataEditorFactory fact,
			final String catalogPath)
	{
		_context = cx;
		_dataEditFactory = fact;
		_catalogPath = catalogPath;
		setLayout (new BorderLayout ());

		_sys = sys;
		this._p = p;

		/** Create the tabbed pane **/
		_tabbedPane = new JTabbedPane ();

		_tabbedPane.setTabPlacement(JTabbedPane.LEFT);

		_knownSchemas = new HashSet<String> ();

		// Add a tab with peer detailed info (description, schemas 
		// list and description ...
		PeerDetailInfoPanel detailInfoPanel = new PeerDetailInfoPanel (_sys, _p, this, _context);
		detailInfoPanel.addObserver(this);
		//_tabbedPane.addTab(TAB_PEERINFO_TITLE, detailInfoPanel);
		addPane(_tabbedPane, detailInfoPanel, null, TAB_PEERINFO_TITLE, VTextIcon.ROTATE_DEFAULT);

		// Add a tab with the mappings information
		//_tabbedPane.addTab(TAB_MAPPINGS_TITLE, createMappingsPanel(_p));
		addPane(_tabbedPane, createMappingsPanel(_p), null, TAB_MAPPINGS_TITLE, VTextIcon.ROTATE_DEFAULT);

		// Add a tab for each schema but don't fill it right now
		//for (String scId : _p.getSchemaIds()) {
		for (Schema sc : _p.getSchemas()) {
			//_tabbedPane.addTab(TAB_SCHEMA_TITLE /*+ scId*/, new JPanel ());
			addPane(_tabbedPane, new JPanel(), null, getSchemaTabName(sc)/*scId + " " + TAB_SCHEMA_TITLE*/, VTextIcon.ROTATE_DEFAULT);
		}

		// Add a tab for the provenance viewer
//		if (!_sys.getRecMode())
			addPane(_tabbedPane, new ProvenanceViewer(_p, _sys, fact), null, TAB_PROVVIEW_TITLE, VTextIcon.ROTATE_DEFAULT);
//		else
//		{
			if (_p.isLocalPeer()){
				try {
					addPane(_tabbedPane, new TransactionViewer(_p, _sys), null, TAB_TRANSVIEWER_TITLE, VTextIcon.ROTATE_DEFAULT);
				} catch (DbException ex) {
					ex.printStackTrace();
					JOptionPane.showMessageDialog(this, "Error while loading the transactions viewer: " + ex.getMessage(), "Transaction viewer", JOptionPane.ERROR_MESSAGE);
				}
			}
//			}

		// When a schema is selected, check that it has already been 
		// loaded, otherwise load it
		_tabbedPane.addChangeListener(new ChangeListener ()
		{
			public void stateChanged(ChangeEvent evt) 
			{
				//int inx = _tabbedPane.getSelectedIndex();
				String tabName = _tabbedPane.getToolTipTextAt(_tabbedPane.getSelectedIndex());

				//if (schemaName.startsWith(TAB_SCHEMA_TITLE))
				if (tabName.endsWith(TAB_SCHEMA_TITLE))
				{
					//schemaName = schemaName.substring(TAB_SCHEMA_TITLE.length(), schemaName.length());
					tabName = getSchemaFromTabName(tabName);//schemaName.substring(0, schemaName.length() - TAB_SCHEMA_TITLE.length() - 1);
					showSchema (_p.getSchema(tabName));
				} else if (tabName.equals(TAB_PEERINFO_TITLE)) {
					_context.setBrowseState(IPeerBrowsingContext.BrowseState.PEER_VIEW);
				} else if (tabName.equals(TAB_MAPPINGS_TITLE)) {
					_context.setBrowseState(IPeerBrowsingContext.BrowseState.MAPPING_VIEW);
				} else if (tabName.equals(TAB_PROVVIEW_TITLE)) {
					_context.setBrowseState(IPeerBrowsingContext.BrowseState.PROV_VIEW);
				} else if (tabName.equals(TAB_TRANSVIEWER_TITLE)) {
					if (_recomputeTransactions)
						try {
							((TransactionViewer) _tabbedPane.getComponentAt(_tabbedPane.getSelectedIndex())).computeEpochs();
							_recomputeTransactions = false;
						} catch (DbException e) {
							JOptionPane.showMessageDialog(PeerTabSetPanel.this, "Error while loading the transactions viewer: " + e.getMessage(), "Transaction viewer", JOptionPane.ERROR_MESSAGE);
						}
						_context.setBrowseState(IPeerBrowsingContext.BrowseState.TRANS_VIEW);
				}
			}
		});

		add (_tabbedPane, BorderLayout.CENTER);
	}

	private void addPane(JTabbedPane pane, JPanel subPanel, Font font, String label, int rotate) {
		if (font != null)
			pane.setFont(font);

		VTextIcon textIcon = new VTextIcon(pane, label, rotate);
		pane.insertTab(null, textIcon, subPanel, label, 0);
		subPanel.setToolTipText(label);
	}

	private JPanel createMappingsPanel (Peer p)
	{
		final JPanel mappingsPanel = new JPanel (new BorderLayout ());

		if (p.getMappings().size()>0)
		{

			final JSplitPane splitMapp = new JSplitPane (JSplitPane.VERTICAL_SPLIT);

			ButtonGroup btnGrp = new ButtonGroup ();
			_topPanel = new JPanel ();
			_topPanel.setLayout(new BoxLayout(_topPanel,BoxLayout.Y_AXIS));

			JRadioButton firstRdio = null;

			for (Mapping mapp : p.getMappings())
			{
				final JRadioButton rdio = new JRadioButton (mapp.toString());
				firstRdio = firstRdio == null? rdio:firstRdio;
				btnGrp.add (rdio);
				final Mapping m = mapp;
				rdio.addActionListener(new ActionListener ()
					{
						public void actionPerformed(java.awt.event.ActionEvent arg0) 
						{
							if (rdio.isSelected())
								mappingDetailSelected(m);
						}
					
					});
				_topPanel.add (rdio);
				_mapMappBtn.put(mapp, rdio);
			}
			final JScrollPane scrollMappings = new JScrollPane (_topPanel);
			splitMapp.setLeftComponent (scrollMappings);
			
			final JScrollPane scrollGraph = new JScrollPane ();
			if (firstRdio != null)
			{
				_mappingGraph = new MappingGraph (p.getMappings().iterator().next());
				scrollGraph.setViewportView(_mappingGraph);
				_mappingGraph.applyLayoutOnFirstReveal();
			}
			splitMapp.setRightComponent(scrollGraph);

			mappingsPanel.add(splitMapp);

			if (firstRdio != null)
				firstRdio.setSelected(true);		


			addComponentListener(new ComponentListener ()
			{
				public void componentResized(ComponentEvent evt) 
				{
					if (!_splitPanePosSet)
					{
						_splitPanePosSet = true;
						splitMapp.setDividerLocation(0.15);
					}
				}

				public void componentHidden(ComponentEvent evt) {
				}
				public void componentMoved(ComponentEvent evt) {
				}
				public void componentShown(ComponentEvent evt) {
				}
			});
		}
		else
		{
			mappingsPanel.setLayout (new GridBagLayout ());
			JLabel noMappLabel = new JLabel ("No mapping defined for this peer yet!");
			noMappLabel.setFont(noMappLabel.getFont().deriveFont(Font.BOLD));
			GridBagConstraints cst = new GridBagConstraints ();
			cst.anchor = GridBagConstraints.CENTER;
			mappingsPanel.add (noMappLabel, cst);
		}

		return mappingsPanel;

	}


	private void mappingDetailSelected (final Mapping mapp)
	{
		_mappingGraph.resetMapping (mapp);
		_mappingGraph.revalidate();
	}


	public void showMapping (final Mapping mapp)
	{
		// TODO: scroll to visible
		_mapMappBtn.get(mapp).setSelected(true);
		mappingDetailSelected(mapp);
		showMappingsTab();	
	}



	public void mappingsBtnClicked(Peer p) {
		showMappingsTab();
		_context.setCurrentPeer(p, this);
	}

	public void schemaDetailsBtnClicked(Peer p, Schema s) {
		showSchema(s);
		_context.setCurrentPeer(p, this);
		_context.setCurrentSchema(s);
	}

	private int getTabIndex(String s) {
		for (int i = 0; i < _tabbedPane.getTabCount(); i++)
			if (_tabbedPane.getToolTipTextAt(i).equals(s))
				return i;
		return -1;
	}

	public void showMappingsTab ()
	{
		_tabbedPane.setSelectedIndex(getTabIndex(TAB_MAPPINGS_TITLE));
		_context.setBrowseState(IPeerBrowsingContext.BrowseState.MAPPING_VIEW);
	}

	public void showPeerInfoTab ()
	{
		_tabbedPane.setSelectedIndex(getTabIndex(TAB_PEERINFO_TITLE));
		_context.setBrowseState(IPeerBrowsingContext.BrowseState.PEER_VIEW);
	}

	public void showTransactionViewerTab ()
	{
		int indx = getTabIndex(TAB_TRANSVIEWER_TITLE);
		_tabbedPane.setSelectedIndex(indx);
		if (_recomputeTransactions)
			try {
				((TransactionViewer) _tabbedPane.getComponentAt(indx)).computeEpochs();
				_recomputeTransactions = false;
			} catch (DbException e) {
				JOptionPane.showMessageDialog(this, "Error while loading the transactions viewer: " + e.getMessage(), "Transaction viewer", JOptionPane.ERROR_MESSAGE);
			}
			_context.setBrowseState(IPeerBrowsingContext.BrowseState.TRANS_VIEW);
	}

	public void showProvenanceViewerTab ()
	{
		_tabbedPane.setSelectedIndex(getTabIndex(TAB_PROVVIEW_TITLE));
		_context.setBrowseState(IPeerBrowsingContext.BrowseState.PROV_VIEW);
	}

	public void showProvenanceViewerTab (Schema s)
	{
		int indx = getTabIndex(TAB_PROVVIEW_TITLE);
		_tabbedPane.setSelectedIndex(indx);
		((ProvenanceViewer) _tabbedPane.getComponentAt(indx)).setPeerAndSchema(_p, s);
	}

	public String getSchemaTabName(Schema s) {
		return getSchemaTabName(s.getSchemaId());
	}

	public String getSchemaTabName (String s)
	{
		if (_p.getSchemas().size() < 2)
			return TAB_SCHEMA_TITLE;
		else
			return s + " " + TAB_SCHEMA_TITLE;		
	}

	public String getSchemaFromTabName(String tabName) {
		if (_p.getSchemas().size() < 2)
			return _p.getSchemas().iterator().next().getSchemaId();
		else
			return tabName.substring(0, tabName.length() - TAB_SCHEMA_TITLE.length() - 1);
	}

	public void showSchema (Schema s)
	{
		if (!_knownSchemas.contains(s.getSchemaId()))
		{
//			_tabbedPane.setComponentAt(_tabbedPane.indexOfTab(TAB_SCHEMA_TITLE + s.getSchemaId()), new SchemaMgtPanel(_sys, _p, s));
			_tabbedPane.setComponentAt(getTabIndex(getSchemaTabName(s)), new SchemaMgtPanel(_sys, _p, s, this, _dataEditFactory, _catalogPath));
			_knownSchemas.add (s.getSchemaId());
		}
		_tabbedPane.setSelectedIndex(getTabIndex(getSchemaTabName(s)));
		_context.setBrowseState(IPeerBrowsingContext.BrowseState.SCHEMA_VIEW);
		_context.setCurrentSchema(s);
	}

	public boolean hasCurrentTransaction ()
	{
		boolean res=false;

		for (Component comp : _tabbedPane.getComponents())
			if (comp instanceof SchemaTransactionIntf)
			{
				res = res || ((SchemaTransactionIntf) comp).hasCurrentTransaction();
			}
		return res;
	}

	public void setRecomputeTransactions()
	{
		_recomputeTransactions = true;
	}

	public void setRefreshDataViews(boolean localEdits) {
		if (!localEdits)
		{
			for (String schema : _knownSchemas)
				((SchemaMgtPanel) _tabbedPane.getComponentAt(getTabIndex(getSchemaTabName(schema)))).refreshDataViews();
		}
//		if(! this._sys.getRecMode()) {
			int indx = getTabIndex(TAB_PROVVIEW_TITLE);
			RelationDataModel model = ((ProvenanceViewer) _tabbedPane.getComponentAt(indx)).getRelationDataModel();
			if (model != null)
				model.reset();
//		}

	}
}
