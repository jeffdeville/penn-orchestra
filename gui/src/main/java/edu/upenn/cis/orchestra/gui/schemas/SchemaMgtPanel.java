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
package edu.upenn.cis.orchestra.gui.schemas;

import java.awt.BorderLayout;
import java.awt.CardLayout;
import java.awt.Component;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTextArea;
import javax.swing.JTree;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.filechooser.FileNameExtensionFilter;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.MutableTreeNode;
import javax.swing.tree.TreePath;

import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationContext;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.datamodel.Update;
import edu.upenn.cis.orchestra.datamodel.exceptions.RelationNotFoundException;
import edu.upenn.cis.orchestra.gui.peers.PeerCommands;
import edu.upenn.cis.orchestra.gui.peers.PeerTransactionsIntf;
import edu.upenn.cis.orchestra.gui.schemas.browsertree.SchemaBrowserRelationNode;
import edu.upenn.cis.orchestra.gui.schemas.browsertree.SchemaBrowserTransactionViewNode;
import edu.upenn.cis.orchestra.gui.schemas.browsertree.SchemaBrowserTree;
import edu.upenn.cis.orchestra.gui.schemas.graph.SchemaGraph;
import edu.upenn.cis.orchestra.gui.schemas.graph.SchemaGraphObserver;

public class SchemaMgtPanel extends JPanel 
			implements SchemaGraphObserver,
						SchemaTransactionIntf
{
	public static final long serialVersionUID = 1L;
	
	// CardLayout used to switch between panels on the right-hand side
	private final CardLayout _cardLayout = new CardLayout ();
	private final JPanel _detailsPanel = new JPanel (_cardLayout);
	private final Set<String> _knownRels = new HashSet<String> ();
	private static final String CARD_RELATION_PREFIX = "REL";
	private static final String CARD_SCHOVERVIEW_PREFIX = "SCHEMAOVERVW";
	private static final String CARD_TRANSACTION_ID = "CURRTRANS";
	
	private boolean _transSelected = false;
	
	static final String TREE_SCHEMAOVERVIEW_LABEL = "All relations";
	
	
	private final OrchestraSystem _sys;
	private final Peer _p;
	private final Schema _sc;
	private final PeerTransactionsIntf _peerTransIntf;
	
	private final RelationDataEditorFactory _dataEditFactory;
	
	private JPanel currTransPanel = null;
	private JFileChooser _fChooser;
	
	private SchemaGraph _schemaGraph;
	
	public SchemaMgtPanel (final OrchestraSystem sys, 
								final Peer p, 
								final Schema sc,
								final PeerTransactionsIntf peerTransIntf,
								final RelationDataEditorFactory dataEditFactory, 
								final String catalogPath)
	{
		super (new BorderLayout ());
		_sys = sys;
		_p = p;
		_sc = sc;
		_peerTransIntf = peerTransIntf;
		_dataEditFactory = dataEditFactory;
		final JSplitPane splitPane = createJSplitPane ();
		add (splitPane, BorderLayout.CENTER);
		_fChooser = new JFileChooser(catalogPath);
		FileNameExtensionFilter filter = new FileNameExtensionFilter(
				"Transaction export", "trans");
		_fChooser.setFileFilter(filter);		
	}

	private JSplitPane createJSplitPane ()
	{
		final JSplitPane splitPane = new JSplitPane ();		
		splitPane.setLeftComponent(createOptionsPanel());		
		splitPane.setRightComponent(createDetailsPanel());
		splitPane.setDividerLocation(250);

		return splitPane;

	}
	
	private JPanel createOptionsPanel ()
	{
		final JPanel optionsPanel = new JPanel (new BorderLayout ());
	
		final JTree treeSchemaBrowser = createSchemaBrowser ();
		optionsPanel.add (treeSchemaBrowser, BorderLayout.CENTER);
		
		JPanel bottomPanel = new JPanel (new BorderLayout());
		
		final JPanel transactPanel = createTransactionButtons ();
		bottomPanel.add (transactPanel, BorderLayout.NORTH);
		
		final JPanel peerOpPanel = createPeerOperationsButtons ();
		bottomPanel.add (peerOpPanel, BorderLayout.SOUTH);
		
		optionsPanel.add (bottomPanel, BorderLayout.SOUTH);
		
		
		return optionsPanel;
	}
	
	private JTree createSchemaBrowser ()
	{

		final DefaultMutableTreeNode rootNode = new DefaultMutableTreeNode ();
		
		// Root node = Schema overview
		final DefaultMutableTreeNode relRootNode = new DefaultMutableTreeNode (TREE_SCHEMAOVERVIEW_LABEL);
		rootNode.add(relRootNode);
		
		SchemaBrowserTransactionViewNode vn;
		
//		if (_sys.getRecMode())
			vn = new SchemaBrowserTransactionViewNode(SchemaBrowserTransactionViewNode.REC_MODE);
//		else
//			vn = new SchemaBrowserTransactionViewNode(SchemaBrowserTransactionViewNode.EXCHANGE_MODE);
		
		rootNode.add(vn);
		final JTree treeSchemaBrowser = new SchemaBrowserTree (rootNode);
		treeSchemaBrowser.setRootVisible(false);
		
		// Create one node for each relation
		for (Relation rel : _sc.getRelations())
		{
			if (!rel.getName().endsWith("_L") && !rel.getName().endsWith("_R"))
			{
				MutableTreeNode relNode = new SchemaBrowserRelationNode (rel);
				relRootNode.add(relNode);
			}
		}
		
		
		
		
		treeSchemaBrowser.expandPath(new TreePath (relRootNode.getPath()));
		treeSchemaBrowser.setSelectionRow(0);
		
		treeSchemaBrowser.addTreeSelectionListener(new TreeSelectionListener ()
				{
					public void valueChanged(TreeSelectionEvent evt) {
						if (evt.getNewLeadSelectionPath().getLastPathComponent() instanceof SchemaBrowserRelationNode)
						{
							SchemaBrowserRelationNode node = (SchemaBrowserRelationNode) evt.getNewLeadSelectionPath().getLastPathComponent();
							relationSelected(node.getRelation());
							_transSelected = false;
						}
						else
							if (evt.getNewLeadSelectionPath().getLastPathComponent() instanceof SchemaBrowserTransactionViewNode) {
								showCurrentTransaction ();
								_transSelected = true;
							} else {
								showSchemaOverview();
								_transSelected = false;
							}
					}
				});

		return treeSchemaBrowser;
	}
	
	protected void showCurrentTransaction ()
	{
		
		if (currTransPanel != null)
			remove(currTransPanel);
		currTransPanel = new JPanel (new BorderLayout ());
		StringBuffer buff = new StringBuffer ();
		for (RelationDataModel model : getRelationDataModels())
			for (Update upd : model.getTransaction())
			{
				buff.append(upd.toString());
				buff.append("\n");
			}
		JTextArea txtPane = new JTextArea  ();
		txtPane.setText(buff.toString());
		currTransPanel.add (new JScrollPane(txtPane), BorderLayout.CENTER);
		
		_detailsPanel.add (currTransPanel, CARD_TRANSACTION_ID);
		_cardLayout.show(_detailsPanel, CARD_TRANSACTION_ID);
	}
	
	public boolean hasCurrentTransaction ()
	{
		boolean res = false;
		
		Iterator<RelationDataModel> itModels = getRelationDataModels().iterator();
		while (itModels.hasNext() && !res)
			res = !itModels.next().getTransaction().isEmpty();
				
		return res;
	}

	private Map<RelationContext,List<Update>> getCompleteTransaction ()
	{
		final List<RelationDataModel> models = getRelationDataModels ();
		final Map<RelationContext,List<Update>> cpleteTransaction =  new HashMap<RelationContext, List<Update>> ();
		for (RelationDataModel model : models)
			cpleteTransaction.put(model.getRelationCtx(), model.getTransaction());
		return cpleteTransaction;
	}
	
	public TxnPeerID applyTransaction () throws SchemaTransactionException
	{
		final Map<RelationContext,List<Update>> cpleteTransaction =  getCompleteTransaction ();
		try
		{
			TxnPeerID retval = _dataEditFactory.getInstance(_sys).addTransaction(cpleteTransaction);
			for (RelationDataModel model : getRelationDataModels ())
				model.clearTransaction();
			
			return retval;
		} catch (RelationDataEditorException ex)
		{
			throw new SchemaTransactionException (ex);
		}
		
	}
	
	public void rollbackTransaction ()
	{
		final List<RelationDataModel> models = getRelationDataModels ();
		for (RelationDataModel model : models)
			model.rollbackTransaction();		
	}
	
	protected List<RelationDataModel> getRelationDataModels ()
	{
		final List<RelationDataModel> models = new ArrayList<RelationDataModel> ();
		for (int i = 0 ; i < _detailsPanel.getComponentCount() ; i++)
		{
			Component comp = _detailsPanel.getComponent(i);
			if (comp instanceof RelationDataPanel)
				models.add(((RelationDataPanel) comp).getRelationDataModel());
		}
		return models;
	}
	
	private JPanel createTransactionButtons ()
	{
		final JPanel transactButtons = new JPanel ();
		transactButtons.setLayout(new GridBagLayout ());
		
//		if (_sys.getRecMode())
			transactButtons.setBorder(BorderFactory.createTitledBorder("Transactions"));
//		else
//			transactButtons.setBorder(BorderFactory.createTitledBorder("Updates"));
		
		GridBagConstraints cst;
		
		JButton btnCommit = new JButton ("Apply");
		btnCommit.addActionListener(new ActionListener ()
				{
					public void actionPerformed(ActionEvent evt) {						
						try
						{
							TxnPeerID tid = applyTransaction();
							_peerTransIntf.setRecomputeTransactions();
							_peerTransIntf.setRefreshDataViews(true);
							JOptionPane.showMessageDialog(getParent(), "Committed as transaction" + (tid == null ? "" : " with ID " + tid), "Transaction added", JOptionPane.INFORMATION_MESSAGE);
						} catch (SchemaTransactionException ex)
						{
							JOptionPane.showMessageDialog(getParent(), "Error while committing: " + ex.getMessage(), "Warning", JOptionPane.WARNING_MESSAGE);
							ex.printStackTrace();							
						}
						if (_transSelected)
							showCurrentTransaction();
					}
				});
		cst = new GridBagConstraints ();
		cst.gridx=0;
		cst.gridy=0;
		cst.anchor = GridBagConstraints.EAST;
		cst.insets = new Insets (0, 0, 5, 3);
		transactButtons.add (btnCommit, cst);
		
		JButton btnRollback = new JButton ("Rollback");
		btnRollback.addActionListener(new ActionListener ()
		{
			public void actionPerformed(ActionEvent evt) {
				rollbackTransaction();
				if (_transSelected)
					showCurrentTransaction();
			}
		});
		cst = new GridBagConstraints ();
		cst.gridx=1;
		cst.gridy=0;
		cst.anchor = GridBagConstraints.WEST;
		cst.insets = new Insets (0, 0, 5, 0);
		transactButtons.add (btnRollback, cst);		
		
		JButton btnSave = new JButton ("Save");
		btnSave.addActionListener(new ActionListener ()
			{
				public void actionPerformed(ActionEvent arg0) {
					saveTransaction ();
				}
			});
		cst = new GridBagConstraints ();
		cst.gridx=0;
		cst.gridy=1;
		cst.anchor = GridBagConstraints.EAST;
		cst.insets = new Insets (0, 0, 0, 3);
		transactButtons.add (btnSave, cst);		

		JButton btnLoad = new JButton ("Load");
		btnLoad.addActionListener(new ActionListener ()
			{
				public void actionPerformed(ActionEvent arg0) {
					loadTransaction ();
					if (_transSelected)
						showCurrentTransaction();
				}
			});
		btnLoad.setPreferredSize(btnRollback.getPreferredSize());
		cst = new GridBagConstraints ();
		cst.gridx=1;
		cst.gridy=1;
		cst.anchor = GridBagConstraints.WEST;
		transactButtons.add (btnLoad, cst);		

		return transactButtons;
	}
	
	
	
	
	private JPanel createPeerOperationsButtons ()
	{
		final JPanel transactButtons = new JPanel ();
		transactButtons.setLayout(new GridBagLayout ());
		transactButtons.setBorder(BorderFactory.createTitledBorder("Peer operations"));
		
		GridBagConstraints cst;
		int currRow = 0;
		
		final SchemaMgtPanel scPanel = this;
		if (_sys.getRecMode())
		{
			JButton btnCommit = new JButton ("Publish and Reconcile");
			if (_sys.isLocalPeer(_p)) {
			btnCommit.addActionListener(new ActionListener ()
					{
						public void actionPerformed(ActionEvent evt) 
						{
							_peerTransIntf.setRecomputeTransactions();
							PeerCommands.publishAndReconcile(scPanel, _sys, _peerTransIntf);
						}
					});
			} else {
				btnCommit.setEnabled(false);
			}
			cst = new GridBagConstraints ();
			cst.gridx=0;
			cst.gridy=currRow++;
			cst.anchor = GridBagConstraints.CENTER;
			transactButtons.add (btnCommit, cst);
		}
		
		cst = new GridBagConstraints ();
		cst.gridx=0;
		cst.gridy=currRow++;
		cst.anchor = GridBagConstraints.CENTER;
		return transactButtons;
	}
	
	private JPanel createDetailsPanel ()
	{
		initSchemaOverview ();
		
		return _detailsPanel;
	}
	
	private void initSchemaOverview ()
	{
		//dinesh++
		//Additional ORchestra System Parameter to add/modify/drop relations
		_schemaGraph = new SchemaGraph (_sc,_p.getId(),_sys);
		//dinesh--
		_schemaGraph.addObserver(this);
		final JScrollPane scrollGraph = new JScrollPane (_schemaGraph);
		_schemaGraph.applyLayoutOnFirstReveal();
		_detailsPanel.add(scrollGraph, CARD_SCHOVERVIEW_PREFIX);
	}
	
	private void showSchemaOverview ()
	{
		_cardLayout.show(_detailsPanel, CARD_SCHOVERVIEW_PREFIX);
	}
	
	private void createRelationPanel (Relation rel)
	{
		if (!_knownRels.contains(rel.getName()))
		{
			_knownRels.add(rel.getName());
			RelationContext relCtx = new RelationContext (rel, _sc, _p, false);
			
			RelationDataEditorIntf relDataEdit = _dataEditFactory.getInstance(_sys);
			RelationDataPanel relPanel = new RelationDataPanel (relDataEdit, relCtx);
			_detailsPanel.add(relPanel, CARD_RELATION_PREFIX + rel.getName());
		}
		
	}
	
	private void relationSelected (Relation rel)
	{
		createRelationPanel (rel);
		_cardLayout.show(_detailsPanel, CARD_RELATION_PREFIX + rel.getName());
	}
	
	private RelationDataPanel getRelationDataPanel (Relation rel)
	{
		createRelationPanel(rel);
		for (Component comp : _detailsPanel.getComponents())
			if (comp instanceof RelationDataPanel)
			{
				if (((RelationDataPanel) comp).getRelationDataModel().getRelationCtx().getRelation().equals(rel))
				return (RelationDataPanel) comp;
			}		
		return null;
	}
	
	
	public void relationDbleClicked(Relation rel) {
		relationSelected(rel);
	}
	
	
	
	private void saveTransaction ()
	{
		//JFileChooser fChooser = new JFileChooser(Config.getWorkDir());
		
		int res = _fChooser.showSaveDialog(this);
		
		if (res == JFileChooser.APPROVE_OPTION)
		{
			
			File f = _fChooser.getSelectedFile();
			if (!f.getName().endsWith(".trans"))
				f = new File (f.getPath() + ".trans");
			try
			{
			
				FileOutputStream outStream = new FileOutputStream (f, false);
				Map<RelationContext, List<Update>> cpleteTrans = getCompleteTransaction();
				Map<String,List<Update>> cpleteTransRelName = new HashMap<String, List<Update>> ();
				for (Map.Entry<RelationContext, List<Update>> entry : cpleteTrans.entrySet())
				{
					cpleteTransRelName.put(entry.getKey().getRelation().getName(), entry.getValue());
				}
				
				ObjectOutputStream objStream = new ObjectOutputStream (outStream);
				objStream.writeObject(cpleteTransRelName);
				
				outStream.close();
			} catch (FileNotFoundException ex)
			{
				ex.printStackTrace();
				JOptionPane.showMessageDialog(this, "Error while saving: \n" + ex.getMessage(), "Save transaction", JOptionPane.ERROR_MESSAGE);
			} catch (IOException ex)
			{
				ex.printStackTrace();
				JOptionPane.showMessageDialog(this, "Error while saving: \n" + ex.getMessage(), "Save transaction", JOptionPane.ERROR_MESSAGE);
			}			
		}
	}

	
	private void loadTransaction ()
	{
		//JFileChooser fChooser = new JFileChooser ();
		
		int res = _fChooser.showOpenDialog(this);
		
		if (res == JFileChooser.APPROVE_OPTION)
		{
			
			File f = _fChooser.getSelectedFile();
			try
			{
			
				FileInputStream outStream = new FileInputStream (f);
				
				ObjectInputStream objStream = new ObjectInputStream (outStream);
				Map map = (Map) objStream.readObject();
				for (Object key : map.keySet())
				{
					try
					{
						Relation rel = _sc.getRelation((String) key);
						RelationDataPanel relPanel = getRelationDataPanel(rel);
						List updates = (List) map.get(key);
						for (Object upd : updates)
						{
							try
							{
								relPanel.getRelationDataModel().addUpdate((Update) upd);
							} catch (RelationDataEditorException ex)
							{
								JOptionPane.showMessageDialog(this, "Error while loading transaction: \n" + ex.getMessage(), "Load transaction", JOptionPane.ERROR_MESSAGE);
							} catch (RelationDataConstraintViolationException ex)
							{
								JOptionPane.showMessageDialog(this, "Error while loading transaction: \n" + ex.getMessage(), "Load transaction", JOptionPane.ERROR_MESSAGE);
							}				
							
						}
					} catch (RelationNotFoundException ex)
					{
						JOptionPane.showMessageDialog(this, "Cannot load updates for unknown relation " + (String) key, "Load updates", JOptionPane.ERROR_MESSAGE);
					}
				}
					
				
				outStream.close();
			} catch (FileNotFoundException ex)
			{
				ex.printStackTrace();
				JOptionPane.showMessageDialog(this, "Error while loading transaction: \n" + ex.getMessage(), "Load transaction", JOptionPane.ERROR_MESSAGE);
			} catch (IOException ex)
			{
				ex.printStackTrace();
				JOptionPane.showMessageDialog(this, "Error while loading transaction: \n" + ex.getMessage(), "Load transaction", JOptionPane.ERROR_MESSAGE);
			} catch (ClassNotFoundException ex)
			{
				ex.printStackTrace();
				JOptionPane.showMessageDialog(this, "Error while loading transaction: \n" + ex.getMessage(), "Load transaction", JOptionPane.ERROR_MESSAGE);
			} catch (ClassCastException ex)
			{
				ex.printStackTrace();
				JOptionPane.showMessageDialog(this, "Error while loading transaction: \n" + ex.getMessage(), "Load transaction", JOptionPane.ERROR_MESSAGE);
			} 
		}
	}

	
	public void refreshDataViews ()
	{
		for (RelationDataModel model : getRelationDataModels())
			model.reset();
	}
}


