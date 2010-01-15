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
package edu.upenn.cis.orchestra.gui.transactions;

import java.awt.Color;
import java.awt.Cursor;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.awt.geom.Rectangle2D;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPopupMenu;
import javax.swing.JSlider;
import javax.swing.SwingUtilities;
import javax.swing.ToolTipManager;

import org.eclipse.zest.layouts.LayoutStyles;
import org.eclipse.zest.layouts.algorithms.SpringLayoutAlgorithm;
import org.jgraph.event.GraphSelectionEvent;
import org.jgraph.event.GraphSelectionListener;
import org.jgraph.graph.CellView;
import org.jgraph.graph.DefaultEdge;
import org.jgraph.graph.DefaultGraphCell;
import org.jgraph.graph.DefaultPort;
import org.jgraph.graph.GraphConstants;

import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.datamodel.Update;
import edu.upenn.cis.orchestra.datamodel.exceptions.CompareMismatch;
import edu.upenn.cis.orchestra.datamodel.iterators.IteratorException;
import edu.upenn.cis.orchestra.datamodel.iterators.ResultIterator;
import edu.upenn.cis.orchestra.gui.graphs.BasicGraph;
import edu.upenn.cis.orchestra.gui.graphs.GuiGraphConstants;
import edu.upenn.cis.orchestra.gui.graphs.ILayoutHelper;
import edu.upenn.cis.orchestra.gui.graphs.LayoutHelperBuilder;
import edu.upenn.cis.orchestra.gui.graphs.LegendGraph;
import edu.upenn.cis.orchestra.gui.graphs.LayoutHelperBuilder.LayoutAlgorithmType;
import edu.upenn.cis.orchestra.reconciliation.Db;
import edu.upenn.cis.orchestra.reconciliation.DbException;

/**
 * The visualization of transactions, dependencies, and conflicts
 * 
 * @author zives
 *
 */
public class TransactionGraph extends BasicGraph {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public static final boolean ADD_ANTECEDENTS_TO_ACTIVE = false;
	
	Peer _reconcilingPeer;
	List<DefaultGraphCell> _cells;
	Db _db;
	Map<TxnPeerID,TransactionVertex> _txnNodes;
	Map<TxnPeerID,Set<DefaultEdge>> _txnEdges;
	Map<TxnPeerID,Set<DefaultEdge>> _confEdges;
	//PeerID _reconcilingPeerID;
	int _confRecon = -1;
	int _confPos = -1;
	int _confChoice = -1;
	JPopupMenu _conflictMenu;
	JSlider _slider;
	
	/**
	 * Nodes that have no antecedents
	 */
	List<DefaultGraphCell> _notAntecedents;
	int _currentEpoch;
	TxnPeerID _currentTrans;
	
	/**
	 * Mappings between epoch and the active set of nodes in the epoch
	 */
	List<Set<TxnPeerID>> _activeSets;
	List<Set<TxnPeerID>> _frontierSets;
	
	public TransactionGraph() {
		super();
		_cells = new ArrayList<DefaultGraphCell>();
		_txnNodes = new HashMap<TxnPeerID,TransactionVertex>();
		_txnEdges = new HashMap<TxnPeerID,Set<DefaultEdge>>();
		_confEdges = new HashMap<TxnPeerID,Set<DefaultEdge>>();
		_notAntecedents = new ArrayList<DefaultGraphCell>();
		_activeSets = new ArrayList<Set<TxnPeerID>>();
		_frontierSets = new ArrayList<Set<TxnPeerID>>();
	}
	
	public TransactionGraph(Peer p, /*int epoch, List<TxnPeerID> trans,*/ Db theDb, JSlider slider) {
		super ();

		setAntiAliased(true);
		
		_slider = slider;
		_reconcilingPeer = p;
		//_reconcilingPeerID = p.getId();
		_db = theDb;
		_cells = new ArrayList<DefaultGraphCell>();
		_txnNodes = new HashMap<TxnPeerID,TransactionVertex>();
		_txnEdges = new HashMap<TxnPeerID,Set<DefaultEdge>>();
		_confEdges = new HashMap<TxnPeerID,Set<DefaultEdge>>();
		_notAntecedents = new ArrayList<DefaultGraphCell>();
		_activeSets = new ArrayList<Set<TxnPeerID>>();
		_frontierSets = new ArrayList<Set<TxnPeerID>>();
		
		 Runnable doEpoch = new Runnable() {
		     public void run() {
		 		try {
					setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
					computeAllEpochs(_db.getCurrentRecno());
					setEpoch(0);
				} catch (DbException d) {
					JOptionPane.showMessageDialog(null, d.getMessage(), "Error accessing DB", JOptionPane.ERROR_MESSAGE);
					d.printStackTrace();
				} finally {
					setCursor(Cursor.getDefaultCursor());
				}
		     }
		 };

		SwingUtilities.invokeLater(doEpoch);
		ToolTipManager.sharedInstance().registerComponent(this);

		// Right-click should trigger the pop-up menu
		getSpecializedGraphUI().addMouseListener(new MouseListener ()
		{
			public void mouseClicked(MouseEvent evt) {
                if (evt.getButton() == MouseEvent.BUTTON2 || evt.getButton() == MouseEvent.BUTTON3) {
                    Object obj = TransactionGraph.this.getSelectionCell();
                    if (obj != null && obj instanceof TransactionVertex) 
                    {
                    	TransactionVertex cell = (TransactionVertex) obj;
                    	
                		if (_confEdges.containsKey(cell.getTxn())) {
                			_currentTrans = cell.getTxn();

                			contextMenu(evt.getX(), evt.getY());
                		}
                    }
				} 

			}
			public void mouseEntered(MouseEvent evt) {}
			public void mouseExited(MouseEvent evt) {}
			public void mousePressed(MouseEvent evt) {}
			public void mouseReleased(MouseEvent evt) {}
		});
		
		// Double-click should trigger resolution
		getSelectionModel().addGraphSelectionListener(new GraphSelectionListener ()
		{
			public void valueChanged(GraphSelectionEvent evt) 
			{
					if (getSelectionCount()==2
								&& getSelectionCell() instanceof TransactionVertex)
					{
						_currentTrans = ((TransactionVertex) getSelectionCell()).getTxn();
						try {
							resolveDialog(_currentTrans);
		            	} catch (DbException dbe) {
		            		JOptionPane.showMessageDialog(TransactionGraph.this, dbe.getMessage(), "Error finding conflict sets", JOptionPane.ERROR_MESSAGE);
		            	}
					}
				}
		});

		// Right-click menu should give resolution as an option
		 _conflictMenu = new JPopupMenu(  ); 
        JMenuItem item;
        _conflictMenu.add(item = new JMenuItem("Resolve"));
        item.addActionListener(new ActionListener(  ) {
            public void actionPerformed(ActionEvent event) {
            	try {
            		resolveDialog(_currentTrans);
            	} catch (DbException dbe) {
            		JOptionPane.showMessageDialog(TransactionGraph.this, dbe.getMessage(), "Error finding conflict sets", JOptionPane.ERROR_MESSAGE);
            	}
            }
        });
        /*
        _conflictMenu.add(item = new JMenuItem("Cancel"));
        item.addActionListener(new ActionListener(  ) {
            public void actionPerformed(ActionEvent event) {
            		resetSelection();
            }        	
        });*/
	}
	
	/**
	 * Resets the selected set of transactions on the screen, i.e.,
	 * after the user cancels a proposed resolution.
	 *
	 */
	public void resetSelection() {
    	try {
			Map<Integer,List<List<Set<TxnPeerID>>>> m = _db.getConflicts();
			resetConflictChoice(m.get(new Integer(_confRecon)).get(_confPos));
			_confRecon = -1;
			_confPos = -1;
			_confChoice = -1;
    	} catch (DbException d) {
    	}
	}

	/**
	 * Resolves the conflicts using the highlighted set and
	 * refreshes the display.
	 *
	 */
	public void resolveWithSelected() {
		Map<Integer,Map<Integer,Integer>> conflictResolutions = new HashMap<Integer,Map<Integer,Integer>>();
		Map<Integer,Integer> l = new HashMap<Integer,Integer>();
		conflictResolutions.put(new Integer(_confRecon), l);
		l.put(new Integer(_confPos), new Integer(_confChoice));

		try {
			_db.resolveConflicts(conflictResolutions);
			computeAllEpochs(_db.getCurrentRecno());
			setEpoch(_currentEpoch);
		} catch (DbException d) {
			try {
				Map<Integer,List<List<Set<TxnPeerID>>>> m = _db.getConflicts();
				resetConflictChoice(m.get(new Integer(_confRecon)).get(_confPos));
			} catch (DbException d2) {
				
			}
		}
		_confRecon = -1;
		_confPos = -1;
		_confChoice = -1;
	}
	
	public void contextMenu(int x, int y) {
		_conflictMenu.show(this, x, y);
	}
	
	/**
	 * Shows the resolution modal dialog box
	 * 
	 * @param chosenPeer
	 * @param x
	 * @param y
	 * @throws DbException
	 */
	public void resolveDialog(TxnPeerID chosenPeer) throws DbException {
		Map<Integer,List<List<Set<TxnPeerID>>>> m = _db.getConflicts();
		
		// Find the transaction in the conflicts
		for (Integer recon : m.keySet()) {
			int pos = 0;
			for (List<Set<TxnPeerID>> conf : m.get(recon)) {
				
				// For each conflict option in the conflict
				int preferred = -1;
				for (int i = 0; i < conf.size(); i++) {

					if (conf.get(i).contains(chosenPeer)) {
						preferred = i;
						_confRecon = recon.intValue();
						_confPos = pos;
						_confChoice = preferred;
					}
				}
				if (preferred != conf.size()) {
					highlightConflictChoice(conf, preferred);
					//_conflictMenu.show(this, x, y);
					Object[] options = {"Resolve with this",
					                    "Cancel resolution"};
					int n = JOptionPane.showOptionDialog(this,
					    "Would you like to resolve using this set of transactions?",
					    "Conflict resolution",
					    JOptionPane.YES_NO_CANCEL_OPTION,
					    JOptionPane.QUESTION_MESSAGE,
					    null,
					    options,
					    options[0]);
					
					if (n == 0)
						resolveWithSelected();
					else
						resetSelection();
					break;
				}
				pos++;
			}
		}
	}
	
	public int computeAllEpochs() throws DbException {
		int ret = _db.getCurrentRecno();
		if (_currentEpoch > ret)
			_currentEpoch = ret;
		computeAllEpochs(ret);
		
		return ret;
	}

	/**
	 * Computes the set of transactions across all epochs up to the max
	 * 
	 * @param maxEpoch
	 * @throws DbException
	 */
	public void computeAllEpochs(int maxEpoch) throws DbException {
		getGraphLayoutCache().remove(_cells.toArray(), true, true);
		
		List<TxnPeerID> transSet = new ArrayList<TxnPeerID>();//trans;
		_cells.clear();
		_txnNodes.clear();
		_notAntecedents = new ArrayList<DefaultGraphCell>();
		_activeSets = new ArrayList<Set<TxnPeerID>>();
		_frontierSets = new ArrayList<Set<TxnPeerID>>();
		
		for (int epoch = 0; epoch <= maxEpoch; epoch++) {
			_activeSets.add(new HashSet<TxnPeerID>());
			_frontierSets.add(new HashSet<TxnPeerID>());
			ResultIterator<TxnPeerID> results = _db.getTransactionsForReconciliation(epoch);
			
			try {
				while (results.hasNext()) {
					TxnPeerID t = results.next();
					transSet.add(t);
					_activeSets.get(epoch).add(t);
				}
				results.close();
			} catch (IteratorException ie) {
				JOptionPane.showMessageDialog(this, ie.getMessage(), "Peer DB Iterator Error", JOptionPane.ERROR_MESSAGE);
				ie.printStackTrace();
			}

			HashSet<TxnPeerID> newAntecedents = new HashSet<TxnPeerID>();
			for (TxnPeerID t : _activeSets.get(epoch)) { //_transSet) {
				for (Update u : _db.getTransaction(t)) {
					for (TxnPeerID ante : u.getPrevTids())
						if (!_activeSets.get(epoch).contains(ante))
							newAntecedents.add(ante);
				}
			}
			
			_frontierSets.get(epoch).addAll(newAntecedents);
	
		}
		convertTransToNodesAndEdges(transSet, _db);
		_slider.setMinimum(0);
		_slider.setMaximum(maxEpoch);
		
		applyLayout();
	}
	
	/**
	 * Change the epoch in which the transactions are shown
	 * 
	 * @param epoch
	 */
	public void setEpoch(int epoch) throws DbException {
		_currentEpoch = epoch;
		List<TransactionVertex> allTrans = new ArrayList<TransactionVertex>();
		
		// Compute all vertices not from our epoch
		for (int i = 0; i < _activeSets.size(); i++)
			if (i != epoch) {
				for (TxnPeerID t : _activeSets.get(i)) {
					if (!allTrans.contains(_txnNodes.get(t)))
						allTrans.add(_txnNodes.get(t));
					
					if (!allTrans.contains(_txnNodes.get(t)))
						allTrans.add(_txnNodes.get(t));
				}
			}
		
		Map<Object,Object> attrs = new java.util.Hashtable<Object,Object>();
		// Clear all nodes by default
		attrs.clear();
		TransactionVertex.setInvisible(attrs);

		getGraphLayoutCache().edit(allTrans.toArray(), attrs);

		// Clear all edges going into these
		for (int i = 0; i < _activeSets.size(); i++) {
			attrs.clear();
			GraphConstants.setLineColor(attrs, Color.WHITE);
			if (i != epoch) {
				for (TxnPeerID t : _activeSets.get(i)) {
					for (DefaultGraphCell c : _txnEdges.get(t)) {
						getGraphLayoutCache().editCell(c, attrs);
					}
					for (DefaultGraphCell c : _confEdges.get(t)) {
						getGraphLayoutCache().editCell(c, attrs);
					}
				}
			}
		}
		
		// Clear all entries by default
		attrs = new java.util.Hashtable<Object,Object>();
		GraphConstants.setBackground(attrs, Color.WHITE);
		GraphConstants.setForeground(attrs, Color.WHITE);//LIGHT_GRAY);
		getGraphLayoutCache().edit(allTrans.toArray(), attrs);

		// Gray the frontier set
		for (TxnPeerID t : _frontierSets.get(epoch)) {
			attrs.clear();
			_txnNodes.get(t).setDimmed();
			getGraphLayoutCache().editCell(_txnNodes.get(t), attrs);
			
			// Color the frontier edges
			attrs.clear();
			GraphConstants.setLineColor(attrs, Color.LIGHT_GRAY);
			for (DefaultGraphCell c : _txnEdges.get(t)) {
				getGraphLayoutCache().editCell(c, attrs);
			}
		}

		// Color the active set
		for (TxnPeerID t : _activeSets.get(epoch)) {
			attrs.clear();
			_txnNodes.get(t).setVisible(attrs);
			getGraphLayoutCache().editCell(_txnNodes.get(t), attrs);
			
			attrs.clear();
			GraphConstants.setLineColor(attrs, Color.BLACK);
			for (DefaultGraphCell c : _txnEdges.get(t)) {
				getGraphLayoutCache().editCell(c, attrs);
			}
			attrs.clear();
			GraphConstants.setLineColor(attrs, Color.RED);
			for (DefaultGraphCell c : _confEdges.get(t)) {
				getGraphLayoutCache().editCell(c, attrs);
			}
		}
		_slider.setValue(_currentEpoch);
	}

	/**
	 * Resets the display of the transactions
	 * 
	 * @param confSet
	 */
	public void resetConflictChoice(final List<Set<TxnPeerID>> confSet) {
		// For each conflict option in the conflict
		for (int i = 0; i < confSet.size(); i++) {
			// Iterate through the members of the conflict set
			Iterator<TxnPeerID> transIt = confSet.get(i).iterator();
			
			while (transIt.hasNext()) {
				TxnPeerID trans = transIt.next();
				TransactionVertex item = _txnNodes.get(trans);
				
				item.setVisible(item.getAttributes());
				getGraphLayoutCache().editCell(item, item.getAttributes());
				
				for (DefaultEdge e: _confEdges.get(trans)) {
					resetConflictEdge(e);
					getGraphLayoutCache().editCell(e, e.getAttributes());
				}
			}
		}
	}
	
	/**
	 * Highlights the proposed set of transactions in resolving a deferral,
	 * and dims the ones that conflict.
	 * 
	 * @param confSet
	 * @param preferred
	 */
	public void highlightConflictChoice(final List<Set<TxnPeerID>> confSet, int preferred) {
		// For each conflict option in the conflict
		for (int i = 0; i < confSet.size(); i++) {
			// Iterate through the members of the conflict set
			Iterator<TxnPeerID> transIt = confSet.get(i).iterator();
			
			// Highlight these nodes
			if (i != preferred) {
			// Dim the non-preferred nodes
				while (transIt.hasNext()) {
					TxnPeerID theTrans = transIt.next();
					TransactionVertex item = _txnNodes.get(theTrans);
					
					item.setDimmed();
					getGraphLayoutCache().editCell(item, item.getAttributes());
					
					for (DefaultEdge e: _confEdges.get(theTrans)) {
						dimConflictEdge(e);
						getGraphLayoutCache().editCell(e, e.getAttributes());
					}
				}
				
			}
		}
		
		Iterator<TxnPeerID> transIt = confSet.get(preferred).iterator();
		// Highlight these nodes
		while (transIt.hasNext()) {
			TxnPeerID trans = transIt.next();
			TransactionVertex item = _txnNodes.get(trans);
			
			item.setHighlighted();
			getGraphLayoutCache().editCell(item, item.getAttributes());

			for (DefaultEdge e: _confEdges.get(trans)) {
				dashConflictEdge(e);
				getGraphLayoutCache().editCell(e, e.getAttributes());
			}
		}
	}
	
	public boolean isConflicting(TxnPeerID trans) {
		return _confEdges.keySet().contains(trans);
	}
	
	private void addConflictEdges(final List<DefaultGraphCell> cells, final Db db,
			final Map<TxnPeerID,TransactionVertex> txnNodes) throws DbException {
		// Add conflicts as bidirectional edges
		Map<Integer,List<List<Set<TxnPeerID>>>> m = db.getConflicts();
		
		int confID = 0;
		for (Integer recon : m.keySet()) {
			if (true) //recon.intValue() == _currentEpoch)
				for (List<Set<TxnPeerID>> conf : m.get(recon)) {
					
					// For each conflict option in the conflict
					for (int i = 0; i < conf.size(); i++) {
						
						// Iterate through the members of the 1st conflict set
						Iterator<TxnPeerID> first = conf.get(i).iterator();
						while (first.hasNext()) {
							TxnPeerID f = first.next();

							// Iterate through the remaining conflict sets
							for (int j = i + 1; j < conf.size(); j++) {
								
								Iterator<TxnPeerID> second = conf.get(j).iterator();
								while (second.hasNext()) {
									TxnPeerID s = second.next();
									
									// Create the edge
									DefaultEdge edge = new DefaultEdge ("C" + String.valueOf(confID));
									
									setConflictEdge(edge, txnNodes.get(f), txnNodes.get(s));
					
									// add the edge to the cells list
									cells.add (edge);
									
									// Record that this edge is associated with a conflict
									// with both transactions
									_confEdges.get(f).add(edge);
									_confEdges.get(s).add(edge);
								}
							}
						}
						confID++;
					}
				}
		}
	}

	/**
	 * Makes the edge between a transaction and antecedent a dependency edge
	 * 
	 * @param edge
	 * @param ante
	 * @param trans
	 */
	private static void setDependencyEdge(DefaultEdge edge, DefaultGraphCell ante, DefaultGraphCell trans) {
		// Connect to default ports
		edge.setSource(ante.getChildAt(0));
		edge.setTarget(trans.getChildAt(0));
		
		// Add arrow
		GraphConstants.setLineColor(edge.getAttributes(), Color.BLACK);
		GraphConstants.setLineEnd(edge.getAttributes(), GraphConstants.ARROW_CLASSIC);
		GraphConstants.setLineStyle(edge.getAttributes(), GraphConstants.STYLE_SPLINE);
		GraphConstants.setLabelAlongEdge(edge.getAttributes(), true);
		
	}
	
	private static void dashConflictEdge(DefaultEdge edge) {
		float[] dash = {5,5};
		GraphConstants.setDashPattern(edge.getAttributes(), dash);
		GraphConstants.setLineColor(edge.getAttributes(), Color.RED);
	}

	private static void resetConflictEdge(DefaultEdge edge) {
		float[] dash = {1.0f,0};
		GraphConstants.setDashPattern(edge.getAttributes(), dash);
		GraphConstants.setLineColor(edge.getAttributes(), Color.RED);
	}

	private static void dimConflictEdge(DefaultEdge edge) {
		resetConflictEdge(edge);
		GraphConstants.setLineColor(edge.getAttributes(), Color.WHITE);
	}

	/**
	 * Makes the edge between a transaction and antecedent a conflict edge
	 * 
	 * @param edge
	 * @param ante
	 * @param trans
	 */
	private static void setConflictEdge(DefaultEdge edge, DefaultGraphCell f, DefaultGraphCell s) {
		// Connect to default ports
		edge.setSource(f.getChildAt(0));
		edge.setTarget(s.getChildAt(0));
		
		// Add arrow
		GraphConstants.setLineColor(edge.getAttributes(), Color.RED);
		GraphConstants.setLineEnd(edge.getAttributes(), GraphConstants.ARROW_SIMPLE);
		GraphConstants.setLineBegin(edge.getAttributes(), GraphConstants.ARROW_SIMPLE);
		GraphConstants.setLineStyle(edge.getAttributes(), GraphConstants.STYLE_SPLINE);
		GraphConstants.setLabelAlongEdge(edge.getAttributes(), true);
	}

	/**
	 * Visualizes transactions as vertices, dataflow as
	 * directed arcs, and conflicts as bidirectional arcs 
	 * 
	 * @throws DbException
	 */
	private void convertTransToNodesAndEdges(final List<TxnPeerID> transSet, final Db db) throws DbException {
		Set<TxnPeerID> notAntecedent = new HashSet<TxnPeerID>();
		// Create a node for each transaction
		for (TxnPeerID trans : transSet) {
			DefaultPort port = new DefaultPort ();

			String updateList = "";
			int updateCount = 0;
			int prio = -1;
			boolean addElipsis = false;
			for (Update u : db.getTransaction(trans)) {
				int uPrio = -1;
				try {
					uPrio = db.getUpdatePriority(u);
				} catch (CompareMismatch cm) {
					JOptionPane.showMessageDialog(this, cm.getMessage(), "Update Compare Mismatch Error", JOptionPane.ERROR_MESSAGE);
					cm.printStackTrace();
				}
				if (prio != 0 && prio < uPrio)
					prio = uPrio;
				
				if (updateCount++ == 0) {
					updateList = u.toString();
					if (updateList.indexOf('[') > -1)
						updateList = updateList.substring(0, updateList.indexOf('['));
				} else if (updateCount < 6) {
					updateList = updateList + ",\n" + u.toString();
					if (updateList.indexOf('[') > -1)
						updateList = updateList.substring(0, updateList.indexOf('['));
				}else if (updateCount > 6)
					addElipsis = true;
			}
			if (addElipsis)
				updateList = updateList + "...";
			
			TransactionVertex c = new TransactionVertex(trans, updateList, prio, 
					db.hasAcceptedTxn(trans), db.hasRejectedTxn(trans));
			
			// Set the cell graphic properties
			c.add (port);
			port.setParent(c);
			_cells.add(c);
			_txnNodes.put(trans, c);
			_confEdges.put(trans, new HashSet<DefaultEdge>());
			_txnEdges.put(trans, new HashSet<DefaultEdge>());
		}
		
		// Add edges to antecedent nodes (if they're in the set)
		for (TxnPeerID trans : transSet) {
			boolean hasAntecedents = false;
			List<Update> updates = db.getTransaction(trans);
			
			for (Update u : updates) {
				for (TxnPeerID ante : u.getPrevTids()) {
					hasAntecedents = true;
					//notAntecedent.remove(ante);
					if (_txnNodes.get(trans) != null && _txnNodes.get(ante) != null) {
						// Create the edge
						DefaultEdge edge = new DefaultEdge ();
						
						setDependencyEdge(edge, _txnNodes.get(ante), _txnNodes.get(trans));
						
						// add the edge to the cells list
						_cells.add (edge);
						
						// Record the edge flowing into this transaction
						_txnEdges.get(trans).add(edge);
					}
				}
			}
			if (!hasAntecedents)
				notAntecedent.add(trans);
			
		}
		
		addConflictEdges(_cells, db, _txnNodes);
		
		// We can now add all the cells to the graph model
		getGraphLayoutCache().insert(_cells.toArray());
		
		//_notAntecedents = new GraphCell[notAntecedent.size()];
		for (TxnPeerID pid : notAntecedent)
			_notAntecedents.add(_txnNodes.get(pid));
	}
	
	/**
	 * Adds legend info to the JGraph
	 * 
	 * @param g
	 * @param width
	 * @param height
	 */
	public static LegendGraph createLegend(int width, int height, Color col) {
		LegendGraph g = new LegendGraph();
		g.setBackground(col);
		g.setSize(width, height);
		g.setPreferredSize(new Dimension(width, height));
		ArrayList<DefaultGraphCell> c = new ArrayList<DefaultGraphCell>();
		int i = 0;
		c.add(new TransactionVertex("Untrusted", 0, false, false));
		c.get(i++).add(new DefaultPort());
		c.add(new TransactionVertex("Trusted", 1, false, false));
		c.get(i++).add(new DefaultPort());
		c.add(new TransactionVertex("Accepted", 1, true, false));
		c.get(i++).add(new DefaultPort());
		c.add(new TransactionVertex("Local", edu.upenn.cis.orchestra.datamodel.TrustConditions.OWN_TXN_PRIORITY, true, false));
		c.get(i++).add(new DefaultPort());
		
		c.add(new DefaultEdge("updated-by"));
		setDependencyEdge((DefaultEdge)c.get(i), (DefaultGraphCell)c.get(2), (DefaultGraphCell)c.get(3));
		c.add(new DefaultEdge("updated-by"));
		setDependencyEdge((DefaultEdge)c.get(i+1), (DefaultGraphCell)c.get(1), (DefaultGraphCell)c.get(2));
		c.add(new DefaultEdge("conflicts"));
		setConflictEdge((DefaultEdge)c.get(i+2), (DefaultGraphCell)c.get(0), (DefaultGraphCell)c.get(1));
		
		Object[] c2 = new Object[1];
		c2[0] = c.get(0);
		
		g.getGraphLayoutCache().insert(c.toArray());
		g.setRoots(c2);
		//g.applyLayout();
		
		
		// Let's fix the cells positions by hand to get a better result
		/*double deltaX = 40D;
		
		double maxHeight = 0D;
		for (int j = 0 ; j < 4 ; j++)
			maxHeight = Math.max (maxHeight, g.getGraphLayoutCache().getMapping(c.get(j), false).getBounds().getHeight());
		
		for (int j = 0 ; j < 4 ; j++)
			recenterVertically (g, c.get(j), deltaX, maxHeight, j);
		*/
		return g;
	}
	
	private static void recenterVertically (LegendGraph g, DefaultGraphCell cell, 
											double deltaX, double maxHeight,
											int indCell)
	{
		Rectangle2D bounds = getRectangle (g, cell, deltaX*indCell, maxHeight);
		GuiGraphConstants.setBounds(cell.getAttributes(), bounds);
		g.getGraphLayoutCache().editCell(cell, cell.getAttributes());		
	}
	
	private static Rectangle2D getRectangle (LegendGraph g, DefaultGraphCell cell,
										double deltaX, double maxHeight)
	{
		CellView view = g.getGraphLayoutCache().getMapping(cell, false);
		Rectangle2D res = new Rectangle2D.Double (
										view.getBounds().getX() + deltaX, 
										(maxHeight - view.getBounds().getHeight())/2,
										view.getBounds().getWidth(),
										view.getBounds().getHeight()
									);
		return res;
	}

	
	/**
	 * Applies a spring layout to this graph.
	 * 
	 * @see edu.upenn.cis.orchestra.gui.graphs.BasicGraph#applyLayout()
	 */
	@Override
	protected void applyLayout ()
	{
		
		List<DefaultGraphCell> newCells = new ArrayList<DefaultGraphCell> ();
 		DefaultGraphCell tmpCell = new DefaultGraphCell ();
		tmpCell.add (new DefaultPort ());
		newCells.add(tmpCell);
		
		for (DefaultGraphCell cell : _notAntecedents)
		{
			DefaultEdge edge = new DefaultEdge ();
			edge.setSource(tmpCell.getChildAt(0));
			edge.setTarget(cell.getChildAt(0));
			GuiGraphConstants.setLineColor(edge.getAttributes(), getBackground());
			newCells.add(edge);
		}
		getGraphLayoutCache().insert(newCells.toArray());

		LayoutHelperBuilder builder = new LayoutHelperBuilder(this, LayoutAlgorithmType.SPRING);
		ILayoutHelper helper = builder.build();
		helper.applyLayout();
		
		//TODO: Check that the ports added to the new cells are also removed.
		getGraphLayoutCache().remove(newCells.toArray());
		
	}


	public String getToolTipText(MouseEvent event) {
		  Object cell = getFirstCellForLocation(event.getX(), event.getY());
		  if (cell instanceof TransactionVertex) {
		    return ((TransactionVertex) cell).getToolTipString();
		  }
		  return null;
		}
}
