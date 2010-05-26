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
package edu.upenn.cis.orchestra.gui.provenance;

import java.awt.BorderLayout;
import java.awt.Cursor;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;

import javax.swing.BorderFactory;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;

import edu.upenn.cis.orchestra.datamodel.IntPeerID;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.gui.graphs.LegendGraph;
import edu.upenn.cis.orchestra.gui.schemas.RelationDataEditorFactory;
import edu.upenn.cis.orchestra.gui.schemas.RelationDataModel;

public class ProvenanceViewer extends JPanel {
	
	public static final long serialVersionUID = 1L;
	
	OrchestraSystem _system;
	
	IntPeerID _pId;
	JSplitPane _splitPane;
	TupleSelectorPanel _transInfoPanel;
	ProvenanceGraph _provGraph;
	JPopupMenu _viewMenu;
	Tuple _selectedTuple;
		
	
	public ProvenanceViewer (Peer p, OrchestraSystem sys, RelationDataEditorFactory fact)
	{
		this (p, p.getSchemas().iterator().next(), sys, fact);
	}
	
	public ProvenanceViewer (Peer p, Schema s, OrchestraSystem sys, RelationDataEditorFactory fact) 
	{
	
		_system = sys;
		_pId = new IntPeerID(new Integer(0));
		
		initFrame(p, s, sys, fact);
	}
	
	private void initFrame(Peer p, Schema s, OrchestraSystem sys, RelationDataEditorFactory fact) {
		setLayout (new BorderLayout ());
		
		// Prepare the split pane
		_splitPane = new JSplitPane ();
		_splitPane.setResizeWeight(1.0);

		// Create the transaction graph
		_provGraph = new ProvenanceGraph(p, s, sys);
		JPanel pan = new JPanel();
		pan.setLayout(new BorderLayout());
		
		// Create the graph scroll pane
		JScrollPane scrollPGraph = new JScrollPane (_provGraph);
		pan.add(scrollPGraph, BorderLayout.CENTER);
		
		
		// Create the legend
		JPanel subPan = new JPanel();
		subPan.setBorder (BorderFactory.createTitledBorder("Legend"));
		subPan.setLayout(new BorderLayout());
		//JLabel lab = new JLabel("Legend:");
		//lab.setSize(80, 15);
		//subPan.add(lab, BorderLayout.PAGE_START);
		LegendGraph g = ProvenanceGraph.createLegend(600, 43, subPan.getBackground());
		subPan.add(g, BorderLayout.CENTER);
		g.applyLayoutOnFirstReveal();
		pan.add(subPan, BorderLayout.PAGE_END);
		
		_splitPane.setLeftComponent(pan);
		
		/*
		_tg.getSelectionModel().addGraphSelectionListener(new GraphSelectionListener ()
					{
						public void valueChanged(GraphSelectionEvent evt) 
						{
								if (_tg.getSelectionCount()==1 
											&& _tg.getSelectionCell() instanceof TupleVertex)
								{
									TxnPeerID t = ((TupleVertex) _tg.getSelectionCell()).getTxn();
									int prio = ((TupleVertex) _tg.getSelectionCell()).getPriority();
									_transInfoPanel.setTransaction(t, prio);
								} else {
									_transInfoPanel.clearUpdates();
									
								}
							}
					});*/
		

		// Add the panel used to show selected /schemas properties
		initTransInfoPanel (_system, p, s, fact);

		_splitPane.setRightComponent(_transInfoPanel);
		
		add(_splitPane, BorderLayout.CENTER);
	}
	
	private void initTransInfoPanel(OrchestraSystem sys, Peer p, Schema s, RelationDataEditorFactory fact) {
		_transInfoPanel = new TupleSelectorPanel(sys, _provGraph, p, s, this, fact);
		
		addListeners();
	}
	
	public void setPeerAndSchema(Peer p, Schema s) {
		_transInfoPanel.setPeerAndSchema(p, s);
	}
	
	public void setContext(Tuple t) {
    	_provGraph.setRoot(t);
	}
	
	/**
	 * Loads the provenance graph with the provenance of {@code theTuple},
	 * clearing any earlier graph.
	 * 
	 * @param theTuple
	 *            the tuple who's provenance is to be loaded
	 */
	public void loadProvenance(final Tuple theTuple) {
		Cursor cur = Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR);
		_splitPane.setCursor(cur);
		_provGraph.setCursor(cur);
		_transInfoPanel.setCursor(cur);
		_transInfoPanel.setEnabled(false);
		setContext(theTuple);

		cur = Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR);
		_splitPane.setCursor(cur);
		_provGraph.setCursor(cur);
		_transInfoPanel.setCursor(cur);
		_transInfoPanel.setEnabled(true);
	}
	

	public void addListeners() {
		// Add a mouse listener to allow observers to change their state 
		// according to the item on which the user double-clicks.
		_provGraph.getSpecializedGraphUI().addMouseListener(new MouseListener ()
		{
			public void mouseClicked(MouseEvent evt) {
                if (evt.getClickCount()==2 && evt.getButton() == MouseEvent.BUTTON1)
				{
                    Object obj = _provGraph.getSelectionCell();
                    if (obj != null 
                		&& obj instanceof TupleVertex) 
                    {
                    	final TupleVertex cell = (TupleVertex) _provGraph.getSelectionCell();

    					_transInfoPanel.setContext(cell.getTuple().getOrigin(), cell.getTuple());
                    }
				} else if (evt.getButton() == MouseEvent.BUTTON2 || evt.getButton() == MouseEvent.BUTTON3) {
                    Object obj = _provGraph.getSelectionCell();
                    if (obj != null 
                		&& obj instanceof TupleVertex) 
                    {
                    	TupleVertex cell = (TupleVertex) _provGraph.getSelectionCell();

                    	contextMenu(cell.getTuple(), evt.getX(), evt.getY());
                    }
				} 

			}
			public void mouseEntered(MouseEvent evt) {}
			public void mouseExited(MouseEvent evt) {}
			public void mousePressed(MouseEvent evt) {}
			public void mouseReleased(MouseEvent evt) {}
		});
		
		// Right-click menu should give resolution as an option
		_viewMenu = new JPopupMenu(  ); 
		JMenuItem item;
		_viewMenu.add(item = new JMenuItem("Set focus"));
	    item.addActionListener(new ActionListener(  ) {
	    	public void actionPerformed(ActionEvent event) {
    			setTuple(_selectedTuple);
	           }
	       });
			
	}
	
	public void contextMenu(Tuple curTuple, int x, int y) {
		_selectedTuple = curTuple;
		_viewMenu.show(this, x, y);
	}
	

	/**
	 * Change the current epoch
	 * 
	 * @param epoch
	 */
	public void setTuple(Tuple t) {
		_provGraph.setRoot(t);
	}
	
	public RelationDataModel getRelationDataModel ()
	{
		return _transInfoPanel.getRelationDataModel();
	}
}
