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
package edu.upenn.cis.orchestra.gui.graphs;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.Icon;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JPanel;
import javax.swing.JSlider;
import javax.swing.UIManager;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

/**
 * Provides some utilities for panels that will contain a JGraph graph
 * @author Olivier Biton
 *
 */
public class GraphPanel extends JPanel 
{
	public static final long serialVersionUID = 1L;

	private GraphBirdView _birdView=null;
	private BasicGraph _graph;
	
	public GraphPanel (final BasicGraph graph)
	{
		this (graph, true);
	}
	
	public GraphPanel (final BasicGraph graph, boolean useBirdView)
	{
		_graph = graph;
		
		setLayout(new GridBagLayout ());
		
		// Add a bird view of the peer graph
		if (useBirdView)
		{
			_birdView = new GraphBirdView ();
			_birdView.setCurrentGraph(_graph);
			GridBagConstraints cst = new GridBagConstraints ();
			cst.gridx = 0;
			cst.gridy = 0;
			cst.weightx = 1;
			cst.weighty = 1;
			cst.anchor = GridBagConstraints.FIRST_LINE_START;
			cst.fill = GridBagConstraints.BOTH;
			cst.gridwidth = 4;
			add (_birdView, cst);
		}
		
		// Add a button to re-run the graph layout
		final Icon icon = UIManager.getIcon("GraphTools.layoutBtn.icon");
		final JButton btnLayout = new JButton (icon);
		btnLayout.setToolTipText("Apply new layout");
		btnLayout.addActionListener(new ActionListener ()
				{
					public void actionPerformed(ActionEvent arg0) 
					{
						_graph.applyLayout();
					}
				});
		GridBagConstraints cst = new GridBagConstraints ();
		cst.gridx = 0;
		cst.gridy = 1;
		cst.anchor = GridBagConstraints.FIRST_LINE_START;
		add(btnLayout, cst);		
		
		// Add a checkbox to disable edges routing when user is interacting (re-enabled when running layout)
		final JCheckBox chkEnabRouting = new JCheckBox ();
		chkEnabRouting.setSelected(true);
		chkEnabRouting.setToolTipText("Enable/Disable edges automatic routing");
		chkEnabRouting.addActionListener(new ActionListener ()
					{
						public void actionPerformed(ActionEvent arg0) {
							_graph.setEnableRoutingWhenNotLayout(chkEnabRouting.isSelected());
						}
					});
		cst = new GridBagConstraints ();
		cst.gridx = 1;
		cst.gridy = 1;
		cst.anchor = GridBagConstraints.FIRST_LINE_START;
		add(chkEnabRouting, cst);		
		
		// Add a slider for graph zoom
		final JSlider zoomSlider = new JSlider(JSlider.HORIZONTAL, 30,200, 100);
		zoomSlider.setToolTipText("Slide to zoom main pane in/out");
		zoomSlider.addChangeListener(new ChangeListener ()
					{
						public void stateChanged(ChangeEvent evt) 
						{
							_graph.setScale(zoomSlider.getValue()/100F);
						}
					});
		cst = new GridBagConstraints ();
		cst.gridx = 2;
		cst.gridy = 1;
		cst.fill = GridBagConstraints.HORIZONTAL;
		cst.weightx = 0.4;
		add(zoomSlider, cst);
		
		// Add a button to reset the slider
		JButton btnResetSlider = new JButton (UIManager.getIcon("GraphTools.btnResetZoom.icon"));
		btnResetSlider.setMargin(new Insets (0,0,0,0));
		btnResetSlider.addActionListener(new ActionListener ()
					{
						public void actionPerformed(ActionEvent arg0) {
							zoomSlider.setValue(100);
						}
					});
		cst = new GridBagConstraints ();
		cst.gridx = 3;
		cst.gridy = 1;
		add(btnResetSlider, cst);
		

	}
	
	public GraphBirdView getBirdView ()
	{
		return _birdView;
	}
	

	
	public void setCurrentGraph (final BasicGraph graph)
	{
		_graph = graph;
		if (_birdView != null)
			_birdView.setCurrentGraph(graph);
	}
	
}
