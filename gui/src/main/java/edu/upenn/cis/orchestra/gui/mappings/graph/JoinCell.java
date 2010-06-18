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
package edu.upenn.cis.orchestra.gui.mappings.graph;

import javax.swing.BorderFactory;
import javax.swing.UIManager;

import org.jgraph.graph.DefaultPort;

import edu.upenn.cis.orchestra.gui.graphs.GuiDefaultGraphCell;
import edu.upenn.cis.orchestra.gui.graphs.GuiGraphConstants;
import edu.upenn.cis.orchestra.gui.graphs.GuiVertexRenderer;

public class JoinCell extends GuiDefaultGraphCell {

	public static final long serialVersionUID = 1L;
	public static final int JOIN_CELL_SHAPE = GuiVertexRenderer.SHAPE_DIAMOND;
	
	public JoinCell(String joinFields, boolean isBody)
	{
		super (joinFields);
		
		// Set the cell graphic properties
		GuiGraphConstants.setOpaque(getAttributes(), true);
		
		String background = isBody ? "JoinCell.body.background" : "JoinCell.head.background";
		String foreground = isBody ? "JoinCell.body.foreground" : "JoinCell.head.foreground";
		String border = isBody ? "JoinCell.body.border" : "JoinCell.head.border";

		GuiGraphConstants.setBackground(getAttributes(), UIManager.getColor(background));
		GuiGraphConstants.setForeground(getAttributes(), UIManager.getColor(foreground));
		GuiGraphConstants.setBorder(getAttributes(), BorderFactory.createLineBorder(UIManager.getColor(border), 1));
		GuiGraphConstants.setAutoSize(getAttributes(), true);
		GuiGraphConstants.setInset(getAttributes(), 6);
		GuiGraphConstants.setVertexShape(getAttributes(), JoinCell.JOIN_CELL_SHAPE);
		
		// Create a default port
		DefaultPort port = new DefaultPort ();
		add (port);
		port.setParent(this);		
	}
	
}
