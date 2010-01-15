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

import java.awt.Color;

import javax.swing.BorderFactory;
import javax.swing.UIManager;

import org.jgraph.graph.DefaultGraphCell;
import org.jgraph.graph.DefaultPort;

import edu.upenn.cis.orchestra.gui.graphs.GuiGraphConstants;
import edu.upenn.cis.orchestra.gui.graphs.GuiVertexRenderer;

public class ImplyCell extends DefaultGraphCell {

	public static final long serialVersionUID = 1L;
	public static final int IMPLY_CELL_SHAPE = GuiVertexRenderer.SHAPE_CIRCLE;
	
	public ImplyCell ()
	{
		super ();
		
		// Set the cell graphic properties
		GuiGraphConstants.setOpaque(getAttributes(), true);
		GuiGraphConstants.setBackground(getAttributes(), UIManager.getColor("ImplyCell.background"));
		GuiGraphConstants.setForeground(getAttributes(), UIManager.getColor("ImplyCell.foreground"));
		GuiGraphConstants.setBorder(getAttributes(), BorderFactory.createLineBorder(UIManager.getColor("ImplyCell.border"), 1));
		GuiGraphConstants.setAutoSize(getAttributes(), true);
		GuiGraphConstants.setInset(getAttributes(), 2);
		GuiGraphConstants.setVertexShape(getAttributes(), ImplyCell.IMPLY_CELL_SHAPE);
		GuiGraphConstants.setBorder(getAttributes(), BorderFactory.createLineBorder(Color.BLACK, 6));
		
		// Create a default port
		DefaultPort port = new DefaultPort ();
		add (port);
		port.setParent(this);		
	}
	
}
