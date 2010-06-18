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

import java.util.Map;

import javax.swing.BorderFactory;
import javax.swing.UIManager;

import org.jgraph.graph.DefaultPort;
import org.jgraph.graph.GraphConstants;

import edu.upenn.cis.orchestra.gui.graphs.GuiDefaultGraphCell;
import edu.upenn.cis.orchestra.gui.graphs.GuiGraphConstants;
import edu.upenn.cis.orchestra.gui.graphs.GuiVertexRenderer;

/**
 * Represents a mapping
 * 
 * @author zives
 *
 */
public class MappingVertex extends GuiDefaultGraphCell 
{
	public static final long serialVersionUID = 1L;
	
	private String _label;
	private String _tuple;
	
	public MappingVertex (String label, String tupleLabel)
	{
		super(label);
		_label = label;
		_tuple = tupleLabel;
		GuiGraphConstants.setVertexShape(getAttributes(), GuiVertexRenderer.SHAPE_DIAMOND);
		GuiGraphConstants.setOpaque(getAttributes(), false);
		
		add(new DefaultPort());
		setVisible(getAttributes());
	}
	
	/**
	 * Make the node invisible and borderless
	 * 
	 * @param attrs
	 */
	public static void setInvisible(Map attrs) {
		GraphConstants.setBackground(attrs, UIManager.getColor("MappingVertex.invisible.background"));
		GraphConstants.setForeground(attrs, UIManager.getColor("MappingVertex.invisible.foreground"));
		GraphConstants.setBorder(attrs, BorderFactory.createLineBorder(UIManager.getColor("MappingVertex.invisible.border"), 1));
	}

	/**
	 * Make the node visible
	 * 
	 * @param attrs
	 */
	public void setVisible(Map attrs) {
		GraphConstants.setOpaque(attrs, true);
		GraphConstants.setAutoSize(attrs, true);

		GraphConstants.setBackground(attrs, UIManager.getColor("MappingVertex.visible.background"));
		GraphConstants.setForeground(attrs, UIManager.getColor("MappingVertex.visible.foreground"));
		GraphConstants.setBorder(attrs, BorderFactory.createLineBorder(UIManager.getColor("MappingVertex.visible.border"), 1));
//		GraphConstants.setBorder(attrs, BorderFactory.createRaisedBevelBorder());
		GraphConstants.setInset(attrs, 5);
	}
	
	public void setBorderless() {
		GraphConstants.setOpaque(getAttributes(), true);
		GraphConstants.setAutoSize(getAttributes(), true);

		GraphConstants.setBackground(getAttributes(), UIManager.getColor("MappingVertex.borderless.background"));
		GraphConstants.setForeground(getAttributes(), UIManager.getColor("MappingVertex.borderless.foreground"));

		GraphConstants.setBorderColor(getAttributes(), UIManager.getColor("MappingVertex.borderless.border"));
		GraphConstants.setBorder(getAttributes(), BorderFactory.createEmptyBorder());
	}
	
	public String toString ()
	{
		return _label;
	}
	
	public String getTooltipText() {
		return _tuple;
	}	
	
}
