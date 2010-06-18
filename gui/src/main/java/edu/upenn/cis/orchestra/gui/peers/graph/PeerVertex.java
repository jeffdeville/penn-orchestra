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
package edu.upenn.cis.orchestra.gui.peers.graph;

import java.util.Map;

import javax.swing.BorderFactory;
import javax.swing.UIManager;

import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.gui.graphs.GuiDefaultGraphCell;
import edu.upenn.cis.orchestra.gui.graphs.GuiGraphConstants;
import edu.upenn.cis.orchestra.gui.graphs.GuiVertexRenderer;

public class PeerVertex extends GuiDefaultGraphCell 
{
	public static final long serialVersionUID = 1L;
	
//	public static Color PEER_CELL_DEFAULT_COL;
//	public static Color PEER_CELL_DEFAULT_FG_COL;
//	public static Color PEER_TEXT_COL;
	
	public PeerVertex (Peer p, Schema s)
	{
		super (new EncapsPeerSchema(p, s));

//		PEER_CELL_DEFAULT_COL = UIManager.getColor("PeerVertex.normal.background");//new Color(0,0,143);//new Color(224,216,0);
//		PEER_CELL_DEFAULT_FG_COL = UIManager.getColor("PeerVertex.normal.foreground");//new Color(51,98,161);//new Color(255,255,200);
//		PEER_TEXT_COL = UIManager.getColor("PeerVertex.normal.text");//Color.WHITE;

		setNormal(getAttributes());
	}
	
	public static void setNormal(Map attr) {
		// Set the cell graphic properties
		GuiGraphConstants.setVertexShape(attr, GuiVertexRenderer.SHAPE_CIRCLE);		
		GuiGraphConstants.setOpaque(attr, true);
		GuiGraphConstants.setBackground(attr, UIManager.getColor("PeerVertex.normal.background"));
		GuiGraphConstants.setForeground(attr, UIManager.getColor("PeerVertex.normal.text"));
		GuiGraphConstants.setFont(attr, UIManager.getFont("PeerVertex.normal.font"));//new Font("SANS SERIF", Font.BOLD, 12));
		//GraphConstants.setGradientColor(getAttributes(), PEER_CELL_DEFAULT_FG_COL);
		GuiGraphConstants.setAutoSize(attr, true);
		//GuiGraphConstants.setBorder(getAttributes(), BorderFactory.createRaisedBevelBorder());
//		GuiGraphConstants.setBorder(getAttributes(), BorderFactory.createRaisedBevelBorder());
		GuiGraphConstants.setBorder(attr, BorderFactory.createLineBorder(UIManager.getColor("PeerVertex.normal.border")));
		GuiGraphConstants.setInset(attr, 2);
	}
	
	public static void setHighlighted(Map attr) {
		// Set the cell graphic properties
		GuiGraphConstants.setVertexShape(attr, GuiVertexRenderer.SHAPE_CIRCLE);		
		GuiGraphConstants.setOpaque(attr, true);
		GuiGraphConstants.setBackground(attr, UIManager.getColor("PeerVertex.highlighted.background"));
		GuiGraphConstants.setForeground(attr, UIManager.getColor("PeerVertex.highlighted.text"));
		GuiGraphConstants.setFont(attr, UIManager.getFont("PeerVertex.highlighted.font"));//new Font("SANS SERIF", Font.BOLD, 12));
		//GraphConstants.setGradientColor(getAttributes(), PEER_CELL_DEFAULT_FG_COL);
		GuiGraphConstants.setAutoSize(attr, true);
		//GuiGraphConstants.setBorder(getAttributes(), BorderFactory.createRaisedBevelBorder());
//		GuiGraphConstants.setBorder(getAttributes(), BorderFactory.createRaisedBevelBorder());
		GuiGraphConstants.setBorder(attr, BorderFactory.createLineBorder(UIManager.getColor("PeerVertex.highlighted.border")));
		GuiGraphConstants.setInset(attr, 2);
	}
	
	public String toString ()
	{
		EncapsPeerSchema ps = (EncapsPeerSchema) getUserObject();
		
		if (ps.getPeer().getId().equals(ps.getSchema().getSchemaId()))
			return ps.getPeer().getId();
		else
			return ps.getPeer().getId() + " " + ps.getSchema().getSchemaId();
	}
	
	public Peer getPeer ()
	{
		EncapsPeerSchema ps = (EncapsPeerSchema) getUserObject();
		return ps.getPeer();
	}

	public Schema getSchema ()
	{
		EncapsPeerSchema ps = (EncapsPeerSchema) getUserObject();
		return ps.getSchema();		
	}

	@Override
	public String getTooltipText() {
		EncapsPeerSchema ps = (EncapsPeerSchema) getUserObject();
		return "Peer " + ps.getPeer().getId() + " / schema " + ps.getSchema().getSchemaId();
	}
}
