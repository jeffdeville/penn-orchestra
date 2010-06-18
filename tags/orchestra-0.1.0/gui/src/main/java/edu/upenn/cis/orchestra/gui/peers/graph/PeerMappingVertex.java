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

import java.awt.Color;
import java.util.Map;

import javax.swing.UIManager;

import org.jgraph.graph.GraphConstants;

import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.gui.graphs.GuiDefaultGraphCell;
import edu.upenn.cis.orchestra.gui.graphs.GuiGraphConstants;

public class PeerMappingVertex extends GuiDefaultGraphCell implements IPeerMapping
{
	public static final long serialVersionUID = 1L;
	
	public static final Color PEER_CELL_DEFAULT_COL = Color.WHITE;//YELLOW;
	public static final Color PEER_CELL_DEFAULT_FG_COL = Color.LIGHT_GRAY;
	
	private final Mapping _map;
	private final Peer _p; 
	
	public PeerMappingVertex (Peer p, Mapping map, String name)
	{
		super (name);
		
		_p = p;
		_map = map;

		setNormal(getAttributes());
	}
	
	public void setNormal(Map attr) {
		// Set the cell graphic properties
		GraphConstants.setOpaque(attr, false);//true);
//		GraphConstants.setBackground(getAttributes(), UIManager.getColor("PeerMapping.normal.background"));//PEER_CELL_DEFAULT_COL);
		GuiGraphConstants.setForeground(attr, UIManager.getColor("PeerMapping.normal.foreground"));
//		GuiGraphConstants.setFont(getAttributes(), new Font("SANS SERIF", Font.PLAIN, 12));
		GuiGraphConstants.setFont(attr, UIManager.getFont("PeerMapping.normal.font"));
		//GraphConstants.setGradientColor(getAttributes(), PEER_CELL_DEFAULT_FG_COL);
		GraphConstants.setAutoSize(attr, true);
		//GraphConstants.setBorder(getAttributes(), BorderFactory.createEtchedBorder());
		GraphConstants.setInset(attr, 2);
	}
	
	public void setHighlighted(Map attr) {
		GuiGraphConstants.setForeground(attr, UIManager.getColor("PeerMapping.highlighted.foreground"));
		GuiGraphConstants.setFont(attr, UIManager.getFont("PeerMapping.highlighted.font"));
	}
	
	public Mapping getMapping() {
		return _map;
	}
	
	public Peer getPeer() {
		return _p;
	}
	
	public String toString ()
	{
		return getUserObject().toString();
	}

	public String getName()
	{
		return toString();
	}

	@Override
	public String getTooltipText() {
		return "Mapping " + _map.toString();
	}	
	
}
