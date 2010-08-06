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

import javax.swing.UIManager;

import org.jgraph.graph.AttributeMap;
import org.jgraph.util.ParallelEdgeRouter;

import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.gui.graphs.GuiDefaultEdge;
import edu.upenn.cis.orchestra.gui.graphs.GuiGraphConstants;

public class PeerMappingEdge extends GuiDefaultEdge implements IPeerMapping {
	
	public static final long serialVersionUID = 1L;
	
	private final Mapping _map;
	private final Peer _p;

	public PeerMappingEdge(Peer p, Mapping map) {
		super();
		
		_map = map;
		_p = p;
		
		setNormal(getAttributes());
	}

	public PeerMappingEdge(Peer p, Mapping map, Object arg0, AttributeMap arg1) {
		super(arg0, arg1);
		
		_map = map;
		_p = p;
		
		setNormal(getAttributes());
	}

	public PeerMappingEdge(Peer p, Mapping map, Object arg0) {
		super(arg0);
		
		_map = map;
		_p = p;
		setNormal(getAttributes());
	}

	public void setNormal(Map attr)
	{
		//GuiGraphConstants.setFont(getAttributes(), new Font("SANS SERIF", Font.PLAIN, 12));
		GuiGraphConstants.setFont(attr, UIManager.getFont("PeerMapping.normal.font"));//new Font("SANS SERIF", Font.PLAIN, 12));
		GuiGraphConstants.setForeground(attr, UIManager.getColor("PeerMapping.normal.foreground"));
		GuiGraphConstants.setLayoutEdgeRouting(attr, ParallelEdgeRouter.getSharedInstance());
	}
	
	public void setHighlighted(Map attr)
	{
		GuiGraphConstants.setFont(attr, UIManager.getFont("PeerMapping.highlighted.font"));//new Font("SANS SERIF", Font.PLAIN, 12));
		GuiGraphConstants.setForeground(attr, UIManager.getColor("PeerMapping.highlighted.foreground"));
	}
	
	public Mapping getMapping() {
		return _map;
	}

	public Peer getPeer() {
		return _p;
	}
	
	@Override
	public String getTooltipText() {
		return "Mapping " + _map.toString();
	}
	
}
