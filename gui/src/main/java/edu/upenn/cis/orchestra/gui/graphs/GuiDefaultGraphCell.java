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

import org.jgraph.graph.AttributeMap;
import org.jgraph.graph.DefaultGraphCell;
import org.jgraph.graph.DefaultPort;


public class GuiDefaultGraphCell 
		extends DefaultGraphCell
		implements GuiDefaultGraphObj
{
	public static final long serialVersionUID = 1L;
	
	public GuiDefaultGraphCell ()
	{
		super ();
		addDefaultPort ();
	}
	
	public GuiDefaultGraphCell(Object usrObject) 
	{
		super (usrObject);
		addDefaultPort ();
	}
	
	public GuiDefaultGraphCell(Object usrObject, AttributeMap attributes)
	{
		super (usrObject, attributes);
		addDefaultPort ();
	}
	
	private void addDefaultPort ()
	{
		DefaultPort port = new DefaultPort ();
		add (port);
		port.setParent(this);		
	}
	
	public DefaultPort getDefaultPort ()
	{
		return (DefaultPort) getChildAt (0);
	}	
	
	public String getTooltipText ()
	{
		return null;
	}
	
	
}
