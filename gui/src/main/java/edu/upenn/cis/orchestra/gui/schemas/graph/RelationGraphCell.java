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
package edu.upenn.cis.orchestra.gui.schemas.graph;

import javax.swing.BorderFactory;
import javax.swing.UIDefaults;
import javax.swing.UIManager;

import org.jgraph.graph.DefaultGraphCell;
import org.jgraph.graph.GraphConstants;

import edu.upenn.cis.orchestra.datamodel.Relation;

public class RelationGraphCell extends DefaultGraphCell 
{
	public static final long serialVersionUID = 1L;
	
	public RelationGraphCell (Relation rel)
	{
		super (rel);
		UIDefaults def = UIManager.getDefaults();
		
		// Set the cell graphic properties
		GraphConstants.setOpaque(getAttributes(), true);
		GraphConstants.setBackground(getAttributes(), def.getColor("Relation.background"));
		GraphConstants.setForeground(getAttributes(), def.getColor("Relation.foreground"));
		GraphConstants.setFont(getAttributes(), def.getFont("Relation.font"));
		GraphConstants.setAutoSize(getAttributes(), true);
//		GraphConstants.setBorder(getAttributes(), BorderFactory.createRaisedBevelBorder());
		GraphConstants.setBorder(getAttributes(), BorderFactory.createLineBorder(UIManager.getColor("Relation.border"), 1));
		GraphConstants.setInset(getAttributes(), 2);
	}
	
	public Relation getRelation ()
	{
		return (Relation) super.getUserObject();
	}
	
	public String toString ()
	{
		Relation rel = getRelation();
		return rel.getName();
	}

}
