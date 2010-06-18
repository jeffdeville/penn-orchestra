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
package edu.upenn.cis.orchestra.gui.query;

import java.awt.Color;
import java.awt.Component;

import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.ListCellRenderer;

import edu.upenn.cis.orchestra.datamodel.Tuple;

/**
 * Basic class to render a tuple with a tooltip representing its provenance.
 * 
 * Derived from JDK 6 example for ListCellRenderer.
 * 
 * @author zives
 *
 */
public class TupleRenderer extends JLabel implements ListCellRenderer {
	
	public TupleRenderer() {
		setOpaque(true);
	}

	@Override
	public Component getListCellRendererComponent(JList list,
            Object value,
            int index,
            boolean isSelected,
            boolean cellHasFocus) {

		Tuple tup = (Tuple)value;
		
		setText(tup.toString());
	
		String prov = tup.getProvenance();
		
		if (prov != null && prov.length() > 0)
			setToolTipText(tup.getProvenance());
		
		Color background;
		Color foreground;
		
		// check if this cell represents the current DnD drop location
		JList.DropLocation dropLocation = list.getDropLocation();
		if (dropLocation != null
		&& !dropLocation.isInsert()
		&& dropLocation.getIndex() == index) {
			
			background = Color.BLUE;
			foreground = Color.WHITE;
		
		// check if this cell is selected
		} else if (isSelected) {
			background = Color.RED;
			foreground = Color.WHITE;
		
		// unselected, and not the DnD drop location
		} else {
			background = Color.WHITE;
			foreground = Color.BLACK;
		};
		
		setBackground(background);
		setForeground(foreground);
		
		return this;
	}

	public static final long serialVersionUID = 1;	
}
