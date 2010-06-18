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
package edu.upenn.cis.orchestra.gui.schemas;

import java.awt.Component;

import javax.swing.JTable;
import javax.swing.UIManager;
import javax.swing.table.DefaultTableCellRenderer;

public class RelationDataEditorCellRenderer extends DefaultTableCellRenderer 
{
	public static final long serialVersionUID = 1L;

	public static RelationDataEditorCellRenderer _sharedInstance = new RelationDataEditorCellRenderer ();


	private RelationDataEditorCellRenderer ()
	{

	}

	@Override
	public Component getTableCellRendererComponent(JTable table, 
			Object val, 
			boolean isSelected, 
			boolean hasFocus, 
			int row, 
			int col) {
		Component cell = super.getTableCellRendererComponent(table, val, isSelected, hasFocus, row, col);
		RelationDataModelAbs absModel = (RelationDataModelAbs) table.getModel();
		try {
			if (isSelected) {
				cell.setForeground(UIManager.getColor("Relation.selected"));
			} else if (absModel.isSkolem(row, col)) {
				cell.setForeground(UIManager.getColor("Relation.skolem"));
			} else {				
				cell.setForeground(UIManager.getColor("Relation.normal"));
			}
		} catch (IndexOutOfBoundsException e) {
			// TODO: why is this happening in the first place?
			cell.setForeground(UIManager.getColor("Relation.normal"));
		}
		return cell;
	}

	public static RelationDataEditorCellRenderer getSharedInstance ()
	{
		return _sharedInstance;
	}

}
