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

import javax.swing.table.AbstractTableModel;

import edu.upenn.cis.orchestra.datamodel.RelationContext;
import edu.upenn.cis.orchestra.datamodel.RelationField;

public abstract class RelationDataModelAbs extends AbstractTableModel {

	// Relation to show/edit
	final RelationContext _relCtx;
	// Used to communicate with the data layer
	final RelationDataEditorIntf _relDataEdit;
	
	
	/**
	 * Create a new model
	 * @param relDataEdit Used to communicate with the data layer
	 * @param relCtx Relation to edit
	 */
	public RelationDataModelAbs (final RelationDataEditorIntf relDataEdit, 
									final RelationContext relCtx)
	{
		// Copy local attributes
		_relCtx = relCtx;		
		_relDataEdit = relDataEdit;
	}
	
	@Override
	public String getColumnName(int col) {
		RelationField fld = _relCtx.getRelation().getField(col); 
		return fld.getName();
	}
	
	public boolean isPrimaryKey (int col)
	{
		RelationField fld = _relCtx.getRelation().getField(col); 
		return _relCtx.getRelation().getPrimaryKey().getFields().contains(fld);
	}
	
	//TODO: skolems??
	@Override
	public Class<?> getColumnClass(int col) {
		return _relCtx.getRelation().getField(col).getType().getClassObj();
	}

//	@Override
	public int getColumnCount() {
		return _relCtx.getRelation().getFields().size();
	}

	public abstract boolean isSkolem (int row, int col);
	
}
