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


import edu.upenn.cis.orchestra.datamodel.RelationContext;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;


/**
 * This class is used a model for a one-row JTable used to 
 * insert a new row into a relation 
 * @author olivier
 *
 */
public class RelationDataInsertionModel extends RelationDataModelAbs
{
	public static final long serialVersionUID = 1L;
	
	// Current tuple for insertion
	Tuple _insertRow = null;
	// JTable model used for the full relation data, will be reponsible 
	// for the actual insertion (and to update its own cache)
	private RelationDataModel _relDataModel;
	
	/**
	 * Create a new model
	 * @param relDataEdit Used to communicate with the data layer
	 * @param relCtx Relation to insert into
	 * @param relDataModel JTable model used for the full relation data, will be reponsible 
	 *          for the actual insertion (and to update its own cache)
	 */
	public RelationDataInsertionModel (final RelationDataEditorIntf relDataEdit, 
											final RelationContext relCtx,
											final RelationDataModel relDataModel)
	{
		super (relDataEdit, relCtx);
		
		// Copy local attributes
		_relDataModel = relDataModel;
		
		// Initialize the insertion tuple to an empty tuple
		reset ();
	}
	
	protected void setRelationDataModel (RelationDataModel model)
	{
		_relDataModel = model;
	}
	
	public int getRowCount() {
		return 1;
	}
	
	public Object getValueAt(int row, int col) {
		return _insertRow.get(col);
	}
	
	@Override
	public boolean isSkolem(int row, int col) {
		return false;
	}
	
	@Override
	public boolean isCellEditable(int row, int col) {
		return true;
	}
	
		
	@Override
	public void setValueAt(Object val, int row, int col) {
		try
		{
			_insertRow.set(col, val);
		} catch (ValueMismatchException ex)
		{
			assert false: "Should not happen since JTable checks for type";
			ex.printStackTrace();
		}
	}
	
	public void reset ()	
	{
		try
		{
			_insertRow = new Tuple (_relDataEdit.getRelationSchema(_relCtx));
		} catch (RelationDataEditorException ex)
		{
			// TODO
			ex.printStackTrace();
		}
		fireTableRowsUpdated(0, 0);
	}

	// Validate the insert (only if the row is non empty)
	public void validate ()
			throws RelationDataConstraintViolationException,
			   RelationDataEditorException	
	{
		if (!_insertRow.isNull())
		{
			_relDataModel.insert(_insertRow);
			reset ();
		}
	}
	
}
