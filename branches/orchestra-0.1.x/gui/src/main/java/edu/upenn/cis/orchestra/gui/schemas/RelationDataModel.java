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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.datamodel.RelationContext;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.Update;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.datamodel.iterators.IteratorException;
import edu.upenn.cis.orchestra.datamodel.iterators.ResultIterator;

/**
 * The RelationDataModel is a model for a JTable used to edit a relation's 
 * contents.
 * It uses RelationDataEditorIntf to communicate with the data layer.
 * It does not load all data from the cursor in the cache but only when needed, 
 * the cache extents size is defined by CACHE_EXTENT_SIZE  
 * 
 * @author olivier
 * @see RelationDataEditorIntf
 */
public class RelationDataModel extends RelationDataModelAbs
{
	public static final long serialVersionUID = 1L;
	
	// Iterator over the data, used to fill the cache when more data is needed
	ResultIterator<Tuple> _iterator = null;
	
	// How many rows to add to the cache when more data is needed
	//TODO: Temp to 10000 since the SqlDb closes the iterator... to be fixed
	static final int CACHE_EXTENT_SIZE = 10000;
	// Data cache
	final List<Tuple> _cache = new ArrayList<Tuple> (100);
	// Current transaction over the relation.
	final List<Update> _transaction = new ArrayList<Update> ();

	private boolean _cacheCanBeExt=true;
	
	private boolean _isEditable = false;

	/**
	 * Create a new model
	 * @param relDataEdit Used to communicate with the data layer
	 * @param relCtx Relation to edit
	 */
	public RelationDataModel (final RelationDataEditorIntf relDataEdit, 
									final RelationContext relCtx)
	{
		super (relDataEdit, relCtx);

		// Open the data cursor and fill the initial cache
		try
		{
			_iterator = _relDataEdit.getData(relCtx);
			extendCache();
		}
		catch (RelationDataEditorException ex2)
		{
			// TODO
			ex2.printStackTrace();
		}
	}
	
	public RelationDataModel (final RelationDataEditorIntf relDataEdit, 
			final RelationContext relCtx,
			final ResultIterator<Tuple> iter)
	{
		super (relDataEdit, relCtx);
		
		// Open the data cursor and fill the initial cache
		_iterator = iter;
		extendCache();
	}
	
	
	
	public void reset ()
	{
		int initRows = _cache.size();
		
		_cache.clear();
		_transaction.clear();
		_cacheCanBeExt = true;
		if (initRows == 0)
			return;
		fireTableRowsDeleted(0, initRows-1);
		// Open the data cursor and fill the initial cache
		try
		{
			_iterator = _relDataEdit.getData(_relCtx);
			extendCache();
		}
		catch (RelationDataEditorException ex2)
		{
			// TODO
			ex2.printStackTrace();
		}
	}
	
	/** 
	 * Call this method to extend the cache by CACHE_EXTENT_SIZE rows 
	 * if possible.
	 *
	 */
	private boolean extendCache ()
	{
		if (_iterator == null || _cache == null)
			return false;
		
		boolean expanded = false;
		try
		{
			int initCacheSize = _cache.size();
			while ((_cache.size() < initCacheSize + CACHE_EXTENT_SIZE || !Config.getGuiCacheRelEditData())
						&& _iterator.hasNext())
			{
				_cache.add (_iterator.next());
				expanded = true;
			}
			_cacheCanBeExt = _iterator.hasNext();
			// We always keep an empty row to know when to extend, notify 
			// the view that it jas changed
			if (_cache.size()>initCacheSize)
				fireTableRowsUpdated(initCacheSize, initCacheSize);
			if (_cache.size()>initCacheSize+1)
				fireTableRowsInserted(initCacheSize+1, _iterator.hasNext()?_cache.size():_cache.size()-1);
		} catch (IteratorException ex)
		{
			//TODO
			ex.printStackTrace();
		}
		
		return expanded;
	}
	
	public void setEditable(boolean isEditable) {
		_isEditable = isEditable;
	}
	
	
	/**
	 * Get the number of rows available from the cache (+1 if more data 
	 * can be loaded from the cursor). If this empty row becomes visible it 
	 * will trigger the cache extension.
	 */
	public int getRowCount() {
		if (_cacheCanBeExt)
			return _cache.size()+1;
		else
			return _cache.size();
	}
	
	/**
	 * Get the value for a given row / column
	 * If the empty row used to trigger the cache extension is asked 
	 * then extend the cache first.
	 */
	public Object getValueAt(int row, int col) {
		if (row == _cache.size())
			extendCache();
		if (row >= _cache.size())
			return null;
		if (isSkolem(row, col))
			return "NULL(" + (-_cache.get(row).getLabeledNull(col)) + ")"; 
		else
			return _cache.get(row).get(col);
	}
	
	public Tuple getTupleAt(int row) {
		while (row >= _cache.size())
			extendCache();
		
		return _cache.get(row);
	}
	
	public int getSelectedIndex(Tuple t) {
		int row = 0;
		
		do {
			for (; row < _cache.size(); row++)
				if (_cache.get(row).equals(t))
					return row;
		} while (extendCache());
		return -1;
	}
	
	@Override
	public boolean isSkolem(int row, int col) {		
		return _cache.get(row).isLabeledNull(col);
	}
	
	
	
	/**
	 * Allow edition for all cells
	 */
	@Override
	public boolean isCellEditable(int row, int col) {
		return _isEditable;
	}
	

	/**
	 * Set a given row/column value.
	 * If there has already been an update on this row then complete the previous
	 * update, otherwise create a new one. 
	 */
	@Override
	public void setValueAt(Object val, int row, int col) 
	{
	
		Update upd=findUpdate(row);
		if (upd!=null)
		{
			Tuple prevTpl = upd.getNewVal();
			Tuple tpl = upd.getNewVal().duplicate(); 
			try
			{
				tpl.set(col, val);
				if (!tpl.equals(prevTpl))
				{
					if (!(isKeyUsedInLocalTrans(tpl, upd) || 
							(!tpl.getKeySubtuple().equals(prevTpl.getKeySubtuple())
									&& isKeyUsed(tpl))))
					{
						upd.setNewVal(tpl);
						_cache.set (row, tpl);
					}
					//TODO: Else use message
				}
			} catch (ValueMismatchException ex)
			{
				assert false : "ValueMismatch should not happen since JTable check types";
				ex.printStackTrace();
			} catch (RelationDataEditorException ex)
			{
				//TODO: Show alert message
				ex.printStackTrace();				
			}
		} else
		{
			Tuple tpl = new Tuple (_cache.get(row));
			try
			{
				Tuple prevTpl = _cache.get(row);
				if (!(isKeyUsedInLocalTrans(tpl) || 
						(!tpl.getKeySubtuple().equals(prevTpl.getKeySubtuple())
								&& isKeyUsed(tpl))))
				{
					tpl.set(col, val);
					if (!tpl.equals(prevTpl))
					{
						upd = new Update (_cache.get(row), tpl);
						_transaction.add (upd);
						_cache.set (row, tpl);
					}
				}
			} catch (ValueMismatchException ex)
			{
				//Can happen if value too long for field
				//TODO send message to user
			} catch (RelationDataEditorException ex)
			{
				//TODO: Show alert message
				ex.printStackTrace();				
			}		
				
		}
			//TODO: Else use message
			
		
		
	}
	
	
	
	private Update findUpdate (int row)
	{
		Update upd=null;
		// Find if there has already been an update to this row (insert or update)
		// and if yes we will complete the previous update with the new value
		Iterator<Update> itUpdates = _transaction.iterator();
		while (itUpdates.hasNext() && upd == null)
		{
			Update locUpd = itUpdates.next();
			if (locUpd.getNewVal() != null && locUpd.getNewVal().equals(_cache.get(row)))
				upd = locUpd;
		}
		
		return upd;
		
	}
	
	
	public void addUpdate (Update upd)
				throws RelationDataConstraintViolationException,
				RelationDataEditorException
	{
		if (upd.isInsertion())
		{
			if (!upd.getNewVal().getSchema().equals(_relDataEdit.getRelationSchema(_relCtx)))
				throw new RelationDataEditorException ("The schema is incompatible with this update");
			else
				insert (upd.getNewVal());
		}
		if (upd.isUpdate())
		{
			if (!upd.getNewVal().getSchema().equals(_relDataEdit.getRelationSchema(_relCtx)))
				throw new RelationDataEditorException ("The schema is incompatible with this update");
			else
			{
				// Check that this value exists
				if (isKeyUsed(upd.getOldVal()))
				{
					int row = extendCacheUntilVisible (upd.getOldVal());
					if (row != -1)
					{
						for (int col = 0 ; col < upd.getNewVal().getSchema().getNumCols() ; col++)
							setValueAt(upd.getNewVal().get(col), row, col);
						fireTableRowsUpdated(row, row);
					}
				}					
				else
					throw new RelationDataEditorException ("The updated key does not exist");
			}
		}
		if (upd.isDeletion())
		{
			if (!upd.getOldVal().getSchema().equals(_relDataEdit.getRelationSchema(_relCtx)))
				throw new RelationDataEditorException ("The schema is incompatible with this update");
			else
			{
				// Check that this value exists
				if (isKeyUsed(upd.getOldVal()))
				{
					int row = extendCacheUntilVisible (upd.getOldVal());
					delete(row);
				} 
				else
					throw new RelationDataEditorException ("The updated key does not exist");
			}
		}
	}
	
	private boolean isKeyUsed (Tuple tuple)
			throws RelationDataEditorException
	{
		if (isKeyUsedInLocalTrans(tuple))
			return true;
		else
			if (_relDataEdit.isKeyUsed(_relCtx, tuple))
				return !wasKeyChangedInLocalTrans (tuple);
		return false;
	}
	
	private boolean wasKeyChangedInLocalTrans (Tuple tuple)
	{
		Iterator<Update> itTrans = _transaction.iterator();
		while (itTrans.hasNext())
		{
			Update upd = itTrans.next();
			if (upd.isUpdate()  
				&& upd.getOldVal().getKeySubtuple().equals(tuple.getKeySubtuple()))
				return !upd.getOldVal().getKeySubtuple().equals(upd.getNewVal().getKeySubtuple());
		}
		return false;
	}
	
	private int extendCacheUntilVisible (Tuple tuple)
	{
		int res;
		while ((res = findInCache(tuple))==-1 && _cacheCanBeExt)
			extendCache();
		return res;
	}

	private int findInCache (Tuple tuple)
	{
		int res = -1;
		for (int row = 0 ; row < _cache.size() && res == -1 ; row++)
		{
			if (_cache.get(row).getKeySubtuple().equals(tuple.getKeySubtuple()))
				res = row;
		}
		return res;
	}
	
	
	
	/**
	 * Insert a new row into the cache and transaction
	 * @param scTuple
	 */
	public void insert (Tuple scTuple)
				throws RelationDataConstraintViolationException,
					   RelationDataEditorException
	{
		if (isKeyUsedInLocalTrans (scTuple)
				|| _relDataEdit.isKeyUsed(_relCtx, scTuple))
			throw new RelationDataConstraintViolationException ("Primary key violated");
		
		Update upd = new Update (null, scTuple);
		_cache.add(scTuple);			
		_transaction.add(upd);
		
		fireTableRowsInserted(_cache.size()-1, _cache.size()-1);
	}
	
	private boolean isKeyUsedInLocalTrans (Tuple tuple)
	{
		return isKeyUsedInLocalTrans(tuple, null);
	}
	
	
	private boolean isKeyUsedInLocalTrans (Tuple tuple,
											Update currUpd)
	{
		boolean isUsed = false;
		
		Iterator<Update> itUpd = _transaction.iterator();
		while (itUpd.hasNext() && !isUsed)
		{
			Update upd = itUpd.next();
			if (upd.getNewVal()!=null)
				isUsed = upd.getNewVal().getKeySubtuple().equals(tuple.getKeySubtuple());
		}
		
		return isUsed;
	}
	
	
	/**
	 * Is there something in the transaction?
	 * @return
	 */
	public boolean hasCurrentTransaction ()
	{
		return !_transaction.isEmpty();
	}
	
	protected RelationContext getRelationCtx ()
	{
		return _relCtx;
	}
	
	
	protected List<Update> getTransaction ()
	{
		return _transaction;
	}
	
	// Has been committed globally
	protected void clearTransaction ()
	{
		_transaction.clear();	
	}
	
	/**
	 * Commit the current transaction if any
	 *
	 */
	public void validateTransaction ()
	{
		if (hasCurrentTransaction())
		{
			//TODO: Use flag to know if should be decomposed into unit transactions
			// and if updates should become delete/insert
			try
			{
				_relDataEdit.addTransaction(_relCtx, _transaction);
				_transaction.clear();
			} catch (RelationDataEditorException ex)
			{
				//TODO
				ex.printStackTrace();
			}
		}
	}
	
	/**
	 * Rollback the current transaction if ant
	 */
	public void rollbackTransaction ()
	{
		for (Update upd : _transaction)
		{
			if (upd.isInsertion())
			{
				int ind = _cache.indexOf(upd.getNewVal());
				_cache.remove(ind);
				fireTableRowsDeleted(ind, ind);
			}
			if (upd.isUpdate())
			{
				int ind = _cache.indexOf(upd.getNewVal());
				_cache.set(ind, upd.getOldVal());
				fireTableRowsUpdated(ind, ind);
			}
			if (upd.isDeletion())
			{
				_cache.add(upd.getOldVal());
				fireTableRowsInserted(_cache.size()-1, _cache.size()-1);
			}
		}
		_transaction.clear();
	}
	
	
	/**
	 * Delete a given row
	 */
	public void delete (int row)
	{
		Update updPrev = findUpdate(row);
		Update upd=null;
		if (updPrev == null)
			upd = new Update (_cache.get(row), null);
		else
		{
			if (updPrev.isUpdate())
				upd = new Update (updPrev.getOldVal(), null);
			_transaction.remove(updPrev);
		}
		if (upd != null)
			_transaction.add (upd);
		_cache.remove(row);
		fireTableRowsDeleted(row, row);
	}
	
	
	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		if (_iterator!=null)
			_iterator.close ();
	}
}
