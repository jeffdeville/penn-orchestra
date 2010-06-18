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

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationContext;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.datamodel.Update;
import edu.upenn.cis.orchestra.datamodel.exceptions.RelationNotFoundException;
import edu.upenn.cis.orchestra.datamodel.iterators.ResultIterator;
import edu.upenn.cis.orchestra.datamodel.iterators.ResultSetIterator;
import edu.upenn.cis.orchestra.exchange.BasicEngine;
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.reconciliation.Db;
import edu.upenn.cis.orchestra.reconciliation.DbException;

public class RelationDataEditor implements RelationDataEditorIntf 
{
	private OrchestraSystem _system;
	
	public RelationDataEditor (OrchestraSystem sys)
	{
		_system = sys;
	}
	
	
	public void addUpdate(RelationContext relationCtx, Update update)
					throws RelationDataEditorException
	{
		List<Update> updates = new ArrayList<Update> (1);
		updates.add (update);
		addTransaction(relationCtx, updates);
	}
	
	public TxnPeerID addTransaction(Map<RelationContext, List<Update>> transaction) throws RelationDataEditorException {
		final List<Update> cpleteTrans = new ArrayList<Update> ();
		for (List<Update> upd : transaction.values())
			cpleteTrans.addAll(upd);
		Peer p = transaction.keySet().iterator().next().getPeer();
		return addTransaction(p, cpleteTrans);
	}
	
	public TxnPeerID addTransaction(RelationContext relationCtx, List<Update> transaction)
	throws RelationDataEditorException
	{
		return addTransaction(relationCtx.getPeer(), transaction);
	}
	
	private TxnPeerID addTransaction(Peer p, List<Update> transaction)
	throws RelationDataEditorException
	{
		try
		{
			Db db = _system.getRecDb(p.getId());
			return db.addTransaction(transaction);
		} catch (DbException ex)
		{
			throw new RelationDataEditorException (ex);
		}
	}	

/*	public ResultIterator<RelationTuple> getData(RelationContext relationCtx)
					throws RelationDataEditorException
	{		
		try
		{
			Db db = _system.getRecDb(relationCtx.getPeer().getId());
			return db.getRelationContents(relationCtx.getRelation().getName());
		} catch (DbException ex)
		{
			throw new RelationDataEditorException (ex);
		}
	}
*/
	public ResultIterator<Tuple> getData(RelationContext relCtx)
	throws RelationDataEditorException
	{		
		StringBuffer fieldsBuff = new StringBuffer ();
		for (int i = 0 ; i < relCtx.getRelation().getFields().size() ; i++)
			fieldsBuff.append ((i>0?",":"") + "x" + i);

		StringBuffer ruleBuff = new StringBuffer ();
		ruleBuff.append(relCtx.toString());
		ruleBuff.append("(");
		ruleBuff.append(fieldsBuff);
		ruleBuff.append("):-");
		ruleBuff.append(relCtx.toString());
		ruleBuff.append("(");
		ruleBuff.append(fieldsBuff);
		ruleBuff.append(")");

		ResultSetIterator<Tuple> result = null;
		try
		{
			Rule r = Rule.parse(_system, ruleBuff.toString());
			BasicEngine eng = _system.getMappingEngine();
			result = eng.evalQueryRule(r);
		} catch (RelationNotFoundException ex)
		{
			assert false: "RelationNotFoundException should not happen here";
		ex.printStackTrace();
		} catch (ParseException ex)
		{
			assert false: "ParseException should not happen here";
		ex.printStackTrace();			
		} catch (Exception ex)
		{
			ex.printStackTrace();
			throw new RelationDataEditorException (ex);
		}

		return result;
	}
	
	public Relation getRelationSchema(RelationContext relationCtx) 
				throws RelationDataEditorException
	{		
		try
		{
			Db db = _system.getRecDb(relationCtx.getPeer().getId());
			return db.getSchema().getOrCreateRelationSchema(relationCtx.getRelation());
		} catch (Exception ex)
		{
			throw new RelationDataEditorException (ex);
		}
	}

	public boolean isKeyUsed(RelationContext relCtx, Tuple row)
	throws RelationDataEditorException
	{
		try
		{
			Db db = _system.getRecDb(relCtx.getPeer().getId());
			return (db.getValueForKey(db.getCurrentRecno(), row)!=null);
		} catch (DbException ex)
		{
			throw new RelationDataEditorException (ex);
		}		
	}
	
}
