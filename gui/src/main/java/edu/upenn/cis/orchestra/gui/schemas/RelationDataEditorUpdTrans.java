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
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationContext;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.StringType;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.datamodel.Update;
import edu.upenn.cis.orchestra.datamodel.exceptions.RelationNotFoundException;
import edu.upenn.cis.orchestra.datamodel.iterators.IteratorException;
import edu.upenn.cis.orchestra.datamodel.iterators.ResultIterator;
import edu.upenn.cis.orchestra.datamodel.iterators.ResultSetIterator;
import edu.upenn.cis.orchestra.exchange.BasicEngine;
import edu.upenn.cis.orchestra.mappings.Rule;

public class RelationDataEditorUpdTrans implements RelationDataEditorIntf 
{
	private OrchestraSystem _system;
	//private final Map<edu.upenn.cis.orchestra.repository.model.Schema, Schema> _schemasMap = new HashMap<edu.upenn.cis.orchestra.repository.model.Schema, Schema> (); 
	
	public RelationDataEditorUpdTrans (OrchestraSystem sys)
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
		for (Map.Entry<RelationContext, List<Update>> entry : transaction.entrySet())
			addTransaction(entry.getKey(), entry.getValue());
		
		return null;
	}
	
	public TxnPeerID addTransaction(RelationContext relationCtx, List<Update> transaction)
	throws RelationDataEditorException
	{
		for (Update upd : transaction)
		{
			if (upd.isInsertion())
				createInsertion (relationCtx, upd);
			if (upd.isDeletion())
				createDeletion(relationCtx, upd);
			if (upd.isUpdate())
			{
				createDeletion(relationCtx, upd);
				createInsertion (relationCtx, upd);
			}
		}
		return null;
	}
	
	private void createInsertion (RelationContext relCtx, Update upd)
				throws RelationDataEditorException		
	{
		Rule r = createRule (relCtx, upd.getNewVal());
		execRule (r, true);
		r = createRule (relCtx, upd.getNewVal(), "_L_INS");
		execRule (r, true);		
	}

	private void createDeletion (RelationContext relCtx, Update upd)
				throws RelationDataEditorException		
	{
		Rule r = createRule (relCtx, upd.getOldVal());
		execRule (r, false);
		r = createRule (relCtx, upd.getOldVal(), "_L_DEL");
		execRule (r, true);		
	}
	
	
	private void execRule (Rule r, boolean isInsert)
		throws RelationDataEditorException
	{
		try
		{
			r.getHead().setNeg(!isInsert);
			BasicEngine eng = _system.getMappingEngine();
			eng.evalUpdateRule(r);
		} catch (Exception ex)
		{
			throw new RelationDataEditorException (ex);
		}		
	}
	

	private Rule createRule (RelationContext relCtx, Tuple tuple)
				throws RelationDataEditorException
	{
		return createRule(relCtx, tuple,"");
	}


	private Rule createRule (RelationContext relCtx, Tuple tuple,
								String tableSuffix)
			throws RelationDataEditorException
	{
		StringBuffer buff = new StringBuffer ();
		buff.append(relCtx.toString());
		buff.append(tableSuffix);
		buff.append("(");
		for (int i = 0 ; i < relCtx.getRelation().getFields().size() ; i++)
		{
			buff.append ((i>0?",":""));
			if (relCtx.getRelation().getField(i).getType() instanceof StringType
					&& !tuple.isLabeledNull(i))
			{
				buff.append ("'");
				buff.append(tuple.get(i));

				buff.append ("'");
			}
			else
				buff.append(tuple.getValueOrLabeledNull(i).toString());
		}
		buff.append("):- ");
		System.out.println (buff.toString());
		Rule r=null;
		try
		{
			r = Rule.parse(_system, buff.toString());
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
		return r;
	}

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
		return relationCtx.getRelation();
		/*
		try
		{
			if (!_schemasMap.containsKey(relationCtx.getSchema()))
			{
				Schema sc = new Schema (relationCtx.getSchema());
				_schemasMap.put(relationCtx.getSchema(), sc);
			}
			Schema sc = _schemasMap.get(relationCtx.getSchema());
			return sc.getOrCreateRelationSchema(relationCtx.getRelation());
		} catch (BadColumnName ex)
		{
			assert false : "BadColumnName should not happen here";
			ex.printStackTrace();
			return null;
		}
			*/
	}

	public boolean isKeyUsed(RelationContext relCtx, Tuple row)
	throws RelationDataEditorException
	{
		//TODO: mutualize code but don't have time to make clean now...
		StringBuffer fieldsBuff = new StringBuffer ();
		for (int i = 0 ; i < relCtx.getRelation().getFields().size() ; i++)
		{
			if (relCtx.getRelation().getPrimaryKey().getFields().contains(relCtx.getRelation().getField(i)))
			{
				String value;				
				if (relCtx.getRelation().getField(i).getType() instanceof StringType
						&& !row.isLabeledNull(i))
					value = "'" + row.get(i).toString() + "'";
				else
					value = row.getValueOrLabeledNull(i).toString();
				fieldsBuff.append ((i>0?",":"") + value);
			}
			else
				fieldsBuff.append ((i>0?",":"") + "x" + i);
			
		}
		
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
		boolean res;
		try
		{
			res = result.hasNext();
			result.close();
		} catch (IteratorException ex)
		{
			throw new RelationDataEditorException (ex); 
		}
		return res;

	}
	
}
