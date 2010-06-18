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

import java.util.List;
import java.util.Map;

import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationContext;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.datamodel.Update;
import edu.upenn.cis.orchestra.datamodel.iterators.ResultIterator;

public interface RelationDataEditorIntf 
{
	public ResultIterator<Tuple> getData (RelationContext relationCtx)
					throws RelationDataEditorException; 
	
	// Single update transaction 
	public void addUpdate (RelationContext relationCtx, Update update)
					throws RelationDataEditorException;
	
	public TxnPeerID addTransaction (RelationContext relationCtx, List<Update> transaction)
					throws RelationDataEditorException;

	public TxnPeerID addTransaction (Map<RelationContext,List<Update>> transaction)
					throws RelationDataEditorException;
	
	public Relation getRelationSchema (RelationContext relationCtx)
					throws RelationDataEditorException;
	
	public boolean isKeyUsed(RelationContext relCtx, Tuple row)
					throws RelationDataEditorException;

}
