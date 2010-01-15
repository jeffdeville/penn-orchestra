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

package edu.upenn.cis.orchestra.reconciliation;

import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore.USException;

/**
 * Primarily a binding between integer codes and relations.
 * 
 * @author John Frommeyer
 * 
 */
public interface ISchemaIDBinding {

	/**
	 * Returns the relation with the requested ID.
	 * 
	 * @param relCode
	 * @return the relation with the requested ID
	 */
	public Relation getRelationFor(int relCode);

	/**
	 * Returns the schema for the named peer.
	 * 
	 * @param pid
	 * @return the schema for the named peer
	 * @throws USException
	 */
	public Schema getSchema(AbstractPeerID pid) throws USException;

	/**
	 * Returns the relation with the specific name.
	 * 
	 * @param nam
	 * @return the relation with the specific name
	 */
	public Relation getRelationNamed(String nam);

}
