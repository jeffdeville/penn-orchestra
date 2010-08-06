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

import static edu.upenn.cis.orchestra.OrchestraUtil.newHashMap;

import java.util.Collection;
import java.util.Map;

import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore.USException;

/**
 * An in-memory version of {@code ISchemaIDBinding}.
 * 
 * @author John Frommeyer
 * 
 */
public class LocalSchemaIDBinding implements ISchemaIDBinding {

	private final Map<Integer, Relation> idToRel = newHashMap();
	private final Map<String, Relation> nameToRel = newHashMap();
	private final Map<AbstractPeerID, Schema> pidToSchema;

	/**
	 * Construct a {@code ISchemaIDBinding} from the information contained in
	 * {@code peerIDToSchema}.
	 * 
	 * @param peerIDToSchema
	 * 
	 */
	public LocalSchemaIDBinding(Map<AbstractPeerID, Schema> peerIDToSchema) {
		pidToSchema = peerIDToSchema;
		Collection<Schema> schemas = pidToSchema.values();
		for (Schema s : schemas) {
			Collection<Relation> relations = s.getRelations();
			for (Relation relation : relations) {
				nameToRel.put(relation.getName(), relation);
				idToRel
						.put(Integer.valueOf(relation.getRelationID()),
								relation);
			}
		}
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see edu.upenn.cis.orchestra.reconciliation.ISchemaIDBinding#getRelationFor(int)
	 */
	@Override
	public Relation getRelationFor(int relCode) {
		return idToRel.get(Integer.valueOf(relCode));
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see edu.upenn.cis.orchestra.reconciliation.ISchemaIDBinding#getRelationNamed(java.lang.String)
	 */
	@Override
	public Relation getRelationNamed(String nam) {
		return nameToRel.get(nam);
	}

	@Override
	public Schema getSchema(AbstractPeerID pid) throws USException {
		return pidToSchema.get(pid);
	}

}
