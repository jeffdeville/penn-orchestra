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
package edu.upenn.cis.orchestra.datamodel;


import java.io.Serializable;
import java.util.List;

import edu.upenn.cis.orchestra.datamodel.exceptions.UnknownRefFieldException;


/**
 * This class has been introduced for specific index information such as index statistics.
 * Even though an index when it's non unique is not a constraint it's easier to have all the indexes
 * inheriting from constraint since the attributes are the same 
 * @author Olivier Biton
 *
 */
public abstract class RelationIndex extends IntegrityConstraint implements Serializable {
	public static final long serialVersionUID = 1L;
	
   /**
    * New index
    * @param name Index name
    * @param rel Relation to which the index applies
    * @param fields Names of fields on which it applies
    * @throws UnknownRefFieldException If a field in the list does not exist in the relation
    */	
	public RelationIndex(String name, AbstractRelation rel, List<String> fields) 
				throws UnknownRefFieldException {
		super(name, rel, fields);
	}

   /**
    * New index
    * @param name Index name
    * @param rel Relation to which the index applies
    * @param fields Names of fields on which it applies
    * @throws UnknownRefFieldException If a field in the list does not exist in the relation
    */	
	public RelationIndex(String name, AbstractRelation rel, String[] fields) throws UnknownRefFieldException {
		super(name, rel, fields);
	}
	
	

}
