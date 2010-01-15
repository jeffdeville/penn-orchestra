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

import java.util.List;

import edu.upenn.cis.orchestra.datamodel.exceptions.UnknownRefFieldException;

/****************************************************************
 * Unique index definition
 * @author Olivier Biton
 *****************************************************************
 */
public class RelationIndexNonUnique extends RelationIndex {
	private static final long serialVersionUID = 1L;

	/**
	 * Create a new non unique index
	 * @param name Constraint name
	 * @param rel Relation containing the unique index
	 * @param fields Fields concerned by the UNIQUE constraint
	 * @throws UnknownRefFieldException If a unique index field is unknown
	 */
	public RelationIndexNonUnique(String name, Relation rel, List<String> fields)
			throws UnknownRefFieldException
   {
		super (name, rel, fields);
   }
	
	/**
	 * Create a new non unique index
	 * @param name Constraint name
	 * @param rel Relation containing the unique index
	 * @param fields Fields concerned by the UNIQUE constraint
	 * @throws UnknownRefFieldException If a unique index field is unknown
	 */
	public RelationIndexNonUnique(String name, Relation rel, String[] fields)
			throws UnknownRefFieldException
   {
		super (name, rel, fields);
   }	

	
   /**
    * Returns a description of the unique index, conforms to the 
    * flat file format defined in <code>RepositoryDAO</code>
    * @return Unique index description
    */       	
	public String toString ()
    {
	   String description = "NON UNIQUE INDEX " + super.toString();
	   return description;
    } 	

}
