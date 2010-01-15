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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import edu.upenn.cis.orchestra.datamodel.exceptions.UnknownRefFieldException;

/****************************************************************
 * Primary key definition
 * @author Olivier Biton
 *****************************************************************
 */
public class PrimaryKey extends RelationIndex implements Serializable
{
	public static final long serialVersionUID = 1L;

	/**
	 * Create a new primary key
	 * @param pkName Constraint's name
	 * @param rel Relation containing the primary key
	 * @param fields Fields used in the primary key
	 * @throws UnknownRefFieldException If a primary key field is unknown
	 * @roseuid 449AEEFD036B
	 */
	public PrimaryKey(String pkName, AbstractRelation rel, Collection<String> fields) 
	throws UnknownRefFieldException	
	{
		super(pkName, rel, sortByPosition(rel,fields));
	}

	/**
	 * Create a new primary key
	 * @param pkName Constraint's name
	 * @param rel Relation containing the primary key
	 * @param fields Fields used in the primary key
	 * @throws UnknownRefFieldException If a primary key field is unknown
	 * @roseuid 449AEEFD036B
	 */
	public PrimaryKey(String pkName, AbstractRelation rel, String[] fields) 
	throws UnknownRefFieldException
	{
		super(pkName, rel, sortByPosition(rel,Arrays.asList(fields)));
	}

	/**
	 * Returns a description of the primary key, conforms to the 
	 * flat file format defined in <code>RepositoryDAO</code>
	 * @return String description
	 */       
	public String toString ()
	{
		String description = "PRIMARY KEY " + super.toString() ;
		return description;
	}


	static private List<String> sortByPosition(final AbstractRelation rel, Collection<String> cols) {
		for (String col : cols) {
			if (rel.getColNum(col) == null) {
				throw new IllegalArgumentException("Column " + col + " is not in relation " + rel.getName());
			}
		}
		ArrayList<String> sortedCols = new ArrayList<String>(cols);
		Collections.sort(sortedCols, new Comparator<String>() {

			public int compare(String arg0, String arg1) {
				return rel.getColNum(arg0) - rel.getColNum(arg1);
			}
			
		});
		return sortedCols;
	}
}
