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
import java.util.Collections;
import java.util.List;

import edu.upenn.cis.orchestra.datamodel.exceptions.UnknownRefFieldException;

/****************************************************************
 * Abstract class used as a basis for relations constraints
 * It only has a name and a list of fields.
 * @author Olivier Biton
 *****************************************************************
 */
public abstract class IntegrityConstraint implements Serializable
{
	public static final long serialVersionUID = 1L;
	
   /**
    * Constraint name
    */
   protected final String _name;
   /**
    * Fields on which the constraint will apply. An immutable list
    */
   protected final List<RelationField> _fields;

   /**
    * Relation to which the constraint applies
    */
   protected final String _relName;
   
   /**
    * New constraint
    * @param name Constraint name
    * @param rel Relation to which the constraints applied
    * @param fields Fields on which it applies
    * @throws UnknownRefFieldException If a field in the list does not exist in the relation
    * @roseuid 449AEAF80177
    */
   public IntegrityConstraint(String name, AbstractRelation rel, List<String> fields) 
   				throws UnknownRefFieldException
   {
	   this (name, rel, fields.toArray(new String[0]));
   }
   
   /**
    * New constraint
    * @param name Constraint name
    * @param rel Relation to which the constraints applied
    * @param fields Fields on which it applies
    * @throws UnknownRefFieldException If a field in the list does not exist in the relation
    */
   public IntegrityConstraint(String name, AbstractRelation rel, String[] fields) 
   				throws UnknownRefFieldException
   {
	   if (name == null || name.length() == 0) {
		   throw new IllegalArgumentException("Cannot create an integrity constraint with no name");
	   }
	   _name = name;
	   _relName = rel.getName();
	   
	   List<RelationField> fieldsList = new ArrayList<RelationField>(fields.length);
	   for (String field : fields)
	   {
		   if (rel.getField(field)==null)
			   throw new UnknownRefFieldException ("Constraint " + name + " references unknown field " + rel.getName() + "." + field);
		   else
			   fieldsList.add (rel.getField(field));
	   }
	   _fields = Collections.unmodifiableList(fieldsList);
   }
   
   public String getRelation() {
	   return _relName;
   }
   
   /**
    * Get the list of fields on which the constraint applies
    * @return fields
    * @roseuid 449AE9D60399
    */
   public List<RelationField> getFields() 
   {
    return _fields;
   }
   

   
   /**
    * Get the constraint's name
    * @return Constraint's name
    * @roseuid 44AD2D9A0399
    */
   public String getName() 
   {
    return _name;
   }
   

   /**
    * Returns a description of the constraint, conforms to the 
    * flat file format defined in <code>RepositoryDAO</code>
    * @return Constraint's description
    */
   public String toString ()
   {
	   String description;
	   
	   description = getName() + "(";
	   boolean firstField = true;
	   for (RelationField fld : getFields())
	   {
		   description += (firstField?"":", ") + fld.getName();
		   firstField = false;
	   }
	   description += ")";
	   
	   return description;
   }
   
   

   
   
   protected boolean constraintEquals(IntegrityConstraint c) {
	   return _name.equals(c._name) && _relName.equals(c._relName) && _fields.equals(c._fields); 
   }
   
   protected int constraintHashCode() {
	   return _name.hashCode() + 31 *_relName.hashCode() + 61 * _fields.hashCode();
   }
}
