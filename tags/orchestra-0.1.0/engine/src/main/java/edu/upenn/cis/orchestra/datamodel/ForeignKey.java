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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datamodel.exceptions.UnknownRefFieldException;
import edu.upenn.cis.orchestra.util.DomUtils;


/****************************************************************
 * Foreign key definition
 * @author Olivier Biton
 * ****************************************************************
 */
public class ForeignKey extends IntegrityConstraint implements Serializable 
{
	public static final long serialVersionUID = 1L;

	/** Relation referenced by the foreign key */
   protected final AbstractRelation _refRelation; 
   /** Fields referenced by the foreign key. These fields must be part of 
    * _refRelation's primary key. The order is not the primary key's order, 
    * it is changed to match the foreign key fields order (ScConstraint).
    * The list is immutable, so it is safe to return
    */
   protected final List<RelationField> _refFields;
   
   
   /**
    * Create a new foreign key
    * @param fkName Constraint's name
    * @param rel Relation containing the foreign key
    * @param fields List of fields concerned by the foreign key
    * @param refRelation Relation containing the primary key referenced by the foreign key
    * @param refFields Referenced primary key fields ordered to match the foreign key fields order  
    * @throws UnknownRefFieldException If a field in the constraint fields list or in the referenced
    * 				fields list does not exist 
    */
   public ForeignKey(String fkName, AbstractRelation rel, List<String> fields, AbstractRelation refRelation, List<String> refFields)
   				throws UnknownRefFieldException {
	   this (fkName, rel, fields, refRelation, refFields, false);
   
   }
   
   /**
    * Create a new foreign key
    * @param fkName Constraint's name
    * @param rel Relation containing the foreign key
    * @param fields List of fields concerned by the foreign key
    * @param refRelation Relation containing the primary key referenced by the foreign key
    * @param refFields Referenced primary key fields ordered to match the foreign key fields order  
    * @param sloppy	If set, don't check that foreign key references primary key
    * @throws UnknownRefFieldException If a field in the constraint fields list or in the referenced
    * 				fields list does not exist 
    */
   public ForeignKey(String fkName, AbstractRelation rel, List<String> fields, AbstractRelation refRelation, List<String> refFields, boolean sloppy)
   				throws UnknownRefFieldException
   {
	   this (fkName, rel, fields.toArray(new String[fields.size()]), refRelation, refFields.toArray(new String[refFields.size()]), sloppy);
   
   }
   
   /**
    * Create a new foreign key
    * @param fkName Constraint's name
    * @param rel Relation containing the foreign key
    * @param fields List of fields concerned by the foreign key
    * @param refRelation Relation containing the primary key referenced by the foreign key
    * @param refFields Referenced primary key fields ordered to match the foreign key fields order
    * @param sloppy	If set, don't check that foreign key references primary key
    * @throws UnknownRefFieldException If a field in the constraint fields list or in the referenced
    * 				fields list does not exist 
    */
   public ForeignKey(String fkName, AbstractRelation rel, String[] fields, AbstractRelation refRelation, String[] refFields)
   				throws UnknownRefFieldException {
	   this(fkName, rel, fields, refRelation, refFields, false);
   }
   /**
    * Create a new foreign key
    * @param fkName Constraint's name
    * @param rel Relation containing the foreign key
    * @param fields List of fields concerned by the foreign key
    * @param refRelation Relation containing the primary key referenced by the foreign key
    * @param refFields Referenced primary key fields ordered to match the foreign key fields order
    * @param sloppy	If set, don't check that foreign key references primary key
    * @throws UnknownRefFieldException If a field in the constraint fields list or in the referenced
    * 				fields list does not exist 
    */
   public ForeignKey(String fkName, AbstractRelation rel, String[] fields, AbstractRelation refRelation, String[] refFields, boolean sloppy)
   				throws UnknownRefFieldException {
	   super (fkName, rel, fields);
	   _refRelation = refRelation; 
	   
	   List<RelationField> fieldsList = new ArrayList<RelationField>(fields.length);
	   
	   Set<RelationField> fieldsSet = new HashSet<RelationField>(fields.length);
	   
	   for (String field : refFields)
	   {
		   RelationField found = refRelation.getField(field);
		   if (found ==null) {
			   throw new UnknownRefFieldException ("Foreign key " + fkName + " references unknown field " + refRelation.getName() + "." + field);
		   } else {
			   fieldsList.add(found);
			   fieldsSet.add(found);
		   }
	   }
	   if (!(sloppy || fieldsSet.containsAll(refRelation.getPrimaryKey().getFields()))) {
		   throw new IllegalArgumentException("Foreign key " + fkName + " must reference all elements of primary key in referenced relation");
	   }
	   _refFields = Collections.unmodifiableList(fieldsList);
   }
   
   /**
    * Get the relation containing the primary key referenced by the foreign key
    * @return Referenced relation
    * @roseuid 449AE9F80222
    */
   public AbstractRelation getRefRelation() 
   {
	   return _refRelation;
   }
   
   /**
    * Get the list of referenced primary key fields ordered to match the foreign 
    * key fields order.
    * @return List of referenced primary key fields
    * @roseuid 449AEA07005D 
    */
   public List<RelationField> getRefFields() 
   {
    return _refFields;
   }
   
   
   /**
    * Returns a description of the foreign key, conforms to the 
    * flat file format defined in <code>RepositoryDAO</code>
    * @return Foreign key's description
    */      
   public String toString ()
   {
	   String description;
	   
	   description = "FOREIGN KEY " + super.toString();
	   
	   description += " REFERENCES " + getRefRelation().getName() + "(";
	   boolean first = true;
	   for (RelationField fld : getRefFields())
	   {
		   description += fld.getName() + (first?"":",");
		   first = false;
	   }
	   description += ")";
	   
	   return description;
   }
   
   public void serialize(Document doc, Element foreignkey) {
	   foreignkey.setAttribute("name", _name);
	   foreignkey.setAttribute("source", _relName);
	   foreignkey.setAttribute("target", _refRelation.getName());
	   for (RelationField f : _fields) {
		   DomUtils.addChildWithText(doc, foreignkey, "from", f.getName());
	   }
	   for (RelationField f : _refFields) {
		   DomUtils.addChildWithText(doc, foreignkey, "to", f.getName());
	   }
   }
   
   static public ForeignKey deserialize(Schema schema, Element foreignkey) 
   			throws UnknownRefFieldException {
	   String name = foreignkey.getAttribute("name");
	   String source = foreignkey.getAttribute("source");
	   String target = foreignkey.getAttribute("target");
	   ArrayList<String> from = new ArrayList<String>();
	   ArrayList<String> to = new ArrayList<String>();
	   for (Element e : DomUtils.getChildElementsByName(foreignkey, "from")) {
		   from.add(e.getTextContent());
	   }
	   for (Element e : DomUtils.getChildElementsByName(foreignkey, "to")) {
		   to.add(e.getTextContent());
	   }
	   
	   Relation src = null; 
	   Relation tgt = null;
	   try{
		   src = schema.getRelation(source);
	   }catch(Exception e){}
	   try{
		   tgt = schema.getRelation(target);
	   }catch(Exception e){}
	   
	   return new ForeignKey(name, src, from, tgt, to);
   }
   
   public int hashCode() {
	   return constraintHashCode() + 127 * _refFields.hashCode() + 251 * _refRelation.getName().hashCode();
   }
   
   public boolean equals(Object o) {
	   if (o == null || o.getClass() != this.getClass()) {
		   return false;
	   }
	   ForeignKey fk = (ForeignKey) o;
	   
	   if (! _refRelation.getName().equals(fk._refRelation.getName())) {
		   return false;
	   }
	   
	   if (! _refFields.equals(fk._refFields)) {
		   return false;
	   }
	   
	   return constraintEquals(fk);
   }
}
