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

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datamodel.exceptions.UnsupportedTypeException;
import edu.upenn.cis.orchestra.util.XMLParseException;

/*******************************************************************************
 * Class used to describe a relation field (equivalent to a table column for
 * relational). Types are used as JDBC types.
 * 
 * @author Olivier Biton
 * @see java.sql.Types
 *      ****************************************************************
 */
public class RelationField implements Serializable {
	public static final long serialVersionUID = 1L;

	/** Field name */
	private String _name;
	/** Field description */
	private String _description;
	/** Orchestra type */
	private Type _type;
	
	/** A default value, if the column is to be added */
	private String _defaultValueAsString = null;

	private AbstractRelation _rel;

	/**
	 * The extension used for labeled null columns.
	 */
	public static String LABELED_NULL_EXT = "_LN";
	public static String ANN_EXT = "_TRUST";
	
	/**
	 * The default value for a non-null attribute's _LN column
	 */
	public static String LABELED_NULL_DEFAULT = "1";

	/**
	 * Creates a new field
	 * 
	 * @param name
	 *            Field's name
	 * @param description
	 *            Field's description
	 * @param isNullable
	 *            True if the field can contain null values
	 * @param dbType
	 *            Database type (RDBMS specific)
	 * @throws UnsupportedTypeException
	 */
	public RelationField(String name, String description, boolean isNullable,
			String dbType) throws UnsupportedTypeException {
		_name = name;
		_description = description;
		_type = Type.deserialize(dbType, isNullable, true);
		// assert(_type != null);
	}

	public RelationField(String name, String description, Type type) {
		_name = name;
		_description = description;
		_type = type;
		assert (_type != null);
	}

	/**
	 * Deep copy of a given field. <BR>
	 * Use the method deepCopy to benefit from polymorphism
	 * 
	 * @param field
	 *            Field to copy
	 * @see RelationField#deepCopy()
	 */
	protected RelationField(RelationField field) {
		_name = field.getName();
		_description = field.getDescription();
		_type = field.getType();
		assert (_type != null);
	}

	public Type getType() {
		return _type;
	}

	/**
	 * Get the field's name
	 * 
	 * @return Field's name
	 * @roseuid 44AD2CE8006D
	 */
	public String getName() {
		return _name;
	}
	
	/**
	 * If we have to add this column into the DBMS,
	 * what's its default value (in string form for SQL gen)
	 * 
	 * @return
	 */
	public String getDefaultValueAsString() {
		return _defaultValueAsString;
	}
	
	/**
	 * Set the default value (in string form) for SQL gen, in
	 * case we need to add this column
	 * 
	 * @param s
	 */
	public void setDefaultValueAsString(String s) {
		_defaultValueAsString = s;
	}

	/**
	 * Get the field's description
	 * 
	 * @return Field's description
	 */
	public String getDescription() {
		return _description;
	}

	/**
	 * Get the field's type, as defined in the JDBC API
	 * 
	 * @return Fields type
	 * @see java.sql.Types
	 */
	public int getSqlTypeCode() {
		return _type.getSqlTypeCode();
	}

	/**
	 * Is this field nullable?
	 * 
	 * @return True if the field can contain null values
	 */
	public boolean isNullable() {
		return _type.isNullable();
	}

	/**
	 * Get a deep copy of the field
	 * 
	 * @return Deep copy of this field
	 * @see RelationField#ScField(RelationField)
	 */
	public RelationField deepCopy() {
		return new RelationField(this);
	}

	/**
	 * Returns a description of the field, conforms to the flat file format
	 * defined in <code>RepositoryDAO</code>
	 * 
	 * @return Field's description
	 */
	public String toString() {
		return getName() + " " + _type.getSQLType() + " " + _type.toString();
	}

	/**
	 * Get the database type (RDMBS specific)
	 * 
	 * @return Database type
	 */
	public String getSQLType() {
		return _type.getSQLType();
	}

	/**
	 * Get the database type (DON'T APPEND "NOT NULL")
	 * 
	 * @return
	 */
	public String getSQLTypeName() {
		return _type.getSQLTypeName();
	}

	public void serialize(Document doc, Element field) {
		field.setAttribute("name", _name);
		field.setAttribute("description", _description);
		_type.serialize(doc, field);
	}

	public static RelationField deserialize(Element field)
			throws XMLParseException, UnsupportedTypeException {
		String name = field.getAttribute("name");
		String description = field.getAttribute("description");
		Type type = Type.deserialize(field);
		return new RelationField(name, description, type);
	}

	public boolean equals(RelationField other) {
//		return (_name.equals(other._name) && _type.equals(other._type) && _description
//		.equals(other._description));

		if(_name.equals(other._name) && _type.equals(other._type) && _description
				.equals(other._description)){
			if((_rel == null && other._rel == null) || 
				(_rel != null && other._rel != null && _rel.equals(other._rel))){
				return true;
			}else{
				return false;
			}
		}else{
			return false;
		}
	}

	public boolean equals(Object oth) {
		if (oth instanceof RelationField) {
			return equals((RelationField) oth);
		} else {
			return false;
		}
	}

	public int hashCode() {
		return _name.hashCode();
	}

	/**
	 * Return <code>true</code> if <code>columnName</code> follows the
	 * labeled null naming convention, <code>false</code> otherwise.
	 * 
	 * @param columnName
	 *            name to test.
	 * @return see description.
	 */
	public static boolean isLabeledNull(String columnName) {
		return columnName.endsWith(LABELED_NULL_EXT);
	}

	public void setRelation(AbstractRelation rel) {
		_rel = rel;
	}

	public AbstractRelation getRelation() {
		return _rel;
	}

}
