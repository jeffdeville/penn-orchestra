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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datamodel.exceptions.CompareMismatch;
import edu.upenn.cis.orchestra.datamodel.exceptions.NotNumericalException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnsupportedTypeException;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.sql.ISqlConstant;
import edu.upenn.cis.orchestra.util.XMLParseException;



/**
 * Abstract class to represent an attribute in a tuple's schema.
 * @author Nick Taylor
 */
public abstract class Type implements Serializable {
	private static final long serialVersionUID = 1L;
	private final boolean nullable;
	private boolean labeledNullable;
	private final Class<?> classObj;
	
	/**
	 * Create a new schema element, which can be null
	 * 
	 * @param classObj			The java class associated with this type
	 */
	public Type(Class<?> classObj) {
		this(true,classObj);
	}
	
	/**
	 * Create a new schema element
	 * 
	 * @param nullable			<code>true</code> if this attribute can be null,
	 * 							<code>false</code> otherwise
	 * @param labeledNullable	<code>true</code> if this attribute can be a labelled null,
	 * 							<code>false</code> otherwise
	 * @param classObj			The java class associated with this type
	 */
	public Type(boolean nullable, boolean labeledNullable,Class<?> classObj) {
		this.nullable = nullable;
		this.labeledNullable = labeledNullable;
		this.classObj = classObj;
	}
	
	/**
	 * Create a new schema element
	 * 
	 * @param nullable			<code>true</code> if this attribute can be null,
	 * 							<code>false</code> otherwise
	 * @param classObj			The java class associated with this type
	 */
	public Type(boolean nullable,Class<?> classObj) {
		this(nullable,true,classObj);
	}
		
	/**
	 * Determine if this attribute is nullable 
	 * 
	 * @return	<code>true</code> if it is, <code>false</code> if it is not
	 */
	public boolean isNullable() {
		return nullable;
	}
	
	/**
	 * Determine if this attribute can be set to a labeled null value
	 * 
	 * @return <code>true</code> if it can, <code>false</code> if it cannot
	 */
	public boolean isLabeledNullable() {
		return labeledNullable;
	}
	
	public void setLabeledNullable() {
		setLabeledNullable(true);
	}
	
	public void setLabeledNullable(boolean l) {
		labeledNullable = l;
	}
	
	/**
	 * Get a string representing the type, in a SQL DDL statement,
	 * of this attribute.
	 * 
	 * @return			The SQL type
	 */
	final public String getSQLType() {
		return (isNullable() ? getSQLTypeName() : getSQLTypeName() + " NOT NULL");
	}
	
	/*
	 * Get a string representing the XML schema definition type
	 */
	public abstract String getXSDType();
	
	/**
	 * Compute the hash code for an object of this type.
	 * 
	 * @param o			The object to compute the hash code
	 * @return			The hash code
	 * @throws ValueMismatchException
	 */
	public int getHashCode(Object o) throws ValueMismatchException {
		if (o == null) {
			return 0;
		}
		/*
		// Let's skip this check
		if ( ! isValidForColumn(o)) {
			throw new ValueMismatchException(o, this);
		}
		*/
		return o.hashCode();
	}
	
	/**
	 * Compute the hash code for an object of this type.
	 * 
	 * @param o			The serialized representation of the object to compute the hash code for
	 * @return			The hash code
	 * @throws ValueMismatchException
	 */
	public int getHashCodeFromBytes(byte[] data, int offset, int length) {
		return fromBytes(data,offset,length).hashCode();
	}
	
	/**
	 * Get the string representation of a SQL literal of this type
	 * 
	 * @param o			The object to get the representation of
	 * @return			The string value of the SQL literal
	 */
	public abstract String getSQLLit(Object o);

	/**
	 * Get a string representing the type, in a SQL DDL statement,
	 * of this attribute. Does not include key, null, etc. information.
	 * 
	 * @return			The SQL type
	 */
	public abstract String getSQLTypeName();

	/**
	 * Gets the class object that should be associated with values for
	 * this field.
	 * 
	 * @return			The <code>Class</code> object
	 */
	public final Class<?> getClassObj() {
		return classObj;
	}

	/**
	 * Returns true if the <code>Object</code> passed in is compatible
	 * with this column. This method just checks class, override to perform
	 * more complex checking.
	 * 
	 * @param o		The <code>Object</code> to check
	 * @return		<code>true</code> if it is compatible, <code>false</code>
	 * 				otherwise
	 */
	public boolean isValidForColumn(Object o) {
		if (o == null)
			return nullable;
		else
			return (classObj == o.getClass());
	}

	/**
	 * Duplicates an object of the type associated with this column.
	 * 
	 * @param o			The object to duplicate
	 * @return			A deep copy of <code>o</code>
	 */
	public abstract Object duplicateValue(Object o);
	
	/**
	 * Calls compareTwo(t.get(leftIndex),o), but does so efficiently by
	 * not trying to make a copy of the value in the Tuple t
	 * 
	 * @param t				The tuple in which one value is located
	 * @param index			The index in that tuple of the left value in the comparison
	 * @param o				The right object in the comparison
	 * @return				The comparison value
	 */
	public Integer compare(AbstractTuple<?> t, int leftIndex, Object o) throws CompareMismatch {
		return compareTwo(t.getNoCopy(leftIndex), o);
	}
	
	/**
	 * Calls compareTwo(t.get(leftIndex),t.get(rightIndex)), but does so efficiently by
	 * not trying to make a copy of the values in the Tuple t
	 * 
	 * @param t				The tuple in which one value is located
	 * @param leftIndex		The index in that tuple of the left value for the comparison
	 * @param rightIndex	The index in that tuple of the right value for the comparison
	 * @return				The comparison value
	 */
	public Integer compare(AbstractTuple<?> t, int leftIndex, int rightIndex) throws CompareMismatch {
		return compareTwo(t.getNoCopy(leftIndex), t.getNoCopy(rightIndex));
	}
	/**
	 * Compare two objects of the class for this column, and return -1 
	 * if <code>o1</code> is less than <code>o2</code>, 0 if they are equal,
	 * and 1 if it is greater, or <code>null</code> if one of the two is
	 * null
	 * 
	 * @param o1			The first object
	 * @param o2			The second object
	 * @return				The comparison value described above
	 */
	public abstract Integer compareTwo(Object o1, Object o2) throws CompareMismatch;
	
	/**
	 * Adds two objects of this type. Nulls are treated as zero.
	 * 
	 * @param o1			The first object
	 * @param o2			The second object
	 * @return				Their sum
	 * @throws NotNumericalException			If this type cannot be added
	 * @throws ValueMismatchException		If the objects are not of this type
	 */
	public Object add(Object o1, Object o2) throws NotNumericalException, ValueMismatchException {
		throw new NotNumericalException(this);
	}
	
	/**
	 * Divide an object of this type by the specified integer
	 * 
	 * @param o				The object to divide
	 * @param divisor		The integer to divide by
	 * @return				Their quotient, or <code>null</code> if the divisor is zero
	 * @throws NotNumericalException		If the type cannot be divided
	 * @throws ValueMismatchException	If the object is not of this type
	 */
	public Object divide(Object o, int divisor) throws NotNumericalException, ValueMismatchException {
		throw new NotNumericalException(this);
	}
	
	/**
	 * Divide an object of this type by the specified integer
	 * 
	 * @param o				The object to divide
	 * @param multiplier	The integer to multiply by
	 * @return				Their quotient, or <code>null</code> if the divisor is zero
	 * @throws NotNumericalException		If the type cannot be divided
	 * @throws ValueMismatchException	If the object is not of this type
	 */
	public Object multiply(Object o, int multiplier) throws NotNumericalException, ValueMismatchException {
		throw new NotNumericalException(this);
	}
	
	/**
	 * Gets the type code from <code>java.sql.Types</code> associated with
	 * this type.
	 * 
	 * @return
	 */
	public abstract int getSqlTypeCode();
	
	/**
	 * Gets the {@code edu.upenn.cis.orchestra.sql.ISqlConstant.Type} associated with
	 * this type.
	 * 
	 * @return the {@code edu.upenn.cis.orchestra.sql.ISqlConstant.Type} associated with
	 * this type
	 */
	public abstract ISqlConstant.Type getSqlConstantType();
	
	/**
	 * Get a value out of a <code>ResultSet</code> into an <code>Object</code> compatible
	 * with this column type. Null values in the database are returned as a <code>null</code>.
	 * 
	 * @param rs				The <code>ResultSet</code> to get the data from
	 * @param colno				The column number of the desired column in <code>rs</code>
	 * @return					The <code>Object</code> containg the specified value
	 * @throws SQLException
	 */
	public abstract Object getFromResultSet(ResultSet rs, int colno) throws SQLException;

	/**
	 * Get a value out of a <code>ResultSet</code> into an <code>Object</code> compatible
	 * with this column type. Null values in the database are returned as a <code>null</code>.
	 * 
	 * @param rs				The <code>ResultSet</code> to get the data from
	 * @param colname			The name of the desired column in <code>rs</code>
	 * @return					The <code>Object</code> containing the specified value
	 * @throws SQLException
	 */
	public abstract Object getFromResultSet(ResultSet rs, String colname) throws SQLException;
	
	/**
	 * Set a wildcard in a prepared statement to an object of the specified type
	 * 
	 * @param o					The object of this type
	 * @param ps				The prepared statement 
	 * @param no				The parameter number (indexed from 1) in <code>ps</code>
	 * @throws SQLException
	 * @throws ClassCastException		If <code>o</code> has the wrong type
	 */
	public abstract void setInPreparedStatement(Object o, PreparedStatement ps, int no) throws SQLException;
	
	/**
	 * Determine if the derived components of the type (i.e. everything but its nullability) are equal to one another
	 * 
	 * @param t					The type to compare to
	 * @return					<code>true</code> if the two are equal, <code>false</code> if not
	 */
	public abstract boolean typeEquals(Type t);
	
	/**
	 * Get a canonical byte representation from an object of this type
	 * 
	 * @param o				The object of this type
	 * 
	 * @return				The byte array encoding the value of the object,
	 * 						or <code>null</code> if <code>o</code> is null
	 */
	public abstract byte[] getBytes(Object o);
	
	/**
	 * Get the length of the byte representation of a value of this type, in bytes.
	 * 
	 * @return					The serialized length of all values of this type,
	 * 							or <code>-1</code> if the length varies
	 */
	public abstract int bytesLength();
	
	/**
	 * Reconstruct an object of this type from its byte representation
	 * 
	 * @param bytes			An array containing the byte representation
	 * @param offset		The offset into the byte array of the
	 * 						data for this object
	 * @param length		The data for this object if the byte array
	 * @return				An object of the appropriate type
	 */
	public abstract Object fromBytes(byte[] bytes, int offset, int length);
	
	/**
	 * Reconstruct an object of this type from its byte representation
	 * 
	 * @param bytes			An array consisting of only the byte representation
	 * @return				An object of the appropriate type
	 */
	public final Object fromBytes(byte[] bytes) {
		return fromBytes(bytes, 0, bytes.length);
	}
	
	/**
	 * Get a canonical String representation of an object of this type
	 * 
	 * @param o				The object of this type
	 * @return				Its canonical string representation
	 * @throws NullPointerException		if <code>o</code> is null
	 */
	public abstract String getStringRep(Object o) throws NullPointerException, ValueMismatchException;
	
	/**
	 * Decode an object of this type from its canonical String representation
	 * 
	 * @param rep			The string representation of an object of this type
	 * @return				An object of this type
	 */
	public abstract Object fromStringRep(String rep) throws XMLParseException;
	
	//@Override
	public boolean equalsREMOVE(Object o) {
		if (! (o instanceof Type))
			return false;
		Type t = (Type) o;
		
		if (this.nullable != t.nullable || t.labeledNullable != this.labeledNullable) {
			return false;
		}
		return typeEquals(t);
	}
	
	//@Override
	public int hashCodeREMOVE() {
		int code = 17;
		code = 31 * code + (nullable ? 1 : 0);
		code = 31 * code + (labeledNullable ? 1 : 0);
		return code;
	}
	
	/**
	 * Return true if the values from this type can be stored in the supplied type
	 * 
	 * @param t
	 * @return
	 */
	public boolean canPutInto(Type t) {
		if (t == null) {
			return false;
		}
		if (this.labeledNullable && (! t.labeledNullable)) {
			return false;
		}
		if (this.nullable && (! t.nullable)) {
			return false;
		}
		return (t.getClass() == this.getClass());
	}
	
	/**
	 * Return true if variables from the supplied type can be read into this type
	 * 
	 * @param t
	 * @return
	 */
	public boolean canReadFrom(Type t) {
		if (t == null) {
			return false;
		}
		if (t.labeledNullable && (! this.labeledNullable)) {
			return false;	
		}
		if (t.nullable && (! this.nullable)) {
			return false;
		}
		return (t.getClass() == this.getClass());
	}

	public final String toString() {
		return getSQLType();
	}
	
	public final void serialize(Document doc, Element field) {
		field.setAttribute("nullable", Boolean.toString(nullable));
		field.setAttribute("labeledNullable", Boolean.toString(labeledNullable));
		subclassSerialize(doc, field);
	}
	
	public abstract void subclassSerialize(Document doc, Element field);
	
	public abstract edu.upenn.cis.orchestra.logicaltypes.OptimizerType getOptimizerType();

	public static Type deserialize(Element field) throws XMLParseException, UnsupportedTypeException {
		String type = field.getAttribute("type");
		boolean nullable = field.hasAttribute("nullable") ? 
				Boolean.parseBoolean(field.getAttribute("nullable")) : true;
		boolean labeledNullable = field.hasAttribute("labeledNullable") ? 
						Boolean.parseBoolean(field.getAttribute("labeledNullable")) : true;
		Type retval = deserialize(type, nullable, labeledNullable);
		if (retval == null) {
			throw new XMLParseException("Invalid type definition", field);
		}
		return retval;
	}
	
	public static Type deserialize(String type, boolean nullable, boolean labeledNullable) throws UnsupportedTypeException {
		Type t;
		if ((t = DateType.deserialize(type, nullable, labeledNullable)) != null
			|| (t = TimestampType.deserialize(type, nullable, labeledNullable)) != null
			|| (t = DoubleType.deserialize(type, nullable, labeledNullable)) != null
			|| (t = IntType.deserialize(type, nullable, labeledNullable)) != null
			|| (t = LongType.deserialize(type, nullable, labeledNullable)) != null
			|| (t = StringType.deserialize(type, nullable, labeledNullable)) != null
			|| (t = BoolType.deserialize(type, nullable, labeledNullable)) != null
			|| (t = ClobType.deserialize(type, nullable, labeledNullable)) != null)
		{
			return t;
		} else
			throw new UnsupportedTypeException(type);
		//return null;
	}
	
	/**
	 * Determine if serialized representations of data of
	 * this type can be compared for equality using byte-for-byte
	 * equality
	 * 
	 * @return			<code>true</code> if they can,
	 * 					<code>false</code> if they can't
	 */
	public boolean canCompareSerialized() {
		return true;
	}
	
	/**
	 * Get a comparator for the serialized representations of values of this
	 * type. It will be serializable.
	 * 
	 * @return			The comparator
	 */
	public abstract Comparator<byte[]> getSerializedComparator();
}
