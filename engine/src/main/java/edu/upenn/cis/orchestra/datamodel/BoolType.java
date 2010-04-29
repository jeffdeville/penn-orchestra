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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datamodel.exceptions.CompareMismatch;
import edu.upenn.cis.orchestra.datamodel.exceptions.NotNumericalException;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.sql.ISqlConstant;
import edu.upenn.cis.orchestra.util.XMLParseException;


/**
 * @author Nick Taylor
 * 
 * Class to represent a Boolean-valued attribute in a schema.
 *
 */
public class BoolType extends Type {
	private static final long serialVersionUID = 1L;
	public final static int bytesPerBool = 1;

	public static final BoolType BOOL = new BoolType(true, true);

	public BoolType(boolean nullable, boolean labeledNullable) {
		super(nullable,labeledNullable,Boolean.class);
	}

	public String getSQLTypeName() {
		return "BOOL";
	}

	public String getXSDType() {
		return "xsd:boolean";
	}

	public Object duplicateValue(Object o) {
		// Integers are immutable, so no need to duplicate
		return o;
	}

	public Integer compareTwo(Object o1, Object o2) throws CompareMismatch {
		if (o1 == null || o2 == null) {
			return null;
		}
		if (o1.getClass() != Boolean.class || o2.getClass() != Boolean.class) {
			throw new CompareMismatch(o1, o2, Integer.class);
		}
		Boolean i1 = (Boolean) o1;
		Boolean i2 = (Boolean) o2;
		return i1.compareTo(i2);
	}

	public int getSqlTypeCode() {
		return java.sql.Types.BOOLEAN;
	}

	public Object getFromResultSet(ResultSet rs, int colno) throws SQLException {
		boolean i = rs.getBoolean(colno);
		if (rs.wasNull()) {
			return null;
		} else {
			return i;
		}
	}

	public Object getFromResultSet(ResultSet rs, String colname) throws SQLException {
		boolean i = rs.getBoolean(colname);
		if (rs.wasNull()) {
			return null;
		} else {
			return i;
		}
	}

	public boolean typeEquals(Type t) {
		if (t == null || t.getClass() != this.getClass()) {
			return false;
		} else {
			return true;
		}
	}

	public String getSQLLit(Object o) {
		return o.toString();
	}

	public static byte[] getBytes(boolean value) {
		if (value) {
			return new byte[] {1};
		} else {
			return new byte[] {0};
		}
	}

	public static boolean getValFromBytes(byte[] bytes) {
		return getValFromBytes(bytes, 0, bytes.length);
	}

	public static boolean getValFromBytes(byte[] bytes, int offset, int length) {
		if (length != bytesPerBool) {
			throw new RuntimeException("Byteified integers must have length " + bytesPerBool);
		}
		boolean value = false;

		if (bytes[0] != 0) {
			value = true;
		}

		return value;
	}

	public byte[] getBytes(Object o) {
		if (o == null) {
			return null;
		}

		boolean value = (Boolean) o;
		return getBytes(value);
	}

	
	private static final Comparator<byte[]> comp = new Comparator<byte[]>() {

		public int compare(byte[] arg0, byte[] arg1) {
			if (arg0.length != bytesPerBool || arg1.length != bytesPerBool) {
				throw new IllegalArgumentException("Serialized booleans must have length " + bytesPerBool);
			}
			
			return arg0[0] - arg1[0];
		}
		
	};
	
	public Comparator<byte[]> getSerializedComparator() {
		return comp;
	}
	
	public @Override
	Boolean fromBytes(byte[] bytes, int offset, int length) {
		return getValFromBytes(bytes, offset, length);
	}
	@Override
	public Object add(Object o1, Object o2) throws ValueMismatchException, NotNumericalException {
		throw new NotNumericalException(this);
	}
	@Override
	public Object divide(Object o, int divisor) throws ValueMismatchException, NotNumericalException {
		throw new NotNumericalException(this);
	}

	public void subclassSerialize(Document doc, Element field) {
		field.setAttribute("type", "boolean");
	}

	public static Type deserialize(String type, boolean nullable, boolean labeledNullable) {
		if (type != null && (type.compareToIgnoreCase("boolean") == 0 || 
				type.compareToIgnoreCase("bool") == 0)) {
			return new BoolType(nullable, labeledNullable);
		}
		return null;
	}

	@Override
	public Boolean fromStringRep(String rep) throws XMLParseException {
		try {
			return Boolean.parseBoolean(rep);
		} catch (NumberFormatException e) {
			throw new XMLParseException("Could not decode integer value " + rep, e);
		}
	}

	@Override
	public String getStringRep(Object o) throws NullPointerException, ValueMismatchException {
		if (o == null) {
			throw new NullPointerException();
		}
		if (! this.isValidForColumn(o)) {
			throw new ValueMismatchException(o,this);
		}

		return Boolean.toString((Boolean) o);
	}

	@Override
	public edu.upenn.cis.orchestra.logicaltypes.OptimizerType getOptimizerType() {
		throw new UnsupportedOperationException("Need to add type to optimizer");
	}

	@Override
	public void setInPreparedStatement(Object o, PreparedStatement ps, int no)
	throws SQLException {
		if (o == null) {
			ps.setNull(no, this.getSqlTypeCode());
		} else {
			ps.setBoolean(no, (Boolean) o);
		}
	}

	@Override
	public int bytesLength() {
		return bytesPerBool;
	}

	@Override
	public int getHashCodeFromBytes(byte[] data, int offset, int length) {
		// From javadoc for Boolean
		if (data[0] == 0) {
			return 1237;
		} else {
			return 1231;
		}
	}

	/**  {@inheritDoc}
	 * @see edu.upenn.cis.orchestra.datamodel.Type#getSqlConstantType()
	 */
	@Override
	public edu.upenn.cis.orchestra.sql.ISqlConstant.Type getSqlConstantType() {
		return ISqlConstant.Type.UNKNOWN;
	}
}