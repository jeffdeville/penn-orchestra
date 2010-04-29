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
import java.nio.charset.Charset;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Comparator;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datamodel.exceptions.CompareMismatch;
import edu.upenn.cis.orchestra.datamodel.exceptions.NotNumericalException;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.sql.ISqlConstant;

/**
 * @author Nick Taylor
 *
 * Class to represent a CLOB element in a schema.
 */
public class ClobType extends Type {
	private static final long serialVersionUID = 1L;
	private int length;
	/**
	 * Create a new string element to be used in a table schema.
	 * 
	 * @param nullable		Whether this column can be set to null
	 * @param labeledNullable		Whether this column can be set to a labeled null
	 * @param isVariable	Whether the string has variable length
	 * @param length		(Maximum) length of the string
	 */
	public ClobType(boolean nullable, boolean labeledNullable, int length) {
		super(nullable, labeledNullable, String.class);
		this.length = length;
	}

	public String getXSDType() {
		return "xsd:hexBinary";
	}

	public String getSQLTypeName() {
		return "CLOB(" + length + ")";
	}

	public Object duplicateValue(Object o) {
		// Strings are immutable, no need to duplicate
		return o;
	}

	public Integer compareTwo(Object o1, Object o2) throws CompareMismatch {
		if (o1 == null || o2 == null) {
			return null;
		}

		if (o1.getClass() != String.class || o2.getClass() != String.class) {
			throw new CompareMismatch(o1, o2, String.class);
		}

		String s1 = (String) o1;
		String s2 = (String) o2;
		return s1.compareTo(s2);
	}

	public int getSqlTypeCode() {
		return Types.CLOB;
	}

	public String getFromResultSet(ResultSet rs, int colno) throws SQLException {
		String s = rs.getString(colno);
		if (rs.wasNull()) {
			return null;
		} else {
			return s;
		}
	}

	public String getFromResultSet(ResultSet rs, String colname) throws SQLException {
		String s = rs.getString(colname);
		if (rs.wasNull()) {
			return null;
		} else {
			return s;
		}
	}

	public boolean typeEquals(Type t) {
		if (t == null || t.getClass() != this.getClass()) {
			return false;
		}
		ClobType st = (ClobType) t;
		return (st.length == length);
	}

	//@Override
	public int hashCodeREMOVE() {
		int code = super.hashCode();
		code = 31 * code + length;
		return code;
	}
	
	public String getSQLLit(Object o) {
		String val = (String) o;
		StringBuilder sb = new StringBuilder("'");

		final int length = val.length();
		for (int i = 0; i < length; ++i) {
			char c = val.charAt(i);
			if (c == '\'') {
				sb.append("''");
			} else {
				sb.append(c);
			}
		}

		sb.append("'");

		return sb.toString();
	}


	static final Charset cs = Charset.forName("UTF-8");


	public @Override
	byte[] getBytes(Object o) 
	{
		if (o == null) {
			return null;
		}
		String s = (String) o;
		return s.getBytes(cs);
	}

	public @Override
	String fromBytes(byte[] bytes, int offset, int length) {
		return new String(bytes, offset, length, cs);
	}

	@Override
	public Object add(Object o1, Object o2) throws NotNumericalException {
		throw new NotNumericalException(this);
	}

	@Override
	public Object divide(Object o, int divisor) throws NotNumericalException {
		throw new NotNumericalException(this);
	}

	@Override
	public boolean canPutInto(Type t) {
		if (t == null || t.getClass() != this.getClass()) {
			return false;
		}
		ClobType st = (ClobType) t;
		return (st.length == length);
	}

	@Override
	public boolean canReadFrom(Type t) {
		if (t == null || this.getClass() != t.getClass()) {
			return false;
		}
		ClobType st = (ClobType) t;
		return (st.length == length);
	}

	public int getLength() {
		return length;
	}

	public boolean isValidForColumn(Object o) {
		if (o == null) {
			return isNullable();
		}
		if (!(o instanceof String)) {
			return false;
		}
		String s = (String) o;
		return s.length() == length;
	}

	public void subclassSerialize(Document doc, Element field) {
		field.setAttribute("type", "clob(" + length + ")");
	}

	public static Type deserialize(String type, boolean nullable, boolean labeledNullable) {
		if (type == null)
			return null;

		int left = type.indexOf("(");
		int right = type.indexOf(")");
		if (left != -1 && right != -1 && left > 0 && right == type.length()-1 && left < right) {
			//			String name = type.substring(0, left);
			String length = type.substring(left+1, right);
			try {
				int len = Integer.parseInt(length);
				return new ClobType(nullable, labeledNullable, len);
			} catch (NumberFormatException e) {
				return null;
			}
		}
		return null;
	}

	@Override
	public String fromStringRep(String rep) {
		return rep;
	}

	@Override
	public String getStringRep(Object o) throws NullPointerException, ValueMismatchException {
		if (o == null) {
			throw new NullPointerException();
		}
		if (! this.isValidForColumn(o)) {
			throw new ValueMismatchException(o,this);
		}
		return (String) o;
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
			ps.setString(no, (String) o);
		}
	}

	@Override
	public int bytesLength() {
		// Not known
		return -1;
	}

	
	
	private static class StringSerializedComparator implements Comparator<byte[]>, Serializable {
		private static final long serialVersionUID = 1L;

		public int compare(byte[] o1, byte[] o2) {
			String s1 = new String(o1, cs);
			String s2 = new String(o2, cs);

			return s1.compareTo(s2);
		}
	}
	
	private static final StringSerializedComparator comp = new StringSerializedComparator(); 

	@Override
	public Comparator<byte[]> getSerializedComparator() {
		return comp;
	}

	/**  {@inheritDoc}
	 * @see edu.upenn.cis.orchestra.datamodel.Type#getSqlConstantType()
	 */
	@Override
	public edu.upenn.cis.orchestra.sql.ISqlConstant.Type getSqlConstantType() {
		return ISqlConstant.Type.STRING;
	}
}
