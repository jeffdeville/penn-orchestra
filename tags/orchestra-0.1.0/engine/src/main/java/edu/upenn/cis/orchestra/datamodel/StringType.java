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
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.logicaltypes.CharType;
import edu.upenn.cis.orchestra.logicaltypes.VarCharType;
import edu.upenn.cis.orchestra.sql.ISqlConstant;

/**
 * @author Nick Taylor
 *
 * Class to represent a string (i.e. CHAR or VARCHAR) element in a schema.
 */
public class StringType extends Type {
	private static final long serialVersionUID = 1L;
	private final boolean isVariable;
	private final int length;

	/**
	 * Create a new string element to be used in a table schema.
	 * 
	 * @param nullable		Whether this column can be set to null
	 * @param labeledNullable		Whether this column can be set to a labeled null
	 * @param isVariable	Whether the string has variable length
	 * @param length		(Maximum) length of the string
	 */
	public StringType(boolean nullable, boolean labeledNullable, boolean isVariable,
			int length) {
		super(nullable, labeledNullable, String.class);
		this.isVariable = isVariable;
		this.length = length;
	}

	public String getXSDType() {
		return "xsd:string";
	}

	public String getSQLTypeName() {
		String retval = "CHAR(" + Integer.toString(length) + ")";
		if (isVariable) {
			retval = "VAR" + retval;
		}
		return retval;
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
		if (isVariable)
			return Types.VARCHAR;
		else
			return Types.CHAR;
	}

	public String getFromResultSet(ResultSet rs, int colno) throws SQLException {
		String s = rs.getString(colno);
		if (rs.wasNull()) {
			return null;
		} else {
			if (isVariable) {
				return s;
			} else {
				int pos = s.length();
				while (pos > 0 && Character.isWhitespace(s.charAt(pos - 1))) {
					--pos;
				}
				return s.substring(0, pos);
			}
		}
	}

	public String getFromResultSet(ResultSet rs, String colname) throws SQLException {
		String s = rs.getString(colname);
		if (rs.wasNull()) {
			return null;
		} else {
			if (isVariable) {
				return s;
			} else {
				int pos = s.length();
				while (pos > 0 && Character.isWhitespace(s.charAt(pos - 1))) {
					--pos;
				}
				return s.substring(0, pos);
			}
		}
	}

	public boolean typeEquals(Type t) {
		if (t == null || t.getClass() != this.getClass()) {
			return false;
		}
		StringType st = (StringType) t;
		return (st.isVariable == isVariable && st.length == length);
	}

	//@Override
	public int hashCodeREMOVE() {
		int code = super.hashCode();
		code = 31 * code + (isVariable ? 1 : 0);
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

	public @Override
	byte[] getBytes(Object o) 
	{
		if (o == null) {
			return null;
		}
		String s = (String) o;
		return s.getBytes(serializationCharset);
	}

	public @Override
	String fromBytes(byte[] bytes, int offset, int length) {
		return new String(bytes, offset, length, serializationCharset);
	}

	public boolean isVariableLength() {
		return isVariable;
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
		return s.length() <= length;
	}

	public void subclassSerialize(Document doc, Element field) {
		field.setAttribute("type", (isVariable ? "varchar(" : "char(" ) + length + ")");
	}

	public static Type deserialize(String type, boolean nullable, boolean labeledNullable) {
		if (type == null)
			return null;

		int left = type.indexOf("(");
		int right = type.indexOf(")");
		if (left != -1 && right != -1 && left > 0 && right == type.length()-1 && left < right) {
			String name = type.substring(0, left);
			String length = type.substring(left+1, right);
			try {
				int len = Integer.parseInt(length);
				if (name.equalsIgnoreCase("varchar")) {
					return new StringType(nullable, labeledNullable, true, len);
				} else if (name.equalsIgnoreCase("char")) {
					return new StringType(nullable, labeledNullable, false, len);
				}
			} catch (NumberFormatException e) {
				return null;
			}
		}
		return null;
	}

	@Override
	public String fromStringRep(String rep) {
		// pos = index of last character to include
		int pos = rep.length() - 1;
		// Trim to length of datatype
		if (pos >= length) {
			pos = length - 1;
		}
		// Strip out trailing whitespace
		while (pos >= 0) {
			char c = rep.charAt(pos);
			if (! Character.isWhitespace(c)) {
				break;
			}
			--pos;
		}
		if (pos == rep.length() - 1) {
			return rep;
		} else {
			return rep.substring(0, pos + 1);
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
		return (String) o;
	}

	@Override
	public edu.upenn.cis.orchestra.logicaltypes.OptimizerType getOptimizerType() {
		if (isVariable) {
			return new VarCharType(this.isNullable(), this.isLabeledNullable(), length, length / 2);
		} else {
			return new CharType(length, this.isNullable(), this.isLabeledNullable());
		}
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
		return -1;
	}

	// Only use the first 10 bytes to create the hash
	private final int hashCodeLength = 10;

	public static final Charset serializationCharset = Charset.forName("UTF-8");

	@Override
	public int getHashCode(Object o) throws ValueMismatchException {
		if (o == null) {
			return 0;
		}
		try {
			String s = (String) o;
			byte[] bytes = s.substring(0, s.length() > hashCodeLength ? hashCodeLength : s.length()).getBytes(serializationCharset);
			return hashCode(bytes, 0, bytes.length > hashCodeLength ? hashCodeLength : bytes.length);
		} catch (ClassCastException cce) {
			throw new ValueMismatchException(o,this);
		}
	}

	private static int hashCode(byte[] data, int offset, int length) {
		int hashCode = 0;
		final int end = offset + length;
		for (int i = offset; i < end; ++i) {
			hashCode = 31 * hashCode + data[i];
		}
		return hashCode;
	}

	@Override
	public int getHashCodeFromBytes(byte[] data, int offset, int length) {
		if (length > hashCodeLength) {
			return hashCode(data, offset, hashCodeLength);
		} else {
			return hashCode(data, offset, length);
		}
	}
	
	private static class StringSerializedComparator implements Comparator<byte[]>, Serializable {
		private static final long serialVersionUID = 1L;

		public int compare(byte[] o1, byte[] o2) {
			String s1 = new String(o1, serializationCharset);
			String s2 = new String(o2, serializationCharset);

			return s1.compareTo(s2);
		}
	}
	
	private static final StringSerializedComparator comp = new StringSerializedComparator(); 

	@Override
	public Comparator<byte[]> getSerializedComparator() {
		return comp;
	}
	
	@Override
	public boolean canPutInto(Type t) {
		if (! super.canPutInto(t)) {
			return false;
		}
		StringType st = (StringType) t;
		if (st.isVariable) {
			return st.length >= this.length;
		} else if (this.isVariable) {
			return false;
		} else {
			return st.length == this.length;
		}
	}
	
	@Override
	public boolean canReadFrom(Type t) {
		if (! super.canReadFrom(t)) {
			return false;
		}
		StringType st = (StringType) t;
		if (this.isVariable) {
			return this.length >= st.length;
		} else if (st.isVariable) {
			return false;
		} else {
			return this.length == st.length;
		}
	}
	
	/**  {@inheritDoc}
	 * @see edu.upenn.cis.orchestra.datamodel.Type#getSqlConstantType()
	 */
	@Override
	public edu.upenn.cis.orchestra.sql.ISqlConstant.Type getSqlConstantType() {
		return ISqlConstant.Type.STRING;
	}
}
