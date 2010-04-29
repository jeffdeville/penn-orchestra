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
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.sql.ISqlConstant;
import edu.upenn.cis.orchestra.util.XMLParseException;


/**
 * @author Nick Taylor
 * 
 * Class to represent an integer-valued attribute in a schema.
 *
 */
public class IntType extends Type {
	private static final long serialVersionUID = 1L;
	public final static int bytesPerInt = Integer.SIZE / Byte.SIZE;
	public final static int bytesPerShortInt = 2;
	public final static int maxShortInt = 65535;
	
	public static final IntType INT = new IntType(true, true);

	public IntType(boolean nullable, boolean labeledNullable) {
		super(nullable, labeledNullable, Integer.class);
	}

	public String getSQLTypeName() {
		return "INT";
	}

	public String getXSDType() {
		return "xsd:int";
	}

	public Object duplicateValue(Object o) {
		// Integers are immutable, so no need to duplicate
		return o;
	}

	public Integer compareTwo(Object o1, Object o2) throws CompareMismatch {
		if (o1 == null || o2 == null) {
			return null;
		}
		if (o1.getClass() != Integer.class || o2.getClass() != Integer.class) {
			throw new CompareMismatch(o1, o2, Integer.class);
		}
		Integer i1 = (Integer) o1;
		Integer i2 = (Integer) o2;
		return i1.compareTo(i2);
	}

	public int getSqlTypeCode() {
		return java.sql.Types.INTEGER;
	}

	public Object getFromResultSet(ResultSet rs, int colno) throws SQLException {
		int i = rs.getInt(colno);
		if (rs.wasNull()) {
			return null;
		} else {
			return i;
		}
	}

	public Object getFromResultSet(ResultSet rs, String colname) throws SQLException {
		int i = rs.getInt(colname);
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

	/*
	 * Note that these routines store an integer
	 * in Big-Endian format (MSB first).
	 * 
	 */

	public static byte[] getBytes(int value) {
		byte[] retval = new byte[bytesPerInt];
		retval[0] = (byte) (value >>> 24);
		retval[1] = (byte) (value >>> 16);
		retval[2] = (byte) (value >>> 8);
		retval[3] = (byte) (value);
		return retval;
	}
	
	public static void putBytes(int value, byte[] data, int offset) {
		data[offset] = (byte) (value >>> 24);
		data[offset + 1] = (byte) (value >>> 16);
		data[offset + 2] = (byte) (value >>> 8);
		data[offset + 3] = (byte) (value);		
	}

	public static int getShortValFromBytes(byte[] bytes, int offset, int length) {
		if (length != bytesPerShortInt) {
			throw new RuntimeException("Short ints must have length " + bytesPerShortInt);
		}
		int value = ((int) (bytes[offset] & 0xFF)) << 8;
		value |= ((int) (bytes[offset + 1] & 0xFF));		

		return value;
	}
	
	public static void getShortValBytes(int shortVal, byte[] dest, int pos) {
		if (shortVal < 0 || shortVal > maxShortInt) {
			throw new IllegalArgumentException("short ints must be in the range [0," + maxShortInt + "]");
		}
		dest[pos] = (byte) (shortVal >>> 8);
		dest[pos + 1] = (byte) (shortVal);

	}
	
	public static int getValFromBytes(byte[] bytes) {
		return getValFromBytes(bytes, 0);
	}

	public static int getValFromBytes(byte[] bytes, int offset) {
		int value = ((int) (bytes[offset] & 0xFF)) << 24;
		value |= ((int) (bytes[offset + 1] & 0xFF)) << 16;		
		value |= ((int) (bytes[offset + 2] & 0xFF)) << 8;		
		value |= ((int) (bytes[offset + 3] & 0xFF));		

		return value;
	}

	public byte[] getBytes(Object o) {
		if (o == null) {
			return null;
		}
		int value = ((Integer) o).intValue();
		return getBytes(value);
	}
	public @Override
	Integer fromBytes(byte[] bytes, int offset, int length) {
		if (length != bytesPerInt) {
			throw new RuntimeException("Byteified integers must have length " + bytesPerInt);
		}
		return getValFromBytes(bytes, offset);
	}
	@Override
	public Object add(Object o1, Object o2) throws ValueMismatchException {
		if (o1 == null && o2 == null) {
			return null;
		}
		if (o1 == null) {
			o1 = 0;
		}
		if (o2 == null) {
			o2 = 0;
		}
		if (! (o1 instanceof Integer)) {
			throw new ValueMismatchException(o1, this);
		}
		if (! (o2 instanceof Integer)) {
			throw new ValueMismatchException(o2, this);
		}
		int i1 = (Integer) o1;
		int i2 = (Integer) o2;
		return i1 + i2;
	}
	@Override
	public Object divide(Object o, int divisor) throws ValueMismatchException {
		if (o == null) {
			return null;
		}
		if (! (o instanceof Integer)) {
			throw new ValueMismatchException(o, this);
		}

		if (divisor == 0) {
			return null;
		}

		int i = (Integer) o;
		return i / divisor;
	}

	@Override
	public Object multiply(Object o, int multiplier) throws ValueMismatchException {
		if (o == null) {
			return null;
		}
		if (! (o instanceof Integer)) {
			throw new ValueMismatchException(o, this);
		}

		int i = (Integer) o;
		return i * multiplier;
	}
	
	public void subclassSerialize(Document doc, Element field) {
		field.setAttribute("type", "integer");
	}

	public static Type deserialize(String type, boolean nullable, boolean labeledNullable) {
		if (type != null && (type.compareToIgnoreCase("integer") == 0 ||
				type.compareToIgnoreCase("int") == 0)) {
			return new IntType(nullable, labeledNullable);
		}
		return null;
	}

	@Override
	public Integer fromStringRep(String rep) throws XMLParseException {
		try {
			return Integer.parseInt(rep);
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

		return Integer.toString((Integer) o);
	}

	@Override
	public edu.upenn.cis.orchestra.logicaltypes.OptimizerType getOptimizerType() {
		return edu.upenn.cis.orchestra.logicaltypes.IntType.create(this.isNullable(), this.isLabeledNullable());
	}

	@Override
	public void setInPreparedStatement(Object o, PreparedStatement ps, int no)
	throws SQLException {
		if (o == null) {
			ps.setNull(no, this.getSqlTypeCode());
		} else {
			ps.setInt(no, (Integer) o);
		}
	}

	@Override
	public int bytesLength() {
		return bytesPerInt;
	}

	@Override
	public int getHashCodeFromBytes(byte[] data, int offset, int length) {
		return getValFromBytes(data, offset);
	}

	
	private static class SerializedIntComparator implements Comparator<byte[]>, Serializable {
		private static final long serialVersionUID = 1L;

		public int compare(byte[] o1, byte[] o2) {
			int i1 = IntType.getValFromBytes(o1);
			int i2 = IntType.getValFromBytes(o2);
			if (i1 > i2) {
				return 1;
			} else if (i1 < i2) {
				return -1;
			} else {
				return 0;
			}
		}
		
	}
	
	private static final SerializedIntComparator comp = new SerializedIntComparator();
	
	@Override
	public Comparator<byte[]> getSerializedComparator() {
		return comp;
	}

	/**  {@inheritDoc}
	 * @see edu.upenn.cis.orchestra.datamodel.Type#getSqlConstantType()
	 */
	@Override
	public edu.upenn.cis.orchestra.sql.ISqlConstant.Type getSqlConstantType() {
		return ISqlConstant.Type.NUMBER;
	}
}