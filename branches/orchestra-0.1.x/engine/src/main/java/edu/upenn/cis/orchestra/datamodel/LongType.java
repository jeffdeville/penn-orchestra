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
import java.sql.Types;
import java.util.Comparator;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datamodel.exceptions.CompareMismatch;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.sql.ISqlConstant;
import edu.upenn.cis.orchestra.util.XMLParseException;


public class LongType extends Type {
	public static final int bytesPerLong = Long.SIZE / Byte.SIZE;
	private static final long serialVersionUID = 1L;

	public static final LongType LONG = new LongType(true, true);

	public LongType(boolean nullable, boolean labeledNullable) {
		super(nullable, labeledNullable, Long.class);
	}

	public String getXSDType() {
		return "xsd:long";
	}

	@Override
	public Integer compareTwo(Object o1, Object o2) throws CompareMismatch {
		if (o1 == null || o2 == null) {
			return null;
		}
		if (o1.getClass() != Long.class || o2.getClass() != Long.class) {
			throw new CompareMismatch(o1,o2,Long.class);
		}
		Long l1 = (Long) o1;
		Long l2 = (Long) o2;
		return l1.compareTo(l2);
	}

	@Override
	public Object duplicateValue(Object o) {
		// Longs are immutable
		return o;
	}

	public static long getValFromBytes(byte[] bytes) {
		return getValFromBytes(bytes,0);
	}

	public static long getValFromBytes(byte[] bytes, int offset) {
		long value = ((long) (bytes[offset] & 0xFF)) << 56;		
		value |= ((long) (bytes[offset + 1] & 0xFF)) << 48;		
		value |= ((long) (bytes[offset + 2] & 0xFF)) << 40;		
		value |= ((long) (bytes[offset + 3] & 0xFF)) << 32;		
		value |= ((long) (bytes[offset + 4] & 0xFF)) << 24;		
		value |= ((long) (bytes[offset + 5] & 0xFF)) << 16;		
		value |= ((long) (bytes[offset + 6] & 0xFF)) << 8;		
		value |= ((long) (bytes[offset + 7] & 0xFF));

		return value;
	}

	/**
	 * Convert a long into a byte array. This outputs the integer in big-endian format
	 * 
	 * @param l		The value to convert
	 * @return		The corresponding byte array
	 */
	public static byte[] getBytes(long l) {
		byte[] retval = new byte[bytesPerLong];
		putBytes(l, retval, 0);

		return retval;
	}
	
	public static void putBytes(long l, byte[] dest, int offset) {
		dest[offset] = (byte) (l >>> 56);
		dest[offset+1] = (byte) (l >>> 48);
		dest[offset+2] = (byte) (l >>> 40);
		dest[offset+3] = (byte) (l >>> 32);
		dest[offset+4] = (byte) (l >>> 24);
		dest[offset+5] = (byte) (l >>> 16);
		dest[offset+6] = (byte) (l >>> 8);
		dest[offset+7] = (byte) l;
	}

	@Override
	public Long fromBytes(byte[] bytes, int offset, int length) {
		if (length != bytesPerLong) {
			throw new RuntimeException("Byteified long must have length " + bytesPerLong);
		}
		return getValFromBytes(bytes, offset);
	}

	@Override
	public byte[] getBytes(Object o) {
		return getBytes((long) (Long) o);
	}

	@Override
	public Long getFromResultSet(ResultSet rs, int colno) throws SQLException {
		Long l = rs.getLong(colno);
		if (rs.wasNull()) {
			return null;
		} else {
			return l;
		}
	}

	@Override
	public Long getFromResultSet(ResultSet rs, String colname) throws SQLException {
		Long l = rs.getLong(colname);
		if (rs.wasNull()) {
			return null;
		} else {
			return l;
		}
	}

	@Override
	public String getSQLLit(Object o) {
		return o.toString();
	}

	@Override
	public String getSQLTypeName() {
		return "BIGINT";
	}

	@Override
	public int getSqlTypeCode() {
		return Types.BIGINT;
	}

	@Override
	public boolean typeEquals(Type t) {
		return (this.getClass() == t.getClass());
	}

	@Override
	public Object add(Object o1, Object o2) throws ValueMismatchException {
		if (o1 == null && o2 == null) {
			return null;
		}
		if (o1 == null) {
			o1 = 0l;
		}
		if (o2 == null) {
			o2 = 0l;
		}
		if (! (o1 instanceof Long)) {
			throw new ValueMismatchException(o1, this);
		}
		if (! (o2 instanceof Long)) {
			throw new ValueMismatchException(o2, this);
		}
		long l1 = (Long) o1;
		long l2 = (Long) o2;

		return l1 + l2;
	}

	@Override
	public Object divide(Object o, int divisor) throws ValueMismatchException {
		if (o == null) {
			return null;
		}
		if (! (o instanceof Long)) {
			throw new ValueMismatchException(o, this);
		}

		if (divisor == 0) {
			return null;
		}

		long l = (Long) o;
		return l / divisor;
	}

	@Override
	public Object multiply(Object o, int multiplier) throws ValueMismatchException {
		if (o == null) {
			return null;
		}
		if (! (o instanceof Long)) {
			throw new ValueMismatchException(o, this);
		}

		long l = (Long) o;
		return l * multiplier;
	}
	
	public void subclassSerialize(Document doc, Element field) {
		field.setAttribute("type", "long");
	}

	public static Type deserialize(String type, boolean nullable, boolean labeledNullable) {
		if (type != null && (type.compareToIgnoreCase("long") == 0 || 
				type.compareToIgnoreCase("bigint") == 0)) {
			return new LongType(nullable, labeledNullable);
		}
		return null;
	}

	@Override
	public Long fromStringRep(String rep) throws XMLParseException {
		try {
			return Long.parseLong(rep);
		} catch (NumberFormatException e) {
			throw new XMLParseException("Could not decode long value " + rep, e);
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
		return Long.toString((Long) o);
	}

	@Override
	public edu.upenn.cis.orchestra.logicaltypes.OptimizerType getOptimizerType() {
		throw new UnsupportedOperationException("Need to implement type in optimizer");
	}

	@Override
	public void setInPreparedStatement(Object o, PreparedStatement ps, int no)
	throws SQLException {
		if (o == null) {
			ps.setNull(no, this.getSqlTypeCode());
		} else {
			ps.setLong(no, (Long) o);
		}
	}

	@Override
	public int bytesLength() {
		return bytesPerLong;
	}
	
	@Override
	public int getHashCodeFromBytes(byte[] data, int offset, int length) {
		// Should give same hashCode as getHashCode
		// per javadoc
		long l = getValFromBytes(data, offset);
		return (int)(l^(l>>>32));
	}

	private static final Comparator<byte[]> comp = new SerializedLongComparator();
	
	private static class SerializedLongComparator implements Comparator<byte[]>, Serializable {
		private static final long serialVersionUID = 1L;

		public int compare(byte[] o1, byte[] o2) {
			long l1 = LongType.getValFromBytes(o1);
			long l2 = LongType.getValFromBytes(o2);
			if (l1 > l2) {
				return 1;
			} else if (l2 < l1) {
				return -1;
			} else {
				return 0;
			}
		}
		
	}
	
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