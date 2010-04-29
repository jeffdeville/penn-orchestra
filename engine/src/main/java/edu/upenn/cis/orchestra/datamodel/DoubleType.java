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

public class DoubleType extends Type {
	// Two doubles must differ by more than this fraction to be considered inequal
	final public static double diffFrac = 0.000001;
	private static final long serialVersionUID = 1L;
	public static final int bytesPerDouble = Double.SIZE / Byte.SIZE;

	public static final DoubleType DOUBLE = new DoubleType(true, true);

	public DoubleType(boolean nullable, boolean labeledNullable) {
		super(nullable, labeledNullable, Double.class);
	}

	public String getXSDType() {
		return "xsd:double";
	}

	@Override
	public String getSQLLit(Object o) {
		return o.toString();
	}

	@Override
	public String getSQLTypeName() {
		return "DOUBLE";
	}

	@Override
	public Object duplicateValue(Object o) {
		// Doubles are immutable
		return o;
	}

	@Override
	public Integer compareTwo(Object o1, Object o2) throws CompareMismatch {
		if (o1 == null || o2 == null) {
			return null;
		}

		if (o1.getClass() != Double.class || o2.getClass() != Double.class) {
			throw new CompareMismatch(o1, o2, Double.class);
		}

		double d1 = (Double) o1;
		double d2 = (Double) o2;

		if ((Math.abs(d2 - d1) / Math.min(Math.abs(d1),Math.abs(d2))) < diffFrac) {
			return 0;
		} else if (d1 < d2) {
			return -1;
		} else {
			return 1;
		}
	}

	@Override
	public int getSqlTypeCode() {
		return Types.DOUBLE;
	}

	@Override
	public Double getFromResultSet(ResultSet rs, int colno) throws SQLException {
		double d =  rs.getDouble(colno);
		if (rs.wasNull()) {
			return null;
		} else {
			return d;
		}
	}

	@Override
	public Double getFromResultSet(ResultSet rs, String colname) throws SQLException {
		double d = rs.getDouble(colname);
		if (rs.wasNull()) {
			return null;
		} else {
			return d;
		}
	}

	@Override
	public boolean typeEquals(Type t) {
		return (this.getClass() == t.getClass());
	}

	/*
	 * Note that this stores the long generated from the double
	 * in Big-Endian (MSB first). I think this the resulting octet
	 * of bytes should be directly castable to a double in Visual C
	 * or gcc, but this needs to be tested...
	 */

	public static byte[] getBytes(double d) {
		long value = Double.doubleToLongBits(d);
		return LongType.getBytes(value);
	}

	public static double getValFromBytes(byte[] bytes) {
		return getValFromBytes(bytes, 0, bytes.length);
	}

	public static double getValFromBytes(byte[] bytes, int offset, int length) {
		if (length != bytesPerDouble) {
			throw new RuntimeException("Byteified double must have length " + bytesPerDouble);
		}
		long value = LongType.getValFromBytes(bytes, offset);
		double d = Double.longBitsToDouble(value);
		return d;
	}

	@Override
	public byte[] getBytes(Object o) {
		double d = (Double) o;
		return getBytes(d);
	}

	@Override
	public Object fromBytes(byte[] bytes, int offset, int length) {
		return getValFromBytes(bytes, offset, length);
	}

	@Override
	public Object add(Object o1, Object o2) throws ValueMismatchException {
		if (o1 == null && o2 == null) {
			return null;
		}
		if (o1 == null) {
			o1 = 0.0;
		}
		if (o2 == null) {
			o2 = 0.0;
		}
		if (! (o1 instanceof Double)) {
			throw new ValueMismatchException(o1, this);
		}
		if (! (o2 instanceof Double)) {
			throw new ValueMismatchException(o2, this);
		}
		double d1 = (Double) o1;
		double d2 = (Double) o2;
		return d1 + d2;
	}

	@Override
	public Object divide(Object o, int divisor) throws ValueMismatchException {
		if (o == null) {
			return null;
		}
		if (! (o instanceof Double)) {
			throw new ValueMismatchException(o, this);
		}

		if (divisor == 0) {
			return null;
		}

		double d = (Double) o;
		return d / divisor;
	}

	@Override
	public Object multiply(Object o, int multiplier) throws ValueMismatchException {
		if (o == null) {
			return null;
		}
		if (! (o instanceof Double)) {
			throw new ValueMismatchException(o, this);
		}

		double d = (Double) o;
		return d * multiplier;
	}
	
	public void subclassSerialize(Document doc, Element field) {
		field.setAttribute("type", "double");
	}

	public static Type deserialize(String type, boolean nullable, boolean labeledNullable) {
		if (type != null && (type.compareToIgnoreCase("double") == 0 || 
				type.compareToIgnoreCase("float") == 0)) {
			return new DoubleType(nullable, labeledNullable);
		}
		return null;
	}

	@Override
	public Double fromStringRep(String rep) throws XMLParseException {
		try {
			return Double.parseDouble(rep);
		} catch (NumberFormatException e) {
			throw new XMLParseException("Could not decode double value " + rep, e);
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
		return Double.toString((Double) o);
	}

	@Override
	public edu.upenn.cis.orchestra.logicaltypes.OptimizerType getOptimizerType() {
		return edu.upenn.cis.orchestra.logicaltypes.DoubleType.create(this.isNullable(), this.isLabeledNullable());
	}

	@Override
	public void setInPreparedStatement(Object o, PreparedStatement ps, int no)
	throws SQLException {
		if (o == null) {
			ps.setNull(no, this.getSqlTypeCode());
		} else {
			ps.setDouble(no, (Double) o);
		}
	}

	@Override
	public int bytesLength() {
		return bytesPerDouble;
	}
	
	public boolean canCompareSerialized() {
		// Since we want to allow close double values to
		// compare as equal
		return false;
	}
	
	@Override
	public int getHashCode(Object o) throws ValueMismatchException {
		if (o == null) {
			return 0;
		}
		// Since our definition of equality is a bit loose,
		// we can't easily return a meaningful hashcode except to
		// distinguish null from non-null
		return 5;
	}

	@Override
	public int getHashCodeFromBytes(byte[] data, int offset, int length) {
		return 5;
	}

	private static class SerializedDoubleComparator implements Comparator<byte[]>, Serializable {
		private static final long serialVersionUID = 1L;

		public int compare(byte[] o1, byte[] o2) {
			double d1 = DoubleType.getValFromBytes(o1);
			double d2 = DoubleType.getValFromBytes(o2);
			if (d1 < d2) {
				return -1;
			} else if (d1 > d2) {
				return 1;
			} else {
				return 0;
			}
		}
		
	}
	
	private static final SerializedDoubleComparator comp = new SerializedDoubleComparator();
	
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