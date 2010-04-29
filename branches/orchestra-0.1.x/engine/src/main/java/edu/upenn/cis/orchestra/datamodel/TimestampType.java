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
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.Locale;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datamodel.exceptions.CompareMismatch;
import edu.upenn.cis.orchestra.datamodel.exceptions.NotNumericalException;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.sql.ISqlConstant;
import edu.upenn.cis.orchestra.util.XMLParseException;

public class TimestampType extends Type {
	private static class DateFormats {
		final DateFormat fullDf;

		DateFormats() {
			fullDf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS Z", Locale.US);
		}
	}

	private static final ThreadLocal<DateFormats> dateFormats
	= new ThreadLocal<DateFormats>() {
		protected DateFormats initialValue() {
			return new DateFormats();
		}
	};

	private static final long serialVersionUID = 1L;

	public static final DateType DATE = new DateType(true, true);

	public TimestampType(boolean nullable, boolean labeledNullable) {
		super(nullable, labeledNullable, Date.class);
	}

	public String getXSDType() {
		return "xsd:date";
	}

	@Override
	public Integer compareTwo(Object o1, Object o2) throws CompareMismatch {
		if (o1 == null || o2 == null) {
			return null;
		}
		if (o1.getClass() != Date.class || o2.getClass() != Date.class) {
			throw new CompareMismatch(o1,o2,Date.class);
		}
		Date d1 = (Date) o1;
		Date d2 = (Date) o2;
		return d1.compareTo(d2);
	}

	@Override
	public Date duplicateValue(Object o) {
		Date d = (Date) o;
		return new Date(d.getTime());
	}

	@Override
	public Date fromBytes(byte[] bytes, int offset, int length) {
		return getValFromBytes(bytes, offset, length);
	}

	@Override
	public byte[] getBytes(Object o) {
		return getBytes((Date) o);
	}
	
	
	private static class SerializedTimestampComparator implements Comparator<byte[]>, Serializable {
		private static final long serialVersionUID = 1L;

		public int compare(byte[] o1, byte[] o2) {
			long l1 = LongType.getValFromBytes(o1);
			long l2 = LongType.getValFromBytes(o2);
			if (l1 > l2) {
				return 1;
			} else if (l1 < l2) {
				return -1;
			} else {
				return 0;
			}
		}		
	}
	
	private static SerializedTimestampComparator comp = new SerializedTimestampComparator();
	
	public Comparator<byte[]> getSerializedComparator() {
		return comp;
	}
	
	public static byte[] getBytes(Date d) {
		long millisecs = d.getTime();
		return LongType.getBytes(millisecs);
	}

	public static Date getValFromBytes(byte[] bytes) {
		return getValFromBytes(bytes, 0, bytes.length);
	}

	public static Date getValFromBytes(byte[] bytes, int offset, int length) {
		long millisecs = LongType.getValFromBytes(bytes, offset);
		return new Date(millisecs);			
	}

	@Override
	public Date getFromResultSet(ResultSet rs, int colno) throws SQLException {
		Timestamp ts = rs.getTimestamp(colno);
		if (rs.wasNull()) {
			return null;
		} else {
			return new Date(ts.getTime());
		}
	}

	@Override
	public Date getFromResultSet(ResultSet rs, String colname) throws SQLException {
		Timestamp ts = rs.getTimestamp(colname);
		if (rs.wasNull()) {
			return null;
		} else {
			return new Date(ts.getTime());
		}
	}

	@Override
	public String getSQLLit(Object o) {
		Date d = (Date) o;
		Timestamp ts = new Timestamp(d.getTime());
		return ts.toString();
	}

	@Override
	public String getSQLTypeName() {
		return "TIMESTAMP";
	}

	@Override
	public int getSqlTypeCode() {
		return Types.TIMESTAMP;
	}

	@Override
	public boolean typeEquals(Type t) {
		return (this.getClass() == t.getClass());
	}

	@Override
	public Object add(Object o1, Object o2) throws NotNumericalException {
		throw new NotNumericalException(this);
	}

	@Override
	public Object divide(Object o, int divisor) throws NotNumericalException {
		throw new NotNumericalException(this);
	}

	public void subclassSerialize(Document doc, Element field) {
		field.setAttribute("type", "timestamp");
	}

	public static Type deserialize(String type, boolean nullable, boolean labeledNullable) {
		if (type != null && type.compareToIgnoreCase("timestamp") == 0) {
			return new DateType(nullable, labeledNullable);
		}
		return null;
	}

	public static Date fromString(String date) {
		try {
			return dateFormats.get().fullDf.parse(date);
		} catch (ParseException e) {
			throw new IllegalArgumentException("Couldn't parse date " + date, e);
		}
	}

	@Override
	public Date fromStringRep(String rep) throws XMLParseException {
		try {
			return fromString(rep);
		} catch (IllegalArgumentException e) {
			throw new XMLParseException("Could not parse date string '" + rep + "'", e);
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

		return dateFormats.get().fullDf.format((Date) o);
	}

	@Override
	public edu.upenn.cis.orchestra.logicaltypes.OptimizerType getOptimizerType() {
		return edu.upenn.cis.orchestra.logicaltypes.DateType.create(this.isNullable(), this.isLabeledNullable());
	}

	@Override
	public void setInPreparedStatement(Object o, PreparedStatement ps, int no)
	throws SQLException {
		if (o == null) {
			ps.setNull(no, this.getSqlTypeCode());
		} else {
			ps.setDate(no, new java.sql.Date(((Date) o).getTime()));
		}
	}

	@Override
	public int bytesLength() {
		return LongType.bytesPerLong;
	}
	
	/**  {@inheritDoc}
	 * @see edu.upenn.cis.orchestra.datamodel.Type#getSqlConstantType()
	 */
	@Override
	public edu.upenn.cis.orchestra.sql.ISqlConstant.Type getSqlConstantType() {
		return ISqlConstant.Type.UNKNOWN;
	}
}

