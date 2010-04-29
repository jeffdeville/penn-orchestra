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
import java.sql.Types;
import java.util.Comparator;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datamodel.exceptions.CompareMismatch;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.sql.ISqlConstant;
import edu.upenn.cis.orchestra.util.XMLParseException;


public class DateType extends Type {
	private static final long serialVersionUID = 1L;

	public static final DateType DATE = new DateType(true, true);

	public DateType(boolean nullable, boolean labeledNullable) {
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
		return (Date) o;
	}

	@Override
	public Date fromBytes(byte[] bytes, int offset, int length) {
		return getValFromBytes(bytes, offset, length);
	}

	@Override
	public byte[] getBytes(Object o) {
		return getBytes((Date) o);
	}

	public Comparator<byte[]> getSerializedComparator() {
		return Date.serializedComparator;
	}
	
	public static byte[] getBytes(Date d) {
		return d.getBytes();
	}

	public static Date getValFromBytes(byte[] bytes) {
		return Date.getValFromBytes(bytes);
	}

	public static Date getValFromBytes(byte[] bytes, int offset, int length) {
		return Date.getValFromBytes(bytes, offset, length);
	}

	@Override
	public Date getFromResultSet(ResultSet rs, int colno) throws SQLException {
		return Date.fromSQL(rs.getDate(colno));
	}

	@Override
	public Date getFromResultSet(ResultSet rs, String colname) throws SQLException {
		return Date.fromSQL(rs.getDate(colname));
	}

	@Override
	public String getSQLLit(Object o) {
		Date d = (Date) o;
		return d.toString();
	}

	@Override
	public String getSQLTypeName() {
		return "DATE";
	}

	@Override
	public int getSqlTypeCode() {
		return Types.DATE;
	}

	@Override
	public boolean typeEquals(Type t) {
		return (this.getClass() == t.getClass());
	}

	public void subclassSerialize(Document doc, Element field) {
		field.setAttribute("type", "date");
	}

	public static Type deserialize(String type, boolean nullable, boolean labeledNullable) {
		if (type != null && type.compareToIgnoreCase("date") == 0) {
			return new DateType(nullable, labeledNullable);
		}
		return null;
	}

	@Override
	public Date fromStringRep(String rep) throws XMLParseException {
		try {
			return Date.fromString(rep);
		} catch (IllegalArgumentException e) {
			throw new XMLParseException("Could not parse date string '" + rep + "'");
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

		return ((Date) o).toString();
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
			ps.setDate(no, ((Date) o).getSQL());
		}
	}

	@Override
	public int bytesLength() {
		return Date.bytesPerDate;
	}

	@Override
	public int getHashCodeFromBytes(byte[] data, int offset, int length) {
		// From Date.getBytes and Date.hashCode
		int year = ((int) (data[offset] & 0xFF)) << 8;
		year |= ((int) (data[offset + 1] & 0xFF));
		byte month = data[offset + 2];
		byte day = data[offset + 3];
		
		return year + 37 * month + 1201 * day;
	}

	/**  {@inheritDoc}
	 * @see edu.upenn.cis.orchestra.datamodel.Type#getSqlConstantType()
	 */
	@Override
	public edu.upenn.cis.orchestra.sql.ISqlConstant.Type getSqlConstantType() {
		return ISqlConstant.Type.DATE;
	}
}

