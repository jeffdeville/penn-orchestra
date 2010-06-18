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
import java.util.Calendar;
import java.util.Comparator;

public class Date implements Comparable<Date>, Serializable {
	private static final long serialVersionUID = 1L;
	private final short year;
	// Month: Jan = 1, Feb = 2 etc. Day is day of the month
	private final byte month, day;

	private static final ThreadLocal<Calendar> calendar
	= new ThreadLocal<Calendar>() {
		public Calendar get() {
			Calendar c = super.get();
			c.clear();
			return c;
		}

		protected Calendar initialValue() {
			return Calendar.getInstance();
		}
	};


	public enum Month {
		JANUARY(31),FEBRUARY(29),MARCH(31),APRIL(30),MAY(31),
		JUNE(30),JULY(31),AUGUST(31),SEPTEMBER(30),OCTOBER(31),
		NOVEMBER(30),DECEMBER(31);

		public final int numDays;
		public final String name;

		private Month(int numDays) {
			this.numDays = numDays;
			StringBuilder sb = new StringBuilder();
			sb.append(name().charAt(0));
			sb.append(name().substring(1).toLowerCase());
			name = sb.toString();
		}

		public String toString() {
			return name;
		}
	}

	private static final Month[] months = Month.values();

	/**
	 * Construct a new date
	 * 
	 * @param year
	 * @param month		Indexed from 1
	 * @param day		Day of month, indexed from 1
	 */
	public Date(short year, byte month, byte day) {
		this.year = year;
		this.month = month;
		this.day = day;
		if (year < 1000 || year > 9999) {
			throw new IllegalArgumentException("Illegal year: " + year + " (year must be in the range 1000-9999)");
		}
		if (month <= 0 || month > months.length) {
			throw new IllegalArgumentException("Illegal month: " + month);
		}
		if (day <= 0  || day > months[month-1].numDays) {
			throw new IllegalArgumentException("Illegal day: " + day);
		}
		if (month == 2 && day == 29 && (! isLeapYear(year))) {
			throw new IllegalArgumentException(year + " is not a leap year");
		}
	}

	/**
	 * Construct a new date
	 * 
	 * @param year
	 * @param month		Indexed from 1
	 * @param day		Day of month, indexed from 1
	 */
	public Date(int year, int month, int day) {
		this((short) year, (byte) month, (byte) day);
	}

	public static Date fromSQL(java.sql.Date d) {
		return fromString(d.toString());
	}

	public java.sql.Date getSQL() {
		return java.sql.Date.valueOf(this.toString());
	}

	public int compareTo(Date d) {
		int compare = ((int) year) - d.year;
		if (compare != 0) {
			return compare;
		}
		compare = ((int) month) - d.month;
		if (compare != 0) {
			return compare;
		}
		compare = ((int) day) - d.day;
		return compare;
	}

	public static final int bytesPerDate = 4;
	private static class SerializedDateComparator implements Comparator<byte[]>, Serializable {
		private static final long serialVersionUID = 1L;

		public int compare(byte[] o1, byte[] o2) {
			if (o1.length != bytesPerDate || o2.length != bytesPerDate) {
				throw new IllegalArgumentException("Serialized dates must have length " + bytesPerDate);
			}

			int cmp = o1[0] - o2[0];
			if (cmp != 0) {
				return cmp;
			}
			cmp = o1[1] - o2[1];
			if (cmp != 0) {
				return cmp;
			}
			cmp = o1[2] - o2[2];
			if (cmp != 0) {
				return cmp;
			}
			cmp = o1[3] - o2[3];
			return cmp;
		}

	};

	static final Comparator<byte[]> serializedComparator = new SerializedDateComparator();

	public byte[] getBytes() {
		byte[] retval = new byte[bytesPerDate];
		retval[0] = (byte) (year >>> 8);
		retval[1] = (byte) year;
		retval[2] = month;
		retval [3] = day;
		return retval;
	}

	public static Date getValFromBytes(byte[] data, int offset, int length) {
		if (length != bytesPerDate) {
			throw new IllegalArgumentException("Length must be " + bytesPerDate);
		}

		int year = ((int) (data[offset] & 0xFF)) << 8;
		year |= ((int) (data[offset + 1] & 0xFF));
		return new Date((short) year, data[offset + 2], data[offset + 3]);
	}
	
	public static int getYearFromBytes(byte[] data, int offset, int length) {
		if (length != bytesPerDate) {
			throw new IllegalArgumentException("Length must be " + bytesPerDate);
		}

		int year = ((int) (data[offset] & 0xFF)) << 8;
		year |= ((int) (data[offset + 1] & 0xFF));
		return year;
	}

	public static Date getValFromBytes(byte[] data) {
		return getValFromBytes(data, 0, data.length);
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(year);
		sb.append('-');
		if (month < 10) {
			sb.append('0');
		}
		sb.append(month);
		sb.append('-');
		if (day < 10) {
			sb.append('0');
		}
		sb.append(day);
		return sb.toString();
	}

	public static Date fromString(String date) throws IllegalArgumentException {
		if (date.length() != 10 || date.charAt(4) != '-' || date.charAt(7) != '-') {
			throw new IllegalArgumentException("Date format: YYYY-MM-DD");
		}
		try {
			short year = Short.parseShort(date.substring(0, 4));
			byte month = Byte.parseByte(date.substring(5, 7));
			byte day = Byte.parseByte(date.substring(8, 10));
			return new Date(year,month,day);
		} catch (NumberFormatException nfe) {
			throw new IllegalArgumentException("Date format: YYYY-MM-DD", nfe);
		}
	}

	public short getYear() {
		return year;
	}

	public byte getMonth() {
		return month;
	}

	public byte getDay() {
		return day;
	}

	public String getMonthName() {
		return months[month-1].name;
	}

	public boolean equals(Object o) {
		if (o == null || o.getClass() != Date.class) {
			return false;
		}
		Date d = (Date) o;
		return (year == d.year && month == d.month && day == d.day);
	}

	public int getDayOfYear() {
		Calendar c = calendar.get();
		c.set(year, month - 1, day);
		return c.get(Calendar.DAY_OF_YEAR);
	}

	/**
	 * Get the day after this day.
	 * 
	 * @return		The date for the day after this day.
	 */
	public Date tomorrow() {
		short year = this.year;
		byte month = this.month;
		byte day = this.day;
		++day;
		if (month == 2 && day == 29 && (! isLeapYear(year))) {
			day = 1;
			month = 3;
		} else if (day > months[month-1].numDays) {
			day = 1;
			++month;
			if (month > months.length) {
				++year;
				month = 1;
			}
		}
		return new Date(year,month,day);
	}

	/**
	 * Get the day before this day.
	 * 
	 * @return		The date for the day before this day.
	 */
	public Date yesterday() {
		short year = this.year;
		byte month = this.month;
		byte day = this.day;
		--day;
		if (month == 3 && day == 0 && (! isLeapYear(year))) {
			day = 28;
		} else if (day < 1) {
			--month;
			if (month < 1) {
				--year;
				month = (byte) months.length;
			}
			day = (byte) months[month-1].numDays;
		}
		return new Date(year,month,day);
	}

	public int hashCode() {
		return year + 37 * month + 1201 * day;
	}

	public static boolean isLeapYear(int year) {
		if (year % 400 == 0) {
			return true;
		} else if (year % 100 == 0) {
			return false;
		} else {
			return (year % 4 == 0);
		}
	}
}
