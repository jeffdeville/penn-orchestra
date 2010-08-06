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

import static edu.upenn.cis.orchestra.TestUtil.JUNIT4_TESTNG_GROUP;

import org.junit.Before;
import org.junit.Test;
import org.testng.annotations.BeforeMethod;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import static edu.upenn.cis.orchestra.TestUtil.FAST_TESTNG_GROUP;

@org.testng.annotations.Test(groups = { JUNIT4_TESTNG_GROUP, FAST_TESTNG_GROUP })
public class TestDate {
	
	Date d;
	
	@BeforeMethod()
	@Before
	public void setUp() {
		d = new Date(1982, 2, 17);
	}
	
	@org.testng.annotations.Test(expectedExceptions = IllegalArgumentException.class)
	@Test(expected=IllegalArgumentException.class)
	public void testBadYear() {
		new Date(5,1,1);
	}
	
	@org.testng.annotations.Test(expectedExceptions = IllegalArgumentException.class)
	@Test(expected=IllegalArgumentException.class)
	public void testBadMonth() {
		new Date(2000,13,1);
	}
	
	@org.testng.annotations.Test(expectedExceptions = IllegalArgumentException.class)
	@Test(expected=IllegalArgumentException.class)
	public void testBadMonth2() {
		new Date(2000,0,1);
	}
	
	@org.testng.annotations.Test(expectedExceptions = IllegalArgumentException.class)
	@Test(expected=IllegalArgumentException.class)
	public void testBadDay() {
		new Date(2000,2,30);
	}
	
	@org.testng.annotations.Test(expectedExceptions = IllegalArgumentException.class)
	@Test(expected=IllegalArgumentException.class)
	public void testBadDay2() {
		new Date(2000,1,0);
	}
	
	@org.testng.annotations.Test(expectedExceptions = IllegalArgumentException.class)
	@Test(expected=IllegalArgumentException.class)
	public void testLeapYear1() {
		new Date(1900,2,29);
	}
	
	@org.testng.annotations.Test(expectedExceptions = IllegalArgumentException.class)
	@Test(expected=IllegalArgumentException.class)
	public void testLeapYear2() {
		new Date(2007,2,29);
	}

	@org.testng.annotations.Test
	public void testLeapYear4() {
		new Date(2000,2,29);
	}

	@org.testng.annotations.Test
	public void testLeapYear3() {
		new Date(2008,2,29);
	}

	@org.testng.annotations.Test
	@Test
	public void testCreation() {
		assertEquals("Incorrect year", (short) 1982, d.getYear());
		assertEquals("Incorrect month", (byte) 2, d.getMonth());
		assertEquals("Incorrect day", (byte) 17, d.getDay());
	}
	
	@org.testng.annotations.Test
	@Test
	public void testMonth() {
		assertEquals("Incorrect month name", "February", d.getMonthName());
	}
	
	@org.testng.annotations.Test
	@Test
	public void testStringification() {
		assertEquals("Incorrect stringification", "1982-02-17", d.toString());
	}
	
	@org.testng.annotations.Test
	@Test
	public void testDestringification() {
		assertEquals("Incorrect destringification", d, Date.fromString("1982-02-17"));
		assertEquals("Incorrect destringification", new Date(1998,11,1), Date.fromString("1998-11-01"));
	}
	
	@org.testng.annotations.Test
	@Test
	public void testSerialization() {
		byte[] data = d.getBytes();
		Date deser = Date.getValFromBytes(data);
		assertEquals("Incorrect serialization or deserialization", d, deser);
	}
	
	@org.testng.annotations.Test
	@Test
	public void testTomorrow() {
		Date dd = d.tomorrow();
		assertEquals("Wrong tomorrow", new Date(1982,2,18), dd);
		
		dd = new Date(1980,2,29).tomorrow();
		assertEquals("Wrong tomorrow", new Date(1980,3,1), dd);
		
		dd = new Date(1980,2,28).tomorrow();
		assertEquals("Wrong tomorrow", new Date(1980,2,29), dd);

		dd = new Date(1982,12,31).tomorrow();
		assertEquals("Wrong tomorrow", new Date(1983,1,1), dd);
	}
	
	@org.testng.annotations.Test
	@Test
	public void testYesterday() {
		Date dd = d.yesterday();
		assertEquals("Wrong yesterday", new Date(1982,2,16), dd);
		
		dd = new Date(1980,3,1).yesterday();
		assertEquals("Wrong yesterday", new Date(1980,2,29), dd);
		
		dd = new Date(1980,2,29).yesterday();
		assertEquals("Wrong yesterday", new Date(1980,2,28), dd);

		dd = new Date(1983,1,1).yesterday();
		assertEquals("Wrong yesterday", new Date(1982,12,31), dd);
		
	}
	
	@org.testng.annotations.Test
	@Test
	public void testDayOfYear() {
		assertEquals("Wrong day of year", 48, d.getDayOfYear());
		
		Date dd = new Date(1983,1,1);
		assertEquals("Wrong day of year", 1, dd.getDayOfYear());
	}
	
	@org.testng.annotations.Test
	@Test
	public void testToSQL() {
		java.sql.Date sql = d.getSQL();
		
		Calendar c = new GregorianCalendar(TimeZone.getTimeZone("GMT"));
		c.setTime(sql);
		assertEquals("Incorrect year", 1982, c.get(Calendar.YEAR));
		assertEquals("Incorrect month", Calendar.FEBRUARY, c.get(Calendar.MONTH));
		assertEquals("Incorrect day", 17, c.get(Calendar.DAY_OF_MONTH));
	}
	
	@org.testng.annotations.Test
	@Test
	public void testFromSQL() {
		java.sql.Date sql = java.sql.Date.valueOf("1982-02-17");
		Date fromSql = Date.fromSQL(sql);
		assertEquals("Incorrect date from SQL date", d, fromSql);
	}
	
	@org.testng.annotations.Test
	@Test
	public void testCompare1() {
		Date dd = new Date(2007, 1, 1);
		assertTrue("Should be less than", d.compareTo(dd) < 0);
		assertTrue("Should be greater than", dd.compareTo(d) > 0);
	}

	@org.testng.annotations.Test
	@Test
	public void testCompare2() {
		Date dd = new Date(1982, 3, 1);
		assertTrue("Should be less than", d.compareTo(dd) < 0);
		assertTrue("Should be greater than", dd.compareTo(d) > 0);
	}

	@org.testng.annotations.Test
	@Test
	public void testCompare3() {
		Date dd = new Date(1982, 2, 18);
		assertTrue("Should be less than", d.compareTo(dd) < 0);
		assertTrue("Should be greater than", dd.compareTo(d) > 0);
	}

	@org.testng.annotations.Test
	@Test
	public void testCompare4() {
		Date dd = new Date(1982, 2, 17);
		assertTrue("Should be equal", d.compareTo(dd) == 0);
		assertTrue("Should be equal", d.equals(dd));
	}
}
