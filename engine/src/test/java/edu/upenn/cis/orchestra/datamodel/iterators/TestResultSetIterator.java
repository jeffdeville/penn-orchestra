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
package edu.upenn.cis.orchestra.datamodel.iterators;

import static edu.upenn.cis.orchestra.TestUtil.FAST_TESTNG_GROUP;
import static edu.upenn.cis.orchestra.TestUtil.JUNIT4_TESTNG_GROUP;
import static edu.upenn.cis.orchestra.TestUtil.REQUIRES_DATABASE_TESTNG_GROUP;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.NoSuchElementException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;

@org.testng.annotations.Test(groups = {REQUIRES_DATABASE_TESTNG_GROUP})
public class TestResultSetIterator {
	private static String jdbcUrl = "jdbc:db2://localhost:50000/orchestr";
	private static String username = "";
	private static String password = "";
	private static String jdbcDriver = "com.ibm.db2.jcc.DB2Driver";
	static Connection conn;
	static Statement s;

	private static class It extends ResultSetIterator<Integer> {

		public It(ResultSet rs) throws SQLException {
			super(rs);
		}

		@Override
		public Integer readCurrent() throws IteratorException {
			try {
				return rs.getInt(1);
			} catch (SQLException e) {
				throw new IteratorException(e);
			}
		}

	}

	/**
	 * When running as a TestNG test we use this to be able to pass in parameters for the jdbc connection.
	 * 
	 * @param jdbcUrlParam
	 * @param usernameParam
	 * @param passwordParam
	 * @param jdbcDriverParam
	 */
	@org.testng.annotations.BeforeClass(groups = { JUNIT4_TESTNG_GROUP, FAST_TESTNG_GROUP })
	@Parameters({"db-url", "db-user", "db-password", "jdbc-driver"})
	public void init(String jdbcUrlParam, String usernameParam, String passwordParam, String jdbcDriverParam) {
		jdbcUrl = jdbcUrlParam;
		username = usernameParam;
		password = passwordParam;
		jdbcDriver = jdbcDriverParam;
	}
	
	@BeforeClass
	@org.testng.annotations.BeforeClass(groups = { JUNIT4_TESTNG_GROUP, FAST_TESTNG_GROUP }, dependsOnMethods = "init")
	public static void setUp() throws Exception {
		Class.forName(jdbcDriver);
		conn = DriverManager.getConnection(jdbcUrl, username, password);
		s = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		try {
			s.execute("DROP TABLE testresultsetiterator");
		} catch (SQLException e) {
			// Table already exists;
		}
		s.execute("CREATE TABLE testresultsetiterator(a INTEGER NOT NULL PRIMARY KEY)");
	}

	@Before
  @BeforeMethod(groups = { JUNIT4_TESTNG_GROUP, FAST_TESTNG_GROUP })
	public void clear() throws Exception {
		s.execute("DELETE FROM testresultsetiterator");
	}

	@AfterClass
	@org.testng.annotations.AfterClass(groups = { JUNIT4_TESTNG_GROUP, FAST_TESTNG_GROUP })
	public static void tearDown() throws Exception {
		s.execute("DROP TABLE testresultsetiterator");
		s.close();
		conn.close();
	}

	@Test(expected=NoSuchElementException.class)
  @org.testng.annotations.Test(groups = { JUNIT4_TESTNG_GROUP, FAST_TESTNG_GROUP },expectedExceptions=NoSuchElementException.class)
	public void empty() throws Exception {
		ResultSet rs = s.executeQuery("SELECT * FROM testresultsetiterator");
		It it = new It(rs);
		try {

			assertFalse("Empty result set should not have a previous element", it.hasPrev());
			assertFalse("Empty result set should not have a next element", it.hasNext());
			it.next();
		} finally {
			it.close();
		}
	}

	@Test
  @org.testng.annotations.Test(groups = { JUNIT4_TESTNG_GROUP, FAST_TESTNG_GROUP })
	public void forwards() throws Exception {
		s.executeUpdate("INSERT INTO testresultsetiterator VALUES 1,2,3");
		ResultSet rs = s.executeQuery("SELECT * FROM testresultsetiterator ORDER BY a");
		It it = new It(rs);
		try {
			assertFalse("First row should not have a previous element", it.hasPrev());
			assertTrue("Result set should have a next element", it.hasNext());
			assertEquals("Incorrect first result from iterator", Integer.valueOf(1), it.next());
			assertTrue("Result set should have a next element", it.hasNext());
			assertTrue("Result set should have a previous element", it.hasPrev());
			assertEquals("Incorrect second result from iterator", Integer.valueOf(2), it.next());
			assertTrue("Result set should have a next element", it.hasNext());
			assertTrue("Result set should have a previous element", it.hasPrev());
			assertEquals("Incorrect third result from iterator", Integer.valueOf(3), it.next());
			assertFalse("Last row should not have a next element", it.hasNext());
			assertTrue("Last row should have a previous element", it.hasPrev());
		} finally {
			it.close();
		}
	}

	@Test
  @org.testng.annotations.Test(groups = { JUNIT4_TESTNG_GROUP, FAST_TESTNG_GROUP })
	public void backwards() throws Exception {
		s.executeUpdate("INSERT INTO testresultsetiterator VALUES 1,2,3");
		ResultSet rs = s.executeQuery("SELECT * FROM testresultsetiterator ORDER BY a");
		It it = new It(rs);
		try {
			while (it.hasNext()) {
				it.next();
			}
			assertTrue("Last row should have a previous element", it.hasPrev());
			assertFalse("Last row should not have a next element", it.hasNext());
			assertEquals("Incorrect third result from iterator", Integer.valueOf(3), it.prev());
			assertTrue("Result set should have a next element", it.hasNext());
			assertTrue("Result set should have a previous element", it.hasPrev());
			assertEquals("Incorrect second result from iterator", Integer.valueOf(2), it.prev());
			assertTrue("Result set should have a next element", it.hasNext());
			assertTrue("Result set should have a previous element", it.hasPrev());
			assertEquals("Incorrect first result from iterator", Integer.valueOf(1), it.prev());
			assertFalse("First row should not have a previous element", it.hasPrev());
			assertTrue("Result set should have a next element", it.hasNext());
		} finally {
			it.close();
		}
	}

	@Test
  @org.testng.annotations.Test(groups = { JUNIT4_TESTNG_GROUP, FAST_TESTNG_GROUP })
	public void backAndForthOneElement() throws Exception {
		s.executeUpdate("INSERT INTO testresultsetiterator VALUES 1");
		ResultSet rs = s.executeQuery("SELECT * FROM testresultsetiterator ORDER BY a");
		It it = new It(rs);
		try {
			assertEquals("Incorrect result from first forwards", Integer.valueOf(1), it.next());
			assertFalse("After last row should not have a next element", it.hasNext());
			assertEquals("Incorrect result from first backwards", Integer.valueOf(1), it.prev());

			assertTrue("Before first row should have a next element", it.hasNext());
			assertFalse("Before first row should not have a previous element", it.hasPrev());

			assertEquals("Incorrect result from second forwards", Integer.valueOf(1), it.next());
			assertFalse("After last row should not have a next element", it.hasNext());
			assertTrue("After last row should have a previous element", it.hasPrev());
			assertEquals("Incorrect result from second backwards", Integer.valueOf(1), it.prev());
		} finally {
			it.close();
		}
	}

	@Test
  @org.testng.annotations.Test(groups = { JUNIT4_TESTNG_GROUP, FAST_TESTNG_GROUP })
	public void backAndForthTwoElements() throws Exception {
		s.executeUpdate("INSERT INTO testresultsetiterator VALUES 1,2");
		ResultSet rs = s.executeQuery("SELECT * FROM testresultsetiterator ORDER BY a");
		It it = new It(rs);
		try {
			assertEquals("Incorrect result from first forwards", Integer.valueOf(1), it.next());
			assertEquals("Incorrect result from first forwards", Integer.valueOf(2), it.next());
			assertFalse("After last row should not have a next element", it.hasNext());
			assertEquals("Incorrect result from first backwards", Integer.valueOf(2), it.prev());
			assertEquals("Incorrect result from first backwards", Integer.valueOf(1), it.prev());

			assertTrue("Before first row should have a next element", it.hasNext());
			assertFalse("Before first row should not have a previous element", it.hasPrev());

			assertEquals("Incorrect result from second forwards", Integer.valueOf(1), it.next());
			assertEquals("Incorrect result from second forwards", Integer.valueOf(2), it.next());
			assertFalse("After last row should not have a next element", it.hasNext());
			assertTrue("After last row should have a previous element", it.hasPrev());
			assertEquals("Incorrect result from second backwards", Integer.valueOf(2), it.prev());
			assertEquals("Incorrect result from second backwards", Integer.valueOf(1), it.prev());
		} finally {
			it.close();
		}
	}
}
