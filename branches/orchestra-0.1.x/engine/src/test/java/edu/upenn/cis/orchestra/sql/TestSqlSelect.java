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
package edu.upenn.cis.orchestra.sql;

import static edu.upenn.cis.orchestra.OrchestraUtil.newArrayList;
import static edu.upenn.cis.orchestra.TestUtil.DEV_TESTNG_GROUP;
import static edu.upenn.cis.orchestra.TestUtil.FAST_TESTNG_GROUP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

import java.io.StringReader;

import org.testng.Assert;
import org.testng.annotations.Test;

import edu.upenn.cis.orchestra.sql.ISqlConstant.Type;
import edu.upenn.cis.orchestra.sql.ISqlOrderByItem.NullOrderType;
import edu.upenn.cis.orchestra.sql.ISqlOrderByItem.OrderType;

@Test(groups = FAST_TESTNG_GROUP)
public class TestSqlSelect {

	private ISqlFactory _sqlFactory = SqlFactories.getSqlFactory();
	private ISqlParser _parser = SqlFactories.getSqlFactory().newSqlParser();

	public void testAddSelectFromWhereClause() {
		ISqlSelect select = _sqlFactory.newSqlSelect();
		select.addSelectClause(newArrayList(_sqlFactory
				.newSqlSelectItem("CAST(NULL AS VARCHAR(255))")));
		select.addFromClause(newArrayList(_sqlFactory
				.newSqlFromItem("SOME_TABLE")));
		select.addWhere(_sqlFactory.newSqlExpression(ISqlExpression.Code.EQ,
				_sqlFactory.newSqlConstant("1", ISqlConstant.Type.NUMBER),
				_sqlFactory.newSqlConstant("1", ISqlConstant.Type.NUMBER)));
		Assert
				.assertEquals(
						SqlUtil.normalizeStatement(select.toString()),
						SqlUtil
								.normalizeStatement("select cast(null as varchar(255)) from some_table where(1 = 1)"));

	}

	/**
	 * A SELECT with no WHERE clause should return {@code null} for {@code
	 * getWhere()}.
	 * 
	 * @throws ParseException
	 */
	public void noWhereTest() throws ParseException {
		String selectStr = "SELECT a FROM r;";
		_parser.initParser(new StringReader(selectStr));
		ISqlSelect noWhereSelect = (ISqlSelect) _parser.readStatement();
		assertNull(noWhereSelect.getWhere());
	}

	/**
	 * Testing detection of WHERE clauses.
	 * 
	 * @throws ParseException
	 */
	public void whereTest() throws ParseException {
		String selectStr = "SELECT a FROM r where b = c;";
		_parser.initParser(new StringReader(selectStr));
		ISqlSelect whereSelect = (ISqlSelect) _parser.readStatement();
		ISqlExp where = whereSelect.getWhere();
		assertNotNull(where);
	}

	/**
	 * Testing handling of '||' in SQL statements. This is reported in
	 * https://dbappserv.cis.upenn.edu/bugzilla/show_bug.cgi?id=122
	 * 
	 * @throws ParseException
	 */
	@Test(groups = { DEV_TESTNG_GROUP })
	public void concatTest() throws ParseException {
		String query = "SELECT a,c + 3.5,e || 'q',f FROM R WHERE a = 5 AND R.f = (e || 'de') AND b <> 17 AND d < DATE '2006-12-25';";
		_parser.initParser(new StringReader(query));
		try {
			_parser.readStatement();
		} catch (ParseException e) {
			fail("Could not parse statement with concatenation operator '||'.",
					e);
		}

	}

	/**
	 * Test basic handling of order by clause.
	 * 
	 */
	public void orderByTest() {
		final String expected = SqlUtil
				.normalizeStatement("select * from some_table order by some_col, some_other_col desc nulls first");

		ISqlSelect select = _sqlFactory.newSqlSelect();
		select.addSelectClause(newArrayList(_sqlFactory.newSqlSelectItem("*")));
		select.addFromClause(newArrayList(_sqlFactory
				.newSqlFromItem("SOME_TABLE")));
		select.addOrderBy(newArrayList(_sqlFactory
				.newSqlOrderByItem(_sqlFactory.newSqlConstant("SOME_COL",
						Type.COLUMNNAME)), _sqlFactory.newSqlOrderByItem(
				_sqlFactory.newSqlConstant("SOME_OTHER_COL", Type.COLUMNNAME),
				OrderType.DESC, NullOrderType.NULLS_FIRST)));
		final String actual = SqlUtil.normalizeStatement(select.toString());
		assertEquals(actual, expected);
	}

	/**
	 * Test select with a LEAST in the the select clause.
	 */
	public void selectLeast() {
		final String expected = SqlUtil
				.normalizeStatement("select least (33, 23, 10, 7) as least_value from dual");
		final ISqlSelect select = _sqlFactory.newSqlSelect();
		final ISqlSelectItem selectItem = _sqlFactory.newSqlSelectItem();
		selectItem.setExpression(_sqlFactory.newSqlExpression("least",
				_sqlFactory.newSqlConstant("33", Type.NUMBER), _sqlFactory
						.newSqlConstant("23", Type.NUMBER), _sqlFactory
						.newSqlConstant("10", Type.NUMBER), _sqlFactory
						.newSqlConstant("7", Type.NUMBER)));
		selectItem.setAlias("least_value");
		select.addSelectClause(newArrayList(selectItem));
		select.addFromClause(newArrayList(_sqlFactory.newSqlFromItem("dual")));
		assertEquals(SqlUtil.normalizeStatement(select.toString()), expected);
	}
}
